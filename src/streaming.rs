use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

use crate::error::SpannerError;
use crate::runtime;

const PRODUCER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

struct CompletionSignal(Option<std_mpsc::SyncSender<()>>);

impl Drop for CompletionSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[cfg(test)]
struct ReporterSendProbe {
    pending: Option<std_mpsc::SyncSender<()>>,
    completed: Option<std_mpsc::SyncSender<()>>,
}

type StreamReceiver<T> = mpsc::Receiver<Result<T, SpannerError>>;
type ReceiverSlot<T> = Arc<Mutex<Option<StreamReceiver<T>>>>;

struct ReceiverLease<T: Send + 'static> {
    slot: ReceiverSlot<T>,
    receiver: Option<StreamReceiver<T>>,
}

impl<T: Send + 'static> ReceiverLease<T> {
    fn acquire(slot: ReceiverSlot<T>) -> Result<Self, SpannerError> {
        let receiver = slot
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .ok_or_else(|| {
                SpannerError::Other("Streaming receiver is already in use".to_string())
            })?;
        Ok(Self {
            slot,
            receiver: Some(receiver),
        })
    }

    fn receiver_mut(&mut self) -> &mut StreamReceiver<T> {
        self.receiver
            .as_mut()
            .expect("receiver lease must own its receiver")
    }
}

impl<T: Send + 'static> Drop for ReceiverLease<T> {
    fn drop(&mut self) {
        let Some(receiver) = self.receiver.take() else {
            return;
        };
        let mut slot = self.slot.lock().unwrap_or_else(|error| error.into_inner());
        debug_assert!(slot.is_none(), "receiver slot was refilled while leased");
        if slot.is_none() {
            *slot = Some(receiver);
        }
    }
}

/// Bounded row stream shared by the query and scan table functions.
///
/// On drop, the producer first receives a cancellation token so partition work
/// can quiesce before the stream is released. Its abort handle is only a
/// bounded teardown fallback: Tokio cannot guarantee quiescence for a future
/// that never yields after being aborted. A small reporter task translates
/// producer errors and panics into channel errors rather than allowing them to
/// look like a successful end of stream.
pub struct StreamingState<T: Send + 'static> {
    receiver: ReceiverSlot<T>,
    batch_size: usize,
    producer_abort: AbortHandle,
    reporter_abort: AbortHandle,
    cancellation: CancellationToken,
    reporter_done: Mutex<Option<std_mpsc::Receiver<()>>>,
    shutdown_timeout: Duration,
    #[cfg(test)]
    fallback_probe: Option<std_mpsc::SyncSender<()>>,
}

impl<T> StreamingState<T>
where
    T: Send + 'static,
{
    /// Start a bounded producer and retain cancellation for its lifetime.
    pub fn spawn<F, Fut>(batch_size: usize, producer: F) -> Result<Self, SpannerError>
    where
        F: FnOnce(mpsc::Sender<Result<T, SpannerError>>, CancellationToken) -> Fut,
        Fut: std::future::Future<Output = Result<(), SpannerError>> + Send + 'static,
    {
        #[cfg(test)]
        {
            Self::spawn_inner(batch_size, producer, None, PRODUCER_SHUTDOWN_TIMEOUT, None)
        }
        #[cfg(not(test))]
        {
            Self::spawn_inner(batch_size, producer)
        }
    }

    #[cfg(test)]
    fn spawn_with_reporter_probe<F, Fut>(
        batch_size: usize,
        producer: F,
        reporter_send_pending: std_mpsc::SyncSender<()>,
        reporter_send_completed: std_mpsc::SyncSender<()>,
    ) -> Result<Self, SpannerError>
    where
        F: FnOnce(mpsc::Sender<Result<T, SpannerError>>, CancellationToken) -> Fut,
        Fut: std::future::Future<Output = Result<(), SpannerError>> + Send + 'static,
    {
        Self::spawn_inner(
            batch_size,
            producer,
            Some(ReporterSendProbe {
                pending: Some(reporter_send_pending),
                completed: Some(reporter_send_completed),
            }),
            PRODUCER_SHUTDOWN_TIMEOUT,
            None,
        )
    }

    #[cfg(test)]
    fn spawn_with_shutdown_timeout<F, Fut>(
        batch_size: usize,
        producer: F,
        shutdown_timeout: Duration,
        fallback_probe: std_mpsc::SyncSender<()>,
    ) -> Result<Self, SpannerError>
    where
        F: FnOnce(mpsc::Sender<Result<T, SpannerError>>, CancellationToken) -> Fut,
        Fut: std::future::Future<Output = Result<(), SpannerError>> + Send + 'static,
    {
        Self::spawn_inner(
            batch_size,
            producer,
            None,
            shutdown_timeout,
            Some(fallback_probe),
        )
    }

    fn spawn_inner<F, Fut>(
        batch_size: usize,
        producer: F,
        #[cfg(test)] reporter_send_probe: Option<ReporterSendProbe>,
        #[cfg(test)] shutdown_timeout: Duration,
        #[cfg(test)] fallback_probe: Option<std_mpsc::SyncSender<()>>,
    ) -> Result<Self, SpannerError>
    where
        F: FnOnce(mpsc::Sender<Result<T, SpannerError>>, CancellationToken) -> Fut,
        Fut: std::future::Future<Output = Result<(), SpannerError>> + Send + 'static,
    {
        #[cfg(not(test))]
        let shutdown_timeout = PRODUCER_SHUTDOWN_TIMEOUT;

        let (tx, receiver) = mpsc::channel(batch_size);
        let cancellation = CancellationToken::new();
        let producer = runtime::spawn(producer(tx.clone(), cancellation.clone()))?;
        let producer_abort = producer.abort_handle();
        let reporter_cancellation = cancellation.clone();
        let (reporter_done_tx, reporter_done_rx) = std_mpsc::sync_channel(1);

        let reporter = match runtime::spawn(async move {
            let _completion = CompletionSignal(Some(reporter_done_tx));
            let error = match producer.await {
                Ok(Ok(())) => return,
                Ok(Err(error)) => error,
                Err(error) if error.is_cancelled() && reporter_cancellation.is_cancelled() => {
                    return;
                }
                Err(error) if error.is_panic() => {
                    SpannerError::Other(format!("Streaming producer task panicked: {error}"))
                }
                Err(error) => SpannerError::Other(format!(
                    "Streaming producer task ended unexpectedly: {error}"
                )),
            };

            #[cfg(test)]
            send_terminal_error(tx, error, reporter_send_probe).await;
            #[cfg(not(test))]
            send_terminal_error(tx, error).await;
        }) {
            Ok(reporter) => reporter,
            Err(error) => {
                producer_abort.abort();
                return Err(error);
            }
        };

        Ok(Self {
            receiver: Arc::new(Mutex::new(Some(receiver))),
            batch_size,
            producer_abort,
            reporter_abort: reporter.abort_handle(),
            cancellation,
            reporter_done: Mutex::new(Some(reporter_done_rx)),
            shutdown_timeout,
            #[cfg(test)]
            fallback_probe,
        })
    }

    /// Receive one row on the runtime owner, then drain immediately available
    /// rows into a DuckDB-sized batch. `None` is a clean end of stream.
    pub fn next_batch(&self) -> Result<Option<Vec<T>>, SpannerError> {
        let mut lease = ReceiverLease::acquire(Arc::clone(&self.receiver))?;
        let batch_size = self.batch_size;
        let (lease, result) = runtime::run(async move {
            let result = async {
                let receiver = lease.receiver_mut();
                let first = match receiver.recv().await {
                    Some(Ok(row)) => row,
                    Some(Err(error)) => return Err(error),
                    None => return Ok(None),
                };

                let mut batch = Vec::with_capacity(batch_size);
                batch.push(first);
                while batch.len() < batch_size {
                    match receiver.try_recv() {
                        Ok(Ok(row)) => batch.push(row),
                        Ok(Err(error)) => return Err(error),
                        Err(
                            mpsc::error::TryRecvError::Empty
                            | mpsc::error::TryRecvError::Disconnected,
                        ) => break,
                    }
                }
                Ok(Some(batch))
            }
            .await;
            (lease, result)
        })?;
        drop(lease);
        result
    }
}

impl<T: Send + 'static> Drop for StreamingState<T> {
    fn drop(&mut self) {
        // Cancellation must begin before any teardown work. Closing the locally
        // owned receiver then unblocks a reporter waiting behind buffered rows
        // without submitting work to a runtime whose workers may be saturated.
        self.cancellation.cancel();
        if let Some(receiver) = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            receiver.close();
        }

        let reporter_done = self
            .reporter_done
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let completed = reporter_done.is_none_or(|receiver| {
            matches!(
                receiver.recv_timeout(self.shutdown_timeout),
                Ok(()) | Err(std_mpsc::RecvTimeoutError::Disconnected)
            )
        });
        if !completed {
            // A producer that does not observe cancellation cannot block
            // DuckDB teardown indefinitely. Abort only after the cooperative
            // shutdown window has elapsed. Tokio applies abort when the future
            // next yields, so this bounds drop latency but does not claim
            // quiescence for a future that never yields.
            #[cfg(test)]
            if let Some(probe) = self.fallback_probe.take() {
                let _ = probe.send(());
            }
            self.producer_abort.abort();
            self.reporter_abort.abort();
        }
    }
}

async fn send_terminal_error<T>(
    tx: mpsc::Sender<Result<T, SpannerError>>,
    error: SpannerError,
    #[cfg(test)] mut probe: Option<ReporterSendProbe>,
) {
    #[cfg(test)]
    {
        use std::future::Future;
        use std::task::Poll;

        let mut send = Box::pin(tx.send(Err(error)));
        let _ = std::future::poll_fn(|context| match send.as_mut().poll(context) {
            Poll::Pending => {
                if let Some(pending) = probe.as_mut().and_then(|probe| probe.pending.take()) {
                    let _ = pending.send(());
                }
                Poll::Pending
            }
            Poll::Ready(result) => Poll::Ready(result),
        })
        .await;
        if let Some(completed) = probe.as_mut().and_then(|probe| probe.completed.take()) {
            let _ = completed.send(());
        }
    }
    #[cfg(not(test))]
    {
        let _ = tx.send(Err(error)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{sync_channel, SyncSender};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    struct DropSignal(Option<SyncSender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn dropping_state_cancels_a_pending_producer_promptly() {
        let (started_tx, started_rx) = sync_channel(1);
        let (dropped_tx, dropped_rx) = sync_channel(1);
        let state = StreamingState::<usize>::spawn(1, move |_tx, cancellation| async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            started_tx.send(()).unwrap();
            cancellation.cancelled().await;
            Ok(())
        })
        .unwrap();

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("producer did not start");
        drop(state);
        dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("dropping stream state did not abort its producer");
    }

    #[test]
    fn producer_panic_is_reported_as_a_stream_error() {
        let state = StreamingState::<usize>::spawn(1, |_tx, _cancellation| async move {
            if std::hint::black_box(true) {
                panic!("test producer panic");
            }
            Ok(())
        })
        .unwrap();

        let error = state.next_batch().unwrap_err();
        assert!(error.to_string().contains("panicked"));
    }

    #[test]
    fn producer_error_is_reported_as_a_stream_error() {
        let state = StreamingState::<usize>::spawn(1, |_tx, _cancellation| async move {
            Err(SpannerError::Other("test producer error".to_string()))
        })
        .unwrap();

        let error = state.next_batch().unwrap_err();
        assert!(error.to_string().contains("test producer error"));
    }

    #[test]
    fn next_batch_drains_available_rows_after_the_first_row() {
        let (sent_tx, sent_rx) = sync_channel(1);
        let state = StreamingState::spawn(4, move |tx, _cancellation| async move {
            for row in 1..=4 {
                tx.send(Ok(row)).await.unwrap();
            }
            sent_tx.send(()).unwrap();
            Ok(())
        })
        .unwrap();

        sent_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("producer should enqueue the complete batch");
        assert_eq!(state.next_batch().unwrap(), Some(vec![1, 2, 3, 4]));
        assert_eq!(state.next_batch().unwrap(), None);
    }

    fn assert_next_batch_from_tokio_worker() {
        let state = StreamingState::spawn(2, move |tx, _cancellation| async move {
            tx.send(Ok(10)).await.unwrap();
            tx.send(Ok(20)).await.unwrap();
            Ok(())
        })
        .unwrap();

        assert_eq!(state.next_batch().unwrap(), Some(vec![10, 20]));
        assert_eq!(state.next_batch().unwrap(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn next_batch_is_safe_from_a_current_thread_runtime() {
        tokio::task::yield_now().await;
        assert_next_batch_from_tokio_worker();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn next_batch_is_safe_from_a_multithread_runtime_worker() {
        tokio::task::yield_now().await;
        assert_next_batch_from_tokio_worker();
    }

    #[test]
    fn next_batch_restores_receiver_after_owner_reentry_rejection() {
        let state = Arc::new(
            StreamingState::spawn(1, move |tx, cancellation| async move {
                tx.send(Ok(7)).await.unwrap();
                cancellation.cancelled().await;
                Ok(())
            })
            .unwrap(),
        );
        let task_state = Arc::clone(&state);
        let (error_tx, error_rx) = sync_channel(1);
        let task = runtime::spawn(async move {
            let error = task_state.next_batch().unwrap_err();
            error_tx.send(error.to_string()).unwrap();
        })
        .unwrap();

        let error = error_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("owner-runtime reentry did not return an error");
        assert!(error.contains("extension-owned runtime task"), "{error}");
        assert_eq!(state.next_batch().unwrap(), Some(vec![7]));
        drop(task);
        drop(state);
    }

    #[test]
    fn dropping_state_unblocks_a_reporter_with_a_full_row_channel() {
        let (reporter_blocked_tx, reporter_blocked_rx) = sync_channel(1);
        let (reporter_completed_tx, reporter_completed_rx) = sync_channel(1);
        let state = StreamingState::spawn_with_reporter_probe(
            1,
            move |tx, _cancellation| async move {
                tx.send(Ok(1)).await.unwrap();
                Err(SpannerError::Other("producer failed".to_string()))
            },
            reporter_blocked_tx,
            reporter_completed_tx,
        )
        .unwrap();

        // The probe fires only after the terminal send has been polled and
        // returned Pending. With the receiver still open, that proves the row
        // already occupies the channel's sole slot and blocks the reporter.
        reporter_blocked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reporter terminal send was not blocked by the full row channel");

        drop(state);
        reporter_completed_rx
            .try_recv()
            .expect("stream drop returned before the blocked terminal send completed");
    }

    #[test]
    fn dropping_state_quiesces_partition_children_before_returning() {
        let (started_tx, started_rx) = sync_channel(1);
        let (dropped_tx, dropped_rx) = sync_channel(1);
        let state = StreamingState::<usize>::spawn(1, move |_tx, cancellation| async move {
            runtime::run_bounded_partitions([()], 1, cancellation, move |_, _| {
                let started_tx = started_tx.clone();
                let dropped_tx = dropped_tx.clone();
                async move {
                    let _drop_signal = DropSignal(Some(dropped_tx));
                    started_tx.send(()).unwrap();
                    std::future::pending::<Result<(), SpannerError>>().await
                }
            })
            .await
        })
        .unwrap();

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("partition child did not start");
        drop(state);
        dropped_rx
            .try_recv()
            .expect("stream drop returned before its partition child was quiescent");
    }

    #[test]
    fn dropping_state_uses_timeout_fallback_for_a_cancellation_unaware_producer() {
        let (started_tx, started_rx) = sync_channel(1);
        let (dropped_tx, dropped_rx) = sync_channel(1);
        let (fallback_tx, fallback_rx) = sync_channel(1);
        let state = StreamingState::<usize>::spawn_with_shutdown_timeout(
            1,
            move |_tx, _cancellation| async move {
                let _drop_signal = DropSignal(Some(dropped_tx));
                started_tx.send(()).unwrap();
                // This future yields to Tokio but deliberately ignores the
                // cooperative token. A truly non-yielding future cannot be
                // forced to quiesce; the fallback only bounds teardown.
                std::future::pending::<Result<(), SpannerError>>().await
            },
            Duration::from_millis(10),
            fallback_tx,
        )
        .unwrap();

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("producer did not start");

        drop(state);
        fallback_rx
            .try_recv()
            .expect("stream drop did not enter the timeout fallback");
        dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("hard-aborted producer did not yield and drop");
    }

    #[test]
    fn dropping_state_is_bounded_when_owner_workers_do_not_yield_subprocess() {
        runtime::test_support::run(
            "streaming::tests::dropping_state_is_bounded_when_owner_workers_do_not_yield_child",
            Duration::from_secs(5),
        );
    }

    #[test]
    fn dropping_state_is_bounded_when_owner_workers_do_not_yield_child() {
        const TEST_NAME: &str =
            "streaming::tests::dropping_state_is_bounded_when_owner_workers_do_not_yield_child";
        if !runtime::test_support::is_child(TEST_NAME) {
            return;
        }

        let (producer_started_tx, producer_started_rx) = sync_channel(1);
        let (fallback_tx, fallback_rx) = sync_channel(1);
        let state = StreamingState::<usize>::spawn_with_shutdown_timeout(
            1,
            move |_tx, _cancellation| async move {
                producer_started_tx.send(()).unwrap();
                loop {
                    std::hint::spin_loop();
                }
            },
            Duration::from_millis(20),
            fallback_tx,
        )
        .unwrap();
        producer_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("non-yielding producer did not start");

        let additional_workers = runtime::worker_threads().saturating_sub(1);
        let (saturated_tx, saturated_rx) = sync_channel(additional_workers.max(1));
        for _ in 0..additional_workers {
            let saturated_tx = saturated_tx.clone();
            let _task = runtime::spawn(async move {
                saturated_tx.send(()).unwrap();
                loop {
                    std::hint::spin_loop();
                }
            })
            .unwrap();
        }
        for _ in 0..additional_workers {
            saturated_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("runtime worker was not saturated");
        }

        let started = std::time::Instant::now();
        drop(state);
        assert!(started.elapsed() < Duration::from_secs(1));
        fallback_rx
            .try_recv()
            .expect("stream drop did not enter its bounded abort fallback");
        runtime::test_support::finish_child();
    }
}
