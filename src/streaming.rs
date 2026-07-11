use std::sync::{mpsc as std_mpsc, Mutex};
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

/// Bounded row stream shared by the query and scan table functions.
///
/// On drop, the producer first receives a cancellation token so partition work
/// can quiesce before the stream is released. Its abort handle is only a
/// bounded teardown fallback: Tokio cannot guarantee quiescence for a future
/// that never yields after being aborted. A small reporter task translates
/// producer errors and panics into channel errors rather than allowing them to
/// look like a successful end of stream.
pub struct StreamingState<T> {
    receiver: Mutex<mpsc::Receiver<Result<T, SpannerError>>>,
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
            receiver: Mutex::new(receiver),
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

    /// Receive one blocking row, then drain immediately available rows into a
    /// DuckDB-sized batch. `None` is a clean end of stream.
    pub fn next_batch(&self) -> Result<Option<Vec<T>>, SpannerError> {
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        let first = match receiver.blocking_recv() {
            Some(Ok(row)) => row,
            Some(Err(error)) => return Err(error),
            None => return Ok(None),
        };

        let mut batch = Vec::with_capacity(self.batch_size);
        batch.push(first);
        while batch.len() < self.batch_size {
            match receiver.try_recv() {
                Ok(Ok(row)) => batch.push(row),
                Ok(Err(error)) => return Err(error),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
        Ok(Some(batch))
    }
}

impl<T> Drop for StreamingState<T> {
    fn drop(&mut self) {
        // Close before waiting so a reporter blocked behind buffered rows can
        // finish its terminal-error send and report completion promptly.
        self.receiver
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        self.cancellation.cancel();

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
        let state = StreamingState::spawn(4, |tx, _cancellation| async move {
            for row in 1..=4 {
                tx.send(Ok(row)).await.unwrap();
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(state.next_batch().unwrap(), Some(vec![1, 2, 3, 4]));
        assert_eq!(state.next_batch().unwrap(), None);
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
}
