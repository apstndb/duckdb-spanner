use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;

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

/// Bounded row stream shared by the query and scan table functions.
///
/// The producer task is retained through its abort handle so DuckDB can stop a
/// blocked Spanner iterator as soon as it drops table-function init data. A
/// small reporter task translates producer errors and panics into channel
/// errors rather than allowing them to look like a successful end of stream.
pub struct StreamingState<T> {
    receiver: Mutex<mpsc::Receiver<Result<T, SpannerError>>>,
    batch_size: usize,
    producer_abort: AbortHandle,
    reporter_abort: AbortHandle,
    cancellation_requested: Arc<AtomicBool>,
    reporter_done: Mutex<Option<std_mpsc::Receiver<()>>>,
}

impl<T> StreamingState<T>
where
    T: Send + 'static,
{
    /// Start a bounded producer and retain cancellation for its lifetime.
    pub fn spawn<F, Fut>(batch_size: usize, producer: F) -> Result<Self, SpannerError>
    where
        F: FnOnce(mpsc::Sender<Result<T, SpannerError>>) -> Fut,
        Fut: std::future::Future<Output = Result<(), SpannerError>> + Send + 'static,
    {
        let (tx, receiver) = mpsc::channel(batch_size);
        let producer = runtime::spawn(producer(tx.clone()))?;
        let producer_abort = producer.abort_handle();
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let reporter_cancellation_requested = cancellation_requested.clone();
        let (reporter_done_tx, reporter_done_rx) = std_mpsc::sync_channel(1);

        let reporter = match runtime::spawn(async move {
            let _completion = CompletionSignal(Some(reporter_done_tx));
            let error = match producer.await {
                Ok(Ok(())) => return,
                Ok(Err(error)) => error,
                Err(error)
                    if error.is_cancelled()
                        && reporter_cancellation_requested.load(Ordering::Acquire) =>
                {
                    return;
                }
                Err(error) if error.is_panic() => {
                    SpannerError::Other(format!("Streaming producer task panicked: {error}"))
                }
                Err(error) => SpannerError::Other(format!(
                    "Streaming producer task ended unexpectedly: {error}"
                )),
            };

            let _ = tx.send(Err(error)).await;
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
            cancellation_requested,
            reporter_done: Mutex::new(Some(reporter_done_rx)),
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
        self.cancellation_requested.store(true, Ordering::Release);
        self.producer_abort.abort();

        let reporter_done = self
            .reporter_done
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let completed = reporter_done.is_none_or(|receiver| {
            matches!(
                receiver.recv_timeout(PRODUCER_SHUTDOWN_TIMEOUT),
                Ok(()) | Err(std_mpsc::RecvTimeoutError::Disconnected)
            )
        });
        if !completed {
            self.reporter_abort.abort();
        }
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
    fn dropping_state_aborts_a_pending_producer_promptly() {
        let (started_tx, started_rx) = sync_channel(1);
        let (dropped_tx, dropped_rx) = sync_channel(1);
        let state = StreamingState::<usize>::spawn(1, move |_tx| async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            started_tx.send(()).unwrap();
            std::future::pending::<Result<(), SpannerError>>().await
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
        let state = StreamingState::<usize>::spawn(1, |_tx| async move {
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
    fn next_batch_drains_available_rows_after_the_first_row() {
        let state = StreamingState::spawn(4, |tx| async move {
            for row in 1..=4 {
                tx.send(Ok(row)).await.unwrap();
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(state.next_batch().unwrap(), Some(vec![1, 2, 3, 4]));
        assert_eq!(state.next_batch().unwrap(), None);
    }
}
