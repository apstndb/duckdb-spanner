use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, Value};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::paginator::Paginator;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy, RetryPolicyExt};
use google_cloud_longrunning::model::{operation, Operation as InternalOperation};
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
use reqwest::{Method, StatusCode};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::AbortHandle;

use crate::cache::LruCache;
use crate::error::SpannerError;
use crate::{bind_utils, runtime};

// ---------------------------------------------------------------------------
// Admin client cache
// ---------------------------------------------------------------------------

/// The data client uses the same 15-second bound in `src/client.rs`. Keeping
/// admin client construction aligned prevents unreachable ADC or endpoints
/// from hanging a DDL bind indefinitely.
const ADMIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Bound one HTTP/RPC attempt, including reading its response body. Half of
/// the SDK's 60-second retry window leaves room for a second attempt instead
/// of allowing one stalled response to consume the whole logical request.
const ADMIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// The pinned Google Cloud Rust transport defaults retrying requests to at
/// most 10 attempts and 60 seconds. We make both limits explicit and apply the
/// same contract to the emulator REST fallback.
const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const ADMIN_MAX_ATTEMPTS: u32 = 10;

/// Google Cloud Workflows uses 30 minutes as its default Spanner UpdateDdl LRO
/// timeout. Long schema backfills can take hours, so callers that need a longer
/// wait should submit with `spanner_ddl_async` and inspect the operation later.
const DDL_OPERATION_DEADLINE: Duration = Duration::from_secs(30 * 60);

/// Listing is not an LRO and should finish promptly even with pagination. This
/// generous deadline still guarantees that a bad server cannot block forever.
const OPERATIONS_LIST_DEADLINE: Duration = Duration::from_secs(2 * 60);

/// A second independent guard for broken paginators. Spanner retains completed
/// database operations for seven days, so 1,000 server-sized pages is already
/// far beyond normal use while remaining finite.
const MAX_OPERATION_PAGES: usize = 1_000;

const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const POLL_INITIAL_DELAY: Duration = Duration::from_millis(500);
const POLL_MAX_DELAY: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
struct DdlTimeouts {
    connect: Duration,
    attempt: Duration,
    request: Duration,
    operation: Duration,
    list: Duration,
    retry_initial: Duration,
    retry_max: Duration,
    poll_initial: Duration,
    poll_max: Duration,
    max_attempts: u32,
    max_pages: usize,
}

const DEFAULT_TIMEOUTS: DdlTimeouts = DdlTimeouts {
    connect: ADMIN_CONNECT_TIMEOUT,
    attempt: ADMIN_ATTEMPT_TIMEOUT,
    request: ADMIN_REQUEST_TIMEOUT,
    operation: DDL_OPERATION_DEADLINE,
    list: OPERATIONS_LIST_DEADLINE,
    retry_initial: RETRY_INITIAL_DELAY,
    retry_max: RETRY_MAX_DELAY,
    poll_initial: POLL_INITIAL_DELAY,
    poll_max: POLL_MAX_DELAY,
    max_attempts: ADMIN_MAX_ATTEMPTS,
    max_pages: MAX_OPERATION_PAGES,
};

static OPERATION_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
static BACKOFF_SEED_COUNTER: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

fn duration_label(duration: Duration) -> String {
    if duration.subsec_nanos() == 0 {
        format!("{}s", duration.as_secs())
    } else {
        format!("{duration:?}")
    }
}

fn new_operation_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = OPERATION_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "duckdb_spanner_{:x}_{timestamp:x}_{sequence:x}",
        std::process::id()
    )
}

fn next_backoff_seed() -> u64 {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    BACKOFF_SEED_COUNTER.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed) ^ timestamp
}

struct JitteredBackoff {
    initial: Duration,
    maximum: Duration,
    attempt: u32,
    state: u64,
}

impl JitteredBackoff {
    fn new(initial: Duration, maximum: Duration) -> Self {
        Self::with_seed(initial, maximum, next_backoff_seed())
    }

    fn with_seed(initial: Duration, maximum: Duration, seed: u64) -> Self {
        Self {
            initial,
            maximum,
            attempt: 0,
            state: if seed == 0 {
                0xa076_1d64_78bd_642f
            } else {
                seed
            },
        }
    }

    /// Full jitter over a truncated exponential cap, matching the strategy
    /// used by the pinned Google Cloud Rust request backoff.
    fn next_delay(&mut self) -> Duration {
        let exponent = self.attempt.min(31);
        self.attempt = self.attempt.saturating_add(1);
        let cap = self
            .initial
            .saturating_mul(1_u32 << exponent)
            .min(self.maximum);
        let cap_nanos = cap.as_nanos().min(u64::MAX as u128) as u64;
        if cap_nanos == 0 {
            return Duration::ZERO;
        }

        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;

        let jitter_nanos = if cap_nanos == u64::MAX {
            value
        } else {
            value % (cap_nanos + 1)
        };
        Duration::from_nanos(jitter_nanos)
    }
}

type HttpFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Clone)]
struct EmulatorRequest {
    method: Method,
    url: String,
    query: Vec<(String, String)>,
    json: Option<serde_json::Value>,
}

struct EmulatorResponse {
    status: StatusCode,
    body: HttpFuture<Result<String, String>>,
}

trait EmulatorTransport: Send + Sync {
    fn send(&self, request: EmulatorRequest) -> HttpFuture<Result<EmulatorResponse, String>>;
}

#[derive(Clone)]
struct ReqwestEmulatorTransport {
    client: reqwest::Client,
}

impl ReqwestEmulatorTransport {
    fn new(connect_timeout: Duration) -> Result<Self, SpannerError> {
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .build()
            .map_err(|error| {
                SpannerError::Other(format!(
                    "Failed to build emulator admin HTTP client with {} connect timeout: {error}",
                    duration_label(connect_timeout)
                ))
            })?;
        Ok(Self { client })
    }
}

impl EmulatorTransport for ReqwestEmulatorTransport {
    fn send(&self, request: EmulatorRequest) -> HttpFuture<Result<EmulatorResponse, String>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut builder = client
                .request(request.method, request.url)
                .query(&request.query);
            if let Some(json) = request.json {
                builder = builder.json(&json);
            }

            let response = builder
                .send()
                .await
                .map_err(|error| format!("request transport failed: {error}"))?;
            let status = response.status();
            let body = Box::pin(async move {
                response
                    .text()
                    .await
                    .map_err(|error| format!("response body read failed: {error}"))
            });
            Ok(EmulatorResponse { status, body })
        })
    }
}

struct EmulatorResponseData {
    status: StatusCode,
    body: String,
}

enum EmulatorAttemptError {
    Timeout,
    Transport(String),
}

impl std::fmt::Display for EmulatorAttemptError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("attempt timed out while sending or reading body"),
            Self::Transport(error) => formatter.write_str(error),
        }
    }
}

async fn send_emulator_attempt(
    transport: &dyn EmulatorTransport,
    request: EmulatorRequest,
    timeout: Duration,
) -> Result<EmulatorResponseData, EmulatorAttemptError> {
    match tokio::time::timeout(timeout, async {
        let response = transport
            .send(request)
            .await
            .map_err(EmulatorAttemptError::Transport)?;
        let body = response
            .body
            .await
            .map_err(EmulatorAttemptError::Transport)?;
        Ok(EmulatorResponseData {
            status: response.status,
            body,
        })
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(EmulatorAttemptError::Timeout),
    }
}

fn is_ambiguous_http_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn google_error_code_and_message(body: &str) -> Option<(i64, String)> {
    let json: serde_json::Value = serde_json::from_str(body).ok()?;
    let error = json.get("error").unwrap_or(&json);
    Some((
        error.get("code")?.as_i64()?,
        error.get("message")?.as_str()?.to_string(),
    ))
}

fn is_emulator_schema_change_rejection(body: &str) -> bool {
    google_error_code_and_message(body).is_some_and(|(code, message)| {
        code == 9 && message.contains("Schema change operation rejected")
    })
}

fn is_emulator_replay_conflict(status: StatusCode, body: &str) -> bool {
    status == StatusCode::CONFLICT
        && google_error_code_and_message(body).is_some_and(|(code, _)| code == 6)
}

fn body_excerpt(body: &str) -> String {
    const MAX_CHARS: usize = 4_096;
    let mut chars = body.chars();
    let excerpt: String = chars.by_ref().take(MAX_CHARS).collect();
    if chars.next().is_some() {
        format!("{excerpt}...[truncated]")
    } else {
        excerpt
    }
}

async fn send_emulator_request(
    transport: &dyn EmulatorTransport,
    request: EmulatorRequest,
    context: &str,
    timeouts: DdlTimeouts,
) -> Result<EmulatorResponseData, SpannerError> {
    let started = tokio::time::Instant::now();
    let mut backoff = JitteredBackoff::new(timeouts.retry_initial, timeouts.retry_max);
    let mut last_error = "request was not attempted".to_string();

    for attempt in 1..=timeouts.max_attempts {
        let remaining = timeouts.request.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }

        let attempt_timeout = timeouts.attempt.min(remaining);
        match send_emulator_attempt(transport, request.clone(), attempt_timeout).await {
            Ok(response) if response.status.is_success() => return Ok(response),
            Ok(response) => {
                let retryable = is_ambiguous_http_status(response.status);
                let detail = format!(
                    "status={}, body={}",
                    response.status,
                    body_excerpt(&response.body)
                );
                if !retryable {
                    return Err(SpannerError::Other(format!(
                        "{context} failed on attempt {attempt}: {detail}"
                    )));
                }
                last_error = detail;
            }
            Err(error) => {
                // This helper is used only for read-only GET requests.
                last_error = error.to_string();
            }
        }

        if attempt == timeouts.max_attempts {
            break;
        }
        let remaining = timeouts.request.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }
        tokio::time::sleep(backoff.next_delay().min(remaining)).await;
    }

    Err(SpannerError::Other(format!(
        "{context} did not succeed within {} (maximum {} attempts); last error: {last_error}",
        duration_label(timeouts.request),
        timeouts.max_attempts
    )))
}

fn admin_retry_policy() -> impl RetryPolicy {
    Aip194Strict
        .continue_on_client_timeout()
        .continue_on_too_many_requests()
        .with_time_limit(ADMIN_REQUEST_TIMEOUT)
        .with_attempt_limit(ADMIN_MAX_ATTEMPTS)
}

fn bounded_admin_request<T>(request: T) -> T
where
    T: RequestOptionsBuilder,
{
    request
        .with_idempotency(true)
        .with_attempt_timeout(ADMIN_ATTEMPT_TIMEOUT)
        .with_retry_policy(admin_retry_policy())
}

enum AdminRequestError {
    Timeout,
    GoogleCloud(google_cloud_gax::error::Error),
}

async fn send_admin_request<T>(
    request: impl Future<Output = Result<T, google_cloud_gax::error::Error>>,
) -> Result<T, AdminRequestError> {
    match tokio::time::timeout(ADMIN_REQUEST_TIMEOUT, request).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(AdminRequestError::GoogleCloud(error)),
        Err(_) => Err(AdminRequestError::Timeout),
    }
}

fn admin_request_error(context: &str, error: AdminRequestError) -> SpannerError {
    SpannerError::Other(format!("{context}: {}", admin_request_detail(&error)))
}

fn admin_request_detail(error: &AdminRequestError) -> String {
    match error {
        AdminRequestError::Timeout => {
            format!("timed out after {}", duration_label(ADMIN_REQUEST_TIMEOUT))
        }
        AdminRequestError::GoogleCloud(error) => format!(
            "failed within the {} request bound: {error}",
            duration_label(ADMIN_REQUEST_TIMEOUT)
        ),
    }
}

fn admin_request_is_already_exists(error: &AdminRequestError) -> bool {
    matches!(
        error,
        AdminRequestError::GoogleCloud(error)
            if error.status().is_some_and(|status| {
                status.code == google_cloud_gax::error::rpc::Code::AlreadyExists
            })
    )
}

fn admin_request_is_ambiguous(error: &AdminRequestError) -> bool {
    match error {
        AdminRequestError::Timeout => true,
        AdminRequestError::GoogleCloud(error) => {
            // An HTTP response is more specific than the generic transport
            // classification. Scan the whole GAX source chain first so a
            // wrapped terminal 4xx cannot be mistaken for an ambiguous
            // transport failure.
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
            while let Some(current) = source {
                if let Some(status) = current
                    .downcast_ref::<google_cloud_gax::error::Error>()
                    .and_then(google_cloud_gax::error::Error::http_status_code)
                {
                    return gax_submission_signals_are_ambiguous(Some(status), false, None);
                }
                source = current.source();
            }

            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
            while let Some(current) = source {
                if let Some(error) = current.downcast_ref::<google_cloud_gax::error::Error>() {
                    if gax_submission_signals_are_ambiguous(
                        None,
                        error.is_timeout() || error.is_io() || error.is_transport(),
                        error.status().map(|status| status.code),
                    ) {
                        return true;
                    }
                }
                source = current.source();
            }
            false
        }
    }
}

fn gax_submission_signals_are_ambiguous(
    http_status: Option<u16>,
    timeout_io_or_transport: bool,
    rpc_code: Option<google_cloud_gax::error::rpc::Code>,
) -> bool {
    use google_cloud_gax::error::rpc::Code;

    if let Some(status) = http_status {
        return is_ambiguous_submission_http_code(status);
    }
    timeout_io_or_transport
        || matches!(
            rpc_code,
            Some(Code::DeadlineExceeded | Code::Unknown | Code::Internal | Code::Unavailable)
        )
}

fn is_ambiguous_submission_http_code(status: u16) -> bool {
    status == StatusCode::REQUEST_TIMEOUT.as_u16()
        || status == StatusCode::TOO_MANY_REQUESTS.as_u16()
        || (500..600).contains(&status)
}

async fn recover_ambiguous_submission<T>(
    service: &str,
    operation_name: &str,
    submission_detail: &str,
    lookup: impl Future<Output = Result<T, SpannerError>>,
) -> Result<T, SpannerError> {
    lookup.await.map_err(|lookup_error| {
        SpannerError::Other(format!(
            "{service} outcome is ambiguous for operation {operation_name}; submission error: {submission_detail}; deterministic operation lookup failed: {lookup_error}; do not blindly resubmit the DDL"
        ))
    })
}

/// Maximum number of admin client identities retained at once. The key is the
/// same endpoint/auth identity used by the previous unbounded cache.
const ADMIN_CACHE_CAPACITY: usize = 8;
const ADMIN_MAX_CONCURRENT_INITIALIZATIONS: usize = 4;

type InitializationResult<T> = Result<Arc<T>, Arc<str>>;
type InitializationReceiver<T> = watch::Receiver<Option<InitializationResult<T>>>;
type InitializationTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct ClientFlight<T> {
    generation: u64,
    receiver: InitializationReceiver<T>,
}

struct ClientCache<T> {
    completed: LruCache<String, Arc<T>>,
    in_flight: HashMap<String, ClientFlight<T>>,
    next_generation: u64,
    admission: Arc<Semaphore>,
    max_concurrent_initializations: usize,
}

impl<T> ClientCache<T> {
    fn new(completed_capacity: usize, max_concurrent_initializations: usize) -> Self {
        assert!(
            max_concurrent_initializations >= 1,
            "initialization concurrency must be at least 1"
        );
        Self {
            completed: LruCache::new(completed_capacity),
            in_flight: HashMap::new(),
            next_generation: 0,
            admission: Arc::new(Semaphore::new(max_concurrent_initializations)),
            max_concurrent_initializations,
        }
    }

    fn allocate_generation(&mut self) -> u64 {
        self.next_generation = self.next_generation.wrapping_add(1);
        if self.next_generation == 0 {
            self.next_generation = 1;
        }
        self.next_generation
    }
}

enum ClientCacheLookup<T> {
    Completed(Arc<T>),
    InFlight(InitializationReceiver<T>),
    Start {
        receiver: InitializationReceiver<T>,
        sender: watch::Sender<Option<InitializationResult<T>>>,
        generation: u64,
        permit: OwnedSemaphorePermit,
    },
}

fn lookup_cached_client<T>(
    cache: &Mutex<ClientCache<T>>,
    cache_key: &String,
) -> Result<ClientCacheLookup<T>, SpannerError> {
    let mut cache = cache.lock().unwrap_or_else(|error| error.into_inner());
    if let Some(client) = cache.completed.get(cache_key) {
        return Ok(ClientCacheLookup::Completed(client));
    }
    if let Some(flight) = cache.in_flight.get(cache_key) {
        return Ok(ClientCacheLookup::InFlight(flight.receiver.clone()));
    }

    let permit = Arc::clone(&cache.admission)
        .try_acquire_owned()
        .map_err(|error| {
            SpannerError::Other(format!(
                "Spanner admin client initialization capacity is full (maximum {} unique in-flight endpoints; endpoint={cache_key}): {error}",
                cache.max_concurrent_initializations
            ))
        })?;
    let (sender, receiver) = watch::channel(None);
    let generation = cache.allocate_generation();
    cache.in_flight.insert(
        cache_key.clone(),
        ClientFlight {
            generation,
            receiver: receiver.clone(),
        },
    );
    Ok(ClientCacheLookup::Start {
        receiver,
        sender,
        generation,
        permit,
    })
}

fn flight_is_current<T>(cache: &ClientCache<T>, cache_key: &str, generation: u64) -> bool {
    cache
        .in_flight
        .get(cache_key)
        .is_some_and(|flight| flight.generation == generation)
}

struct FlightCleanupGuard<T> {
    cache: Arc<Mutex<ClientCache<T>>>,
    cache_key: String,
    generation: u64,
    sender: Option<watch::Sender<Option<InitializationResult<T>>>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<T> FlightCleanupGuard<T> {
    fn new(
        cache: Arc<Mutex<ClientCache<T>>>,
        cache_key: String,
        generation: u64,
        sender: watch::Sender<Option<InitializationResult<T>>>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            cache,
            cache_key,
            generation,
            sender: Some(sender),
            permit: Some(permit),
        }
    }

    fn finish(mut self, result: InitializationResult<T>) {
        {
            let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
            if flight_is_current(&cache, &self.cache_key, self.generation) {
                cache.in_flight.remove(&self.cache_key);
                if let Ok(client) = &result {
                    drop(
                        cache
                            .completed
                            .insert(self.cache_key.clone(), Arc::clone(client)),
                    );
                }
            }
            // Keep removal and admission release atomic with respect to new
            // lookups. A released permit may wake another task, but it cannot
            // inspect the cache until this mutex is unlocked.
            drop(self.permit.take());
        }
        // Notify only after both the cache state and admission state agree.
        if let Some(sender) = self.sender.take() {
            sender.send_replace(Some(result));
        }
    }
}

impl<T> Drop for FlightCleanupGuard<T> {
    fn drop(&mut self) {
        if self.sender.is_none() {
            return;
        }
        {
            let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
            if flight_is_current(&cache, &self.cache_key, self.generation) {
                cache.in_flight.remove(&self.cache_key);
            }
            // Keep removal and admission release atomic with respect to new
            // lookups, including lookups that were not waiting on this flight.
            drop(self.permit.take());
        }
        // Close the old flight after the matching generation has been removed
        // and admission is available. This guard cannot remove a newer flight.
        drop(self.sender.take());
    }
}

fn spawn_client_initialization<T, F, Fut>(
    cache: Arc<Mutex<ClientCache<T>>>,
    cache_key: String,
    generation: u64,
    permit: OwnedSemaphorePermit,
    sender: watch::Sender<Option<InitializationResult<T>>>,
    initialize: F,
) -> Result<AbortHandle, SpannerError>
where
    T: Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<Arc<T>, SpannerError>> + Send + 'static,
{
    spawn_client_initialization_with(
        cache,
        cache_key,
        generation,
        permit,
        sender,
        initialize,
        |future| {
            let task = runtime::spawn(future)?;
            let abort_handle = task.abort_handle();
            drop(task);
            Ok(abort_handle)
        },
    )
}

fn spawn_client_initialization_with<T, F, Fut, S>(
    cache: Arc<Mutex<ClientCache<T>>>,
    cache_key: String,
    generation: u64,
    permit: OwnedSemaphorePermit,
    sender: watch::Sender<Option<InitializationResult<T>>>,
    initialize: F,
    spawn: S,
) -> Result<AbortHandle, SpannerError>
where
    T: Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<Arc<T>, SpannerError>> + Send + 'static,
    S: FnOnce(InitializationTask) -> Result<AbortHandle, SpannerError>,
{
    let task_cache = Arc::clone(&cache);
    let task_key = cache_key.clone();
    // Construct the guard before spawning. If the task is aborted before its
    // first poll, dropping the unpolled future still removes this generation,
    // closes its receiver, and releases its admission permit.
    let guard = FlightCleanupGuard::new(task_cache, task_key, generation, sender, permit);
    spawn(Box::pin(async move {
        let result = initialize()
            .await
            .map_err(|error| Arc::<str>::from(error.to_string()));
        guard.finish(result);
    }))
}

async fn wait_for_cached_client<T>(
    mut receiver: InitializationReceiver<T>,
    timeout: Duration,
    cache_key: &str,
) -> Result<Arc<T>, SpannerError> {
    let wait = async {
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result.map_err(|error| SpannerError::Other(error.to_string()));
            }
            receiver.changed().await.map_err(|_| {
                SpannerError::Other(format!(
                    "Spanner admin client initialization ended without a result (endpoint={cache_key})"
                ))
            })?;
        }
    };

    tokio::time::timeout(timeout, wait).await.map_err(|_| {
        SpannerError::Other(format!(
            "Timed out waiting for Spanner admin client initialization after {} (endpoint={cache_key})",
            duration_label(timeout)
        ))
    })?
}

async fn get_or_create_cached_client<T, F, Fut>(
    cache: Arc<Mutex<ClientCache<T>>>,
    cache_key: String,
    timeout: Duration,
    initialize: F,
) -> Result<Arc<T>, SpannerError>
where
    T: Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<Arc<T>, SpannerError>> + Send + 'static,
{
    let receiver = match lookup_cached_client(&cache, &cache_key)? {
        ClientCacheLookup::Completed(client) => return Ok(client),
        ClientCacheLookup::InFlight(receiver) => receiver,
        ClientCacheLookup::Start {
            receiver,
            sender,
            generation,
            permit,
        } => {
            spawn_client_initialization(
                Arc::clone(&cache),
                cache_key.clone(),
                generation,
                permit,
                sender,
                initialize,
            )?;
            receiver
        }
    };

    wait_for_cached_client(receiver, timeout, &cache_key).await
}

static ADMIN_CLIENT_CACHE: LazyLock<Arc<Mutex<ClientCache<DatabaseAdmin>>>> = LazyLock::new(|| {
    Arc::new(Mutex::new(ClientCache::new(
        ADMIN_CACHE_CAPACITY,
        ADMIN_MAX_CONCURRENT_INITIALIZATIONS,
    )))
});

/// Get or create a Spanner Admin client.
///
/// Admin clients are NOT per-database (unlike data clients). The cache key is
/// the endpoint string (or empty for default).
async fn get_or_create_admin_client(
    admin_endpoint: Option<&str>,
) -> Result<Arc<DatabaseAdmin>, SpannerError> {
    let cache_key = admin_endpoint.unwrap_or("").to_string();
    let owned_endpoint = admin_endpoint.map(str::to_string);
    get_or_create_cached_client(
        Arc::clone(&ADMIN_CLIENT_CACHE),
        cache_key,
        DEFAULT_TIMEOUTS.connect,
        move || async move { create_admin_client(owned_endpoint.as_deref()).await },
    )
    .await
}

async fn create_admin_client(
    admin_endpoint: Option<&str>,
) -> Result<Arc<DatabaseAdmin>, SpannerError> {
    let mut builder = DatabaseAdmin::builder();
    let emulator_endpoint = admin_endpoint
        .map(str::to_owned)
        .or_else(|| std::env::var("SPANNER_EMULATOR_HOST").ok());
    if let Some(ep) = emulator_endpoint {
        builder = builder
            .with_endpoint(admin_endpoint_url(&ep))
            .with_credentials(Anonymous::new().build());
    }

    match tokio::time::timeout(DEFAULT_TIMEOUTS.connect, builder.build()).await {
        Ok(Ok(client)) => Ok(Arc::new(client)),
        Ok(Err(error)) => Err(SpannerError::Other(format!(
            "Failed to build Spanner admin client within the {} connect bound (endpoint={}): {error}",
            duration_label(DEFAULT_TIMEOUTS.connect),
            admin_endpoint.unwrap_or("<default>")
        ))),
        Err(_) => Err(SpannerError::Other(format!(
            "Timed out building Spanner admin client after {} (endpoint={})",
            duration_label(DEFAULT_TIMEOUTS.connect),
            admin_endpoint.unwrap_or("<default>")
        ))),
    }
}

fn emulator_endpoint(endpoint: Option<&str>) -> Option<String> {
    endpoint
        .map(str::to_owned)
        .or_else(|| std::env::var("SPANNER_EMULATOR_HOST").ok())
}

fn emulator_admin_endpoint(endpoint: Option<&str>, admin_endpoint: Option<&str>) -> Option<String> {
    admin_endpoint
        .map(admin_endpoint_url)
        .or_else(|| emulator_endpoint(endpoint).map(|ep| admin_endpoint_url(&ep)))
}

async fn update_database_ddl(
    admin: &DatabaseAdmin,
    database_path: String,
    statements: Vec<String>,
    endpoint: Option<&str>,
    admin_endpoint: Option<&str>,
) -> Result<InternalOperation, SpannerError> {
    let operation_id = new_operation_id();
    if let Some(admin_endpoint) = emulator_admin_endpoint(endpoint, admin_endpoint) {
        return update_emulator_database_ddl(
            &admin_endpoint,
            &database_path,
            statements,
            &operation_id,
        )
        .await;
    }

    update_real_database_ddl(admin, database_path, statements, operation_id).await
}

async fn wait_database_operation(
    admin: &DatabaseAdmin,
    operation_name: &str,
    endpoint: Option<&str>,
    admin_endpoint: Option<&str>,
    remaining: Duration,
) -> Result<InternalOperation, SpannerError> {
    wait_with_operation_deadline(
        operation_name,
        remaining,
        DEFAULT_TIMEOUTS.operation,
        async {
            if let Some(admin_endpoint) = emulator_admin_endpoint(endpoint, admin_endpoint) {
                wait_emulator_operation(&admin_endpoint, operation_name).await
            } else {
                wait_operation(admin, operation_name).await
            }
        },
    )
    .await
}

async fn update_real_database_ddl(
    admin: &DatabaseAdmin,
    database_path: String,
    statements: Vec<String>,
    operation_id: String,
) -> Result<InternalOperation, SpannerError> {
    let operation_name = format!("{database_path}/operations/{operation_id}");
    let request = bounded_admin_request(
        admin
            .update_database_ddl()
            .set_database(database_path)
            .set_statements(statements)
            .set_operation_id(operation_id),
    );

    match send_admin_request(request.send()).await {
        Ok(operation) => {
            validate_operation(
                &operation,
                Some(&operation_name),
                "Spanner UpdateDatabaseDdl",
            )?;
            Ok(operation)
        }
        Err(error) if admin_request_is_already_exists(&error) => {
            // The explicit operation ID makes retries replay-safe. If a prior
            // attempt reached Spanner but its response was lost, the replay
            // returns ALREADY_EXISTS; recover the operation by its known name.
            let update_error = admin_request_detail(&error);
            get_real_operation(admin, &operation_name).await.map_err(|lookup_error| {
                SpannerError::Other(format!(
                    "Spanner UpdateDatabaseDdl replay returned ALREADY_EXISTS for {operation_name}; update error: {update_error}; operation lookup failed: {lookup_error}"
                ))
            })
        }
        Err(error) if admin_request_is_ambiguous(&error) => {
            let detail = admin_request_detail(&error);
            recover_ambiguous_submission(
                "Spanner UpdateDatabaseDdl",
                &operation_name,
                &detail,
                get_real_operation(admin, &operation_name),
            )
            .await
        }
        Err(error) => Err(SpannerError::Other(format!(
            "Spanner UpdateDatabaseDdl failed for operation {operation_name}: {}",
            admin_request_detail(&error)
        ))),
    }
}

async fn update_emulator_database_ddl(
    admin_endpoint: &str,
    database_path: &str,
    statements: Vec<String>,
    operation_id: &str,
) -> Result<InternalOperation, SpannerError> {
    let transport = ReqwestEmulatorTransport::new(DEFAULT_TIMEOUTS.connect)?;
    update_emulator_database_ddl_with(
        &transport,
        admin_endpoint,
        database_path,
        statements,
        operation_id,
        DEFAULT_TIMEOUTS,
    )
    .await
}

async fn update_emulator_database_ddl_with(
    transport: &dyn EmulatorTransport,
    admin_endpoint: &str,
    database_path: &str,
    statements: Vec<String>,
    operation_id: &str,
    timeouts: DdlTimeouts,
) -> Result<InternalOperation, SpannerError> {
    // google-cloud-rust's generated REST client currently sends system query
    // parameters that the Spanner emulator rejects for UpdateDatabaseDdl.
    let url = format!(
        "{}/v1/{}/ddl",
        admin_endpoint.trim_end_matches('/'),
        database_path
    );
    let operation_name = format!("{database_path}/operations/{operation_id}");
    let request = EmulatorRequest {
        method: Method::PATCH,
        url,
        query: Vec::new(),
        json: Some(serde_json::json!({
            "statements": statements,
            "operationId": operation_id,
        })),
    };
    let started = tokio::time::Instant::now();
    let mut backoff = JitteredBackoff::new(timeouts.retry_initial, timeouts.retry_max);

    for attempt in 1..=timeouts.max_attempts {
        let remaining = timeouts.request.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(SpannerError::Other(format!(
                "Emulator UpdateDatabaseDdl was definitively rejected by schema contention for operation {operation_name} until the {} submission bound expired",
                duration_label(timeouts.request)
            )));
        }

        let attempt_timeout = timeouts.attempt.min(remaining);
        let response = match send_emulator_attempt(transport, request.clone(), attempt_timeout)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let detail = format!("attempt {attempt}: {error}");
                return recover_ambiguous_submission(
                    "Emulator UpdateDatabaseDdl",
                    &operation_name,
                    &detail,
                    fetch_emulator_operation(transport, admin_endpoint, &operation_name, timeouts),
                )
                .await;
            }
        };

        if response.status.is_success() {
            match operation_from_json(&response.body).and_then(|operation| {
                validate_operation(
                    &operation,
                    Some(&operation_name),
                    "emulator UpdateDatabaseDdl",
                )?;
                Ok(operation)
            }) {
                Ok(operation) => return Ok(operation),
                Err(error) => {
                    let detail =
                        format!("attempt {attempt} returned an unusable success response: {error}");
                    return recover_ambiguous_submission(
                        "Emulator UpdateDatabaseDdl",
                        &operation_name,
                        &detail,
                        fetch_emulator_operation(
                            transport,
                            admin_endpoint,
                            &operation_name,
                            timeouts,
                        ),
                    )
                    .await;
                }
            }
        }

        if is_emulator_replay_conflict(response.status, &response.body) {
            return fetch_emulator_operation(
                transport,
                admin_endpoint,
                &operation_name,
                timeouts,
            )
            .await
            .map_err(|error| {
                SpannerError::Other(format!(
                    "Emulator UpdateDatabaseDdl returned ALREADY_EXISTS for operation {operation_name}; operation lookup failed: {error}; submission body={}",
                    body_excerpt(&response.body)
                ))
            });
        }

        let detail = format!(
            "attempt {attempt}: status={}, body={}",
            response.status,
            body_excerpt(&response.body)
        );
        if is_ambiguous_http_status(response.status) {
            return recover_ambiguous_submission(
                "Emulator UpdateDatabaseDdl",
                &operation_name,
                &detail,
                fetch_emulator_operation(transport, admin_endpoint, &operation_name, timeouts),
            )
            .await;
        }

        if !is_emulator_schema_change_rejection(&response.body) {
            return Err(SpannerError::Other(format!(
                "Emulator UpdateDatabaseDdl failed for operation {operation_name}: {detail}"
            )));
        }

        if attempt == timeouts.max_attempts {
            return Err(SpannerError::Other(format!(
                "Emulator UpdateDatabaseDdl was definitively rejected by schema contention for operation {operation_name} after {attempt} attempts; last rejection: {detail}"
            )));
        }
        let remaining = timeouts.request.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            continue;
        }
        tokio::time::sleep(backoff.next_delay().min(remaining)).await;
    }

    unreachable!("submission loop returns on every terminal condition")
}

async fn wait_emulator_operation(
    admin_endpoint: &str,
    operation_name: &str,
) -> Result<InternalOperation, SpannerError> {
    let transport = ReqwestEmulatorTransport::new(DEFAULT_TIMEOUTS.connect)?;
    wait_emulator_operation_with(&transport, admin_endpoint, operation_name, DEFAULT_TIMEOUTS).await
}

async fn wait_emulator_operation_with(
    transport: &dyn EmulatorTransport,
    admin_endpoint: &str,
    operation_name: &str,
    timeouts: DdlTimeouts,
) -> Result<InternalOperation, SpannerError> {
    let mut poll_backoff = JitteredBackoff::new(timeouts.poll_initial, timeouts.poll_max);
    loop {
        let op =
            fetch_emulator_operation(transport, admin_endpoint, operation_name, timeouts).await?;
        if op.done {
            return Ok(op);
        }

        tokio::time::sleep(poll_backoff.next_delay()).await;
    }
}

async fn fetch_emulator_operation(
    transport: &dyn EmulatorTransport,
    admin_endpoint: &str,
    operation_name: &str,
    timeouts: DdlTimeouts,
) -> Result<InternalOperation, SpannerError> {
    let request = EmulatorRequest {
        method: Method::GET,
        url: format!(
            "{}/v1/{}",
            admin_endpoint.trim_end_matches('/'),
            operation_name
        ),
        query: Vec::new(),
        json: None,
    };
    let response =
        send_emulator_request(transport, request, "Emulator GetOperation", timeouts).await?;
    let operation = operation_from_json(&response.body)?;
    validate_operation(&operation, Some(operation_name), "emulator GetOperation")?;
    Ok(operation)
}

fn operation_from_json(body: &str) -> Result<InternalOperation, SpannerError> {
    let json: serde_json::Value = serde_json::from_str(body).map_err(|error| {
        SpannerError::Other(format!(
            "Emulator operation protocol error: malformed JSON: {error}; body={}",
            body_excerpt(body)
        ))
    })?;
    operation_from_value(json, body)
}

fn operation_from_value(
    json: serde_json::Value,
    source: &str,
) -> Result<InternalOperation, SpannerError> {
    let object = json.as_object().ok_or_else(|| {
        SpannerError::Other(format!(
            "Emulator operation protocol error: operation must be a JSON object; body={}",
            body_excerpt(source)
        ))
    })?;
    let name = object
        .get("name")
        .and_then(serde_json::Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            SpannerError::Other(format!(
                "Emulator operation protocol error: missing or empty string name; body={}",
                body_excerpt(source)
            ))
        })?
        .to_string();
    let done = match object.get("done") {
        // Protobuf JSON legitimately omits scalar fields at their default.
        None => false,
        Some(value) => value.as_bool().ok_or_else(|| {
            SpannerError::Other(format!(
                "Emulator operation protocol error: operation {name} has non-boolean done; body={}",
                body_excerpt(source)
            ))
        })?,
    };
    let has_error = object.contains_key("error");
    let has_response = object.contains_key("response");
    if has_error && has_response {
        return Err(SpannerError::Other(format!(
            "Emulator operation protocol error: operation {name} has both error and response"
        )));
    }
    if !done && (has_error || has_response) {
        return Err(SpannerError::Other(format!(
            "Emulator operation protocol error: unfinished operation {name} has a result"
        )));
    }

    let operation: InternalOperation = serde_json::from_value(json).map_err(|error| {
        SpannerError::Other(format!(
            "Emulator operation protocol error: malformed operation {name}: {error}; body={}",
            body_excerpt(source)
        ))
    })?;
    validate_operation(&operation, None, "emulator operation response")?;
    Ok(operation)
}

fn validate_operation(
    operation: &InternalOperation,
    expected_name: Option<&str>,
    source: &str,
) -> Result<(), SpannerError> {
    if operation.name.is_empty() {
        return Err(SpannerError::Other(format!(
            "{source} protocol error: operation name is empty"
        )));
    }
    if let Some(expected_name) = expected_name {
        if operation.name != expected_name {
            return Err(SpannerError::Other(format!(
                "{source} protocol error: expected operation {expected_name}, got {}",
                operation.name
            )));
        }
    }
    if !operation.done && operation.result.is_some() {
        return Err(SpannerError::Other(format!(
            "{source} protocol error: unfinished operation {} has a result",
            operation.name
        )));
    }
    Ok(())
}

fn operation_deadline_error(operation_name: &str, deadline: Duration) -> SpannerError {
    SpannerError::Other(format!(
        "Spanner DDL operation {operation_name} did not complete within the {} overall deadline; the server-side operation may still be running",
        duration_label(deadline)
    ))
}

async fn wait_with_operation_deadline<T>(
    operation_name: &str,
    remaining: Duration,
    overall_deadline: Duration,
    wait: impl Future<Output = Result<T, SpannerError>>,
) -> Result<T, SpannerError> {
    match tokio::time::timeout(remaining, wait).await {
        Ok(result) => result,
        Err(_) => Err(operation_deadline_error(operation_name, overall_deadline)),
    }
}

fn ddl_operation_error(op: &InternalOperation) -> Option<String> {
    op.error().map(|status| {
        format!(
            "Spanner DDL operation {} failed: code={}, message={}",
            op.name, status.code, status.message
        )
    })
}

// ---------------------------------------------------------------------------
// Common named parameters for DDL / operations VTabs
// ---------------------------------------------------------------------------

fn database_named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    vec![
        (
            "database_path".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "project".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "instance".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "database".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "endpoint".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "admin_endpoint".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
    ]
}

fn ddl_named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    let mut params = database_named_parameters();
    params.push((
        "statements".to_string(),
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
    ));
    params
}

fn ddl_statements_from_bind(bind: &BindInfo) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sql = bind.get_parameter(0);
    let statements = bind.get_named_parameter("statements");
    ddl_statements_from_values(&sql, statements.as_ref())
}

fn ddl_statements_from_values(
    sql: &Value,
    statements: Option<&Value>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sql_is_set = !sql.is_null();
    let statements_is_set = statements.is_some_and(|value| !value.is_null());

    match (sql_is_set, statements_is_set) {
        (true, false) => ddl_statements_from_value(sql),
        (false, true) => ddl_statements_from_value(statements.expect("checked above")),
        (true, true) => Err("specify either sql or statements, not both".into()),
        (false, false) => Err("spanner_ddl requires sql or statements".into()),
    }
}

fn ddl_statements_from_value(value: &Value) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if let Some(list) = value.to_list() {
        return ddl_statements_from_list(list);
    }
    if value.logical_type_id() != LogicalTypeId::Varchar {
        return Err(format!(
            "spanner_ddl sql must be VARCHAR or statements must be LIST<VARCHAR>, not {:?}",
            value.logical_type_id()
        )
        .into());
    }

    let statement = value.to_string();
    validate_ddl_statement(&statement)?;
    Ok(vec![statement])
}

fn ddl_statements_from_list(values: Vec<Value>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if values.is_empty() {
        return Err("spanner_ddl statements must contain at least one statement".into());
    }

    values
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            if value.is_null() {
                return Err(format!("spanner_ddl statements[{idx}] must not be NULL").into());
            }
            if value.logical_type_id() != LogicalTypeId::Varchar {
                return Err(format!(
                    "spanner_ddl statements[{idx}] must be VARCHAR, not {:?}",
                    value.logical_type_id()
                )
                .into());
            }
            let statement = value.to_string();
            validate_ddl_statement(&statement)?;
            Ok(statement)
        })
        .collect()
}

fn validate_ddl_statement(statement: &str) -> Result<(), Box<dyn std::error::Error>> {
    if statement.trim().is_empty() {
        return Err("spanner_ddl statements must not be empty".into());
    }
    Ok(())
}

fn cached_init_result<T>(
    cache: &Mutex<Option<Result<T, String>>>,
    run: impl FnOnce() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: Clone,
{
    let mut cached = cache.lock().unwrap_or_else(|e| e.into_inner());
    if cached.is_none() {
        *cached = Some(run().map_err(|e| e.to_string()));
    }

    match cached.as_ref().expect("cache populated above") {
        Ok(value) => Ok(value.clone()),
        Err(err) => Err(err.clone().into()),
    }
}

// ---------------------------------------------------------------------------
// SpannerDdlVTab — synchronous DDL execution
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DdlBindData {
    database_path: String,
    statements: Vec<String>,
    endpoint: Option<String>,
    admin_endpoint: Option<String>,
    // DuckDB may call init() more than once for one bound table function.
    // Cache the full init outcome so repeated callers do not resend DDL and
    // see the same success or error from the first execution.
    cached_result: Arc<Mutex<Option<Result<DdlResult, String>>>>,
}

pub struct DdlInitData {
    result: Mutex<Option<DdlResult>>,
}

#[derive(Clone)]
struct DdlResult {
    operation_name: String,
    done: bool,
    duration_secs: f64,
}

pub struct SpannerDdlVTab;

impl VTab for SpannerDdlVTab {
    type BindData = DdlBindData;
    type InitData = DdlInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let statements = ddl_statements_from_bind(bind)?;
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);
        let admin_endpoint = bind_utils::resolve_admin_endpoint(bind);

        bind.add_result_column(
            "operation_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column(
            "duration_secs",
            LogicalTypeHandle::from(LogicalTypeId::Double),
        );

        Ok(DdlBindData {
            database_path,
            statements,
            endpoint,
            admin_endpoint,
            cached_result: Arc::new(Mutex::new(None)),
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlBindData>() };

        let result = cached_init_result(&bind_data.cached_result, || {
            let database_path = bind_data.database_path.clone();
            let statements = bind_data.statements.clone();
            let endpoint = bind_data.endpoint.clone();
            let admin_endpoint = bind_data.admin_endpoint.clone();
            let effective_admin_endpoint =
                emulator_admin_endpoint(endpoint.as_deref(), admin_endpoint.as_deref());

            runtime::run(async move {
                let overall_start = Instant::now();
                let admin = get_or_create_admin_client(effective_admin_endpoint.as_deref()).await?;

                let start = Instant::now();
                let op = update_database_ddl(
                    &admin,
                    database_path,
                    statements,
                    endpoint.as_deref(),
                    admin_endpoint.as_deref(),
                )
                .await?;

                let operation_name = op.name.clone();
                let remaining = DEFAULT_TIMEOUTS
                    .operation
                    .checked_sub(overall_start.elapsed())
                    .ok_or_else(|| {
                        operation_deadline_error(&operation_name, DEFAULT_TIMEOUTS.operation)
                    })?;
                let op = wait_database_operation(
                    &admin,
                    &operation_name,
                    endpoint.as_deref(),
                    admin_endpoint.as_deref(),
                    remaining,
                )
                .await?;
                if let Some(err) = ddl_operation_error(&op) {
                    return Err(SpannerError::Other(err));
                }

                let duration_secs = start.elapsed().as_secs_f64();

                Ok::<DdlResult, SpannerError>(DdlResult {
                    operation_name,
                    done: op.done,
                    duration_secs,
                })
            })?
            .map_err(Into::into)
        })?;

        init.set_max_threads(1);

        Ok(DdlInitData {
            result: Mutex::new(Some(result)),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        let result = init_data
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        match result {
            Some(r) => {
                let col_name = output.flat_vector(0);
                col_name.insert(0, &r.operation_name);
                let mut col_done = output.flat_vector(1);
                let mut col_duration = output.flat_vector(2);
                // SAFETY: the bind phase registers these columns as BOOLEAN and DOUBLE,
                // and index 0 is within the single-row output batch.
                unsafe {
                    col_done.as_mut_slice::<bool>()[0] = r.done;
                    col_duration.as_mut_slice::<f64>()[0] = r.duration_secs;
                }
                output.set_len(1);
            }
            None => {
                output.set_len(0);
            }
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Any), // sql TEXT or direct LIST<VARCHAR>
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(ddl_named_parameters())
    }
}

// ---------------------------------------------------------------------------
// SpannerDdlAsyncVTab — asynchronous DDL execution (returns immediately)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DdlAsyncBindData {
    database_path: String,
    statements: Vec<String>,
    endpoint: Option<String>,
    admin_endpoint: Option<String>,
    // See the comment on `DdlBindData::cached_result`.
    cached_result: Arc<Mutex<Option<Result<DdlAsyncResult, String>>>>,
}

pub struct DdlAsyncInitData {
    result: Mutex<Option<DdlAsyncResult>>,
}

#[derive(Clone)]
struct DdlAsyncResult {
    operation_name: String,
    done: bool,
}

pub struct SpannerDdlAsyncVTab;

impl VTab for SpannerDdlAsyncVTab {
    type BindData = DdlAsyncBindData;
    type InitData = DdlAsyncInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let statements = ddl_statements_from_bind(bind)?;
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);
        let admin_endpoint = bind_utils::resolve_admin_endpoint(bind);

        bind.add_result_column(
            "operation_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));

        Ok(DdlAsyncBindData {
            database_path,
            statements,
            endpoint,
            admin_endpoint,
            cached_result: Arc::new(Mutex::new(None)),
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlAsyncBindData>() };

        let result = cached_init_result(&bind_data.cached_result, || {
            let database_path = bind_data.database_path.clone();
            let statements = bind_data.statements.clone();
            let endpoint = bind_data.endpoint.clone();
            let admin_endpoint = bind_data.admin_endpoint.clone();
            let effective_admin_endpoint =
                emulator_admin_endpoint(endpoint.as_deref(), admin_endpoint.as_deref());

            runtime::run(async move {
                let admin = get_or_create_admin_client(effective_admin_endpoint.as_deref()).await?;

                let op = update_database_ddl(
                    &admin,
                    database_path,
                    statements,
                    endpoint.as_deref(),
                    admin_endpoint.as_deref(),
                )
                .await?;
                if let Some(err) = ddl_operation_error(&op) {
                    return Err(SpannerError::Other(err));
                }

                Ok::<DdlAsyncResult, SpannerError>(DdlAsyncResult {
                    operation_name: op.name,
                    done: op.done,
                })
            })?
            .map_err(Into::into)
        })?;

        init.set_max_threads(1);

        Ok(DdlAsyncInitData {
            result: Mutex::new(Some(result)),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        let result = init_data
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        match result {
            Some(r) => {
                let col_name = output.flat_vector(0);
                col_name.insert(0, &r.operation_name);
                let mut col_done = output.flat_vector(1);
                // SAFETY: the bind phase registers this column as BOOLEAN, and index 0
                // is within the single-row output batch.
                unsafe {
                    col_done.as_mut_slice::<bool>()[0] = r.done;
                }
                output.set_len(1);
            }
            None => {
                output.set_len(0);
            }
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Any), // sql TEXT or direct LIST<VARCHAR>
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(ddl_named_parameters())
    }
}

// ---------------------------------------------------------------------------
// SpannerOperationsVTab — list database long-running operations
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct OperationsBindData {
    database_path: String,
    endpoint: Option<String>,
    admin_endpoint: Option<String>,
    filter: Option<String>,
}

pub struct OperationsInitData {
    operations: Mutex<Vec<OperationRow>>,
}

struct OperationRow {
    name: String,
    done: bool,
    metadata_type: String,
    // NULL (not 0 / "") when the operation has no error, i.e. it either
    // hasn't finished yet or finished successfully.
    error_code: Option<i32>,
    error_message: Option<String>,
}

pub struct SpannerOperationsVTab;

impl VTab for SpannerOperationsVTab {
    type BindData = OperationsBindData;
    type InitData = OperationsInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);
        let admin_endpoint = bind_utils::resolve_admin_endpoint(bind);
        let filter = bind_utils::get_named_string(bind, "filter");

        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column(
            "metadata_type",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "error_code",
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        );
        bind.add_result_column(
            "error_message",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        Ok(OperationsBindData {
            database_path,
            endpoint,
            admin_endpoint,
            filter,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<OperationsBindData>() };

        let database_path = bind_data.database_path.clone();
        let endpoint = bind_data.endpoint.clone();
        let admin_endpoint = bind_data.admin_endpoint.clone();
        let filter = bind_data.filter.clone();

        let ops = runtime::run(async move {
            list_database_operations(
                &database_path,
                endpoint.as_deref(),
                admin_endpoint.as_deref(),
                filter.as_deref(),
            )
            .await
        })??;

        init.set_max_threads(1);

        Ok(OperationsInitData {
            operations: Mutex::new(ops),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        let mut ops = init_data
            .operations
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        if ops.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        // Drain at most one DuckDB data chunk per call.
        let batch_size = ops.len().min(crate::vector_size::runtime_vector_size());
        let batch: Vec<OperationRow> = ops.drain(..batch_size).collect();

        let col_name = output.flat_vector(0);
        let mut col_done = output.flat_vector(1);
        let col_metadata_type = output.flat_vector(2);
        let mut col_error_code = output.flat_vector(3);
        let mut col_error_message = output.flat_vector(4);

        // SAFETY: the bind phase registers `done` as BOOLEAN, and the loop
        // only writes indices within the drained batch size.
        let done_values = unsafe { col_done.as_mut_slice::<bool>() };
        for (i, row) in batch.iter().enumerate() {
            col_name.insert(i, &row.name);
            done_values[i] = row.done;
            col_metadata_type.insert(i, &row.metadata_type);
            if row.error_code.is_none() {
                col_error_code.set_null(i);
            }
            match &row.error_message {
                Some(msg) => col_error_message.insert(i, msg),
                None => col_error_message.set_null(i),
            }
        }
        // SAFETY: the bind phase registers `error_code` as INTEGER, and the loop
        // only writes indices within the drained batch size.
        let error_code_values = unsafe { col_error_code.as_mut_slice::<i32>() };
        for (i, row) in batch.iter().enumerate() {
            if let Some(code) = row.error_code {
                error_code_values[i] = code;
            }
        }

        output.set_len(batch.len());
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![]) // No positional parameters
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let mut params = database_named_parameters();
        params.push((
            "filter".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ));
        Some(params)
    }
}

fn operation_to_row(op: InternalOperation) -> OperationRow {
    let metadata_type = op
        .metadata
        .as_ref()
        .and_then(|m| m.type_url().map(str::to_string))
        .unwrap_or_default();

    let (error_code, error_message) = match &op.result {
        Some(operation::Result::Error(status)) => (Some(status.code), Some(status.message.clone())),
        _ => (None, None),
    };

    OperationRow {
        name: op.name,
        done: op.done,
        metadata_type,
        error_code,
        error_message,
    }
}

async fn wait_operation(
    admin: &DatabaseAdmin,
    operation_name: &str,
) -> Result<InternalOperation, SpannerError> {
    let mut poll_backoff =
        JitteredBackoff::new(DEFAULT_TIMEOUTS.poll_initial, DEFAULT_TIMEOUTS.poll_max);
    loop {
        let operation = get_real_operation(admin, operation_name).await?;
        if operation.done {
            return Ok(operation);
        }
        tokio::time::sleep(poll_backoff.next_delay()).await;
    }
}

async fn get_real_operation(
    admin: &DatabaseAdmin,
    operation_name: &str,
) -> Result<InternalOperation, SpannerError> {
    let request = bounded_admin_request(admin.get_operation().set_name(operation_name));
    let operation = send_admin_request(request.send())
        .await
        .map_err(|error| admin_request_error("Spanner GetOperation", error))?;
    validate_operation(&operation, Some(operation_name), "Spanner GetOperation")?;
    Ok(operation)
}

async fn list_database_operations(
    database_path: &str,
    endpoint: Option<&str>,
    admin_endpoint: Option<&str>,
    filter: Option<&str>,
) -> Result<Vec<OperationRow>, SpannerError> {
    let listing = async {
        // The emulator only implements google.longrunning.Operations/ListOperations,
        // while real Spanner exposes DatabaseAdmin/ListDatabaseOperations with auth.
        match emulator_admin_endpoint(endpoint, admin_endpoint) {
            Some(admin_endpoint) => {
                list_emulator_database_operations(database_path, &admin_endpoint, filter).await
            }
            None => list_real_database_operations(database_path, filter).await,
        }
    };
    let mut operations = match tokio::time::timeout(DEFAULT_TIMEOUTS.list, listing).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(SpannerError::Other(format!(
                "Listing Spanner database operations timed out after {}",
                duration_label(DEFAULT_TIMEOUTS.list)
            )));
        }
    };

    // ListDatabaseOperations is instance-scoped, so keep the table function's
    // per-database contract by trimming unrelated operations client-side.
    let database_operation_prefix = database_operation_prefix(database_path);
    operations.retain(|op| op.name.starts_with(&database_operation_prefix));

    Ok(operations.into_iter().map(operation_to_row).collect())
}

async fn list_real_database_operations(
    database_path: &str,
    filter: Option<&str>,
) -> Result<Vec<InternalOperation>, SpannerError> {
    let admin = get_or_create_admin_client(None).await?;
    let request = bounded_admin_request(
        admin
            .list_database_operations()
            .set_parent(instance_path_from_database_path(database_path)?.to_string())
            .set_filter(filter.unwrap_or_default()),
    );
    let mut pages = request.by_page();

    let mut operations = Vec::new();
    let mut seen_tokens = HashSet::new();
    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        page_count += 1;
        let page = page.map_err(|error| {
            SpannerError::Other(format!(
                "Spanner ListDatabaseOperations failed on page {page_count} within the {} per-request bound: {error}",
                duration_label(DEFAULT_TIMEOUTS.request)
            ))
        })?;
        for operation in &page.operations {
            validate_operation(operation, None, "Spanner ListDatabaseOperations")?;
        }
        operations.extend(page.operations);
        record_next_page_token(
            &mut seen_tokens,
            &page.next_page_token,
            page_count,
            DEFAULT_TIMEOUTS.max_pages,
            "Spanner ListDatabaseOperations",
        )?;
    }
    Ok(operations)
}

async fn list_emulator_database_operations(
    database_path: &str,
    endpoint: &str,
    filter: Option<&str>,
) -> Result<Vec<InternalOperation>, SpannerError> {
    let transport = ReqwestEmulatorTransport::new(DEFAULT_TIMEOUTS.connect)?;
    list_emulator_database_operations_with(
        &transport,
        database_path,
        endpoint,
        filter,
        DEFAULT_TIMEOUTS,
    )
    .await
}

async fn list_emulator_database_operations_with(
    transport: &dyn EmulatorTransport,
    database_path: &str,
    endpoint: &str,
    filter: Option<&str>,
    timeouts: DdlTimeouts,
) -> Result<Vec<InternalOperation>, SpannerError> {
    let mut operations = Vec::new();
    let mut page_token = String::new();
    let mut seen_tokens = HashSet::new();

    for page_count in 1..=timeouts.max_pages {
        let mut query = Vec::new();
        if let Some(filter) = filter {
            query.push(("filter".to_string(), filter.to_string()));
        }
        if !page_token.is_empty() {
            query.push(("pageToken".to_string(), page_token));
        }
        let request = EmulatorRequest {
            method: Method::GET,
            url: format!(
                "{}/v1/{database_path}/operations",
                endpoint.trim_end_matches('/')
            ),
            query,
            json: None,
        };
        let response =
            send_emulator_request(transport, request, "Emulator ListOperations", timeouts).await?;
        let page = operations_page_from_json(&response.body)?;
        operations.extend(page.operations);
        page_token = page.next_page_token;
        if page_token.is_empty() {
            return Ok(operations);
        }
        record_next_page_token(
            &mut seen_tokens,
            &page_token,
            page_count,
            timeouts.max_pages,
            "emulator ListOperations",
        )?;
    }

    Err(SpannerError::Other(format!(
        "Emulator ListOperations exceeded the {} page limit",
        timeouts.max_pages
    )))
}

struct OperationsPage {
    operations: Vec<InternalOperation>,
    next_page_token: String,
}

fn operations_page_from_json(body: &str) -> Result<OperationsPage, SpannerError> {
    let json: serde_json::Value = serde_json::from_str(body).map_err(|error| {
        SpannerError::Other(format!(
            "Emulator ListOperations protocol error: malformed JSON: {error}; body={}",
            body_excerpt(body)
        ))
    })?;
    let object = json.as_object().ok_or_else(|| {
        SpannerError::Other(format!(
            "Emulator ListOperations protocol error: response must be a JSON object; body={}",
            body_excerpt(body)
        ))
    })?;

    let mut operations = Vec::new();
    if let Some(value) = object.get("operations") {
        let values = value.as_array().ok_or_else(|| {
            SpannerError::Other(format!(
                "Emulator ListOperations protocol error: operations must be an array; body={}",
                body_excerpt(body)
            ))
        })?;
        for (index, value) in values.iter().enumerate() {
            operations.push(operation_from_value(
                value.clone(),
                &format!("ListOperations operations[{index}]={value}"),
            )?);
        }
    }

    let next_page_token = match object.get("nextPageToken") {
        Some(value) => value.as_str().map(str::to_string).ok_or_else(|| {
            SpannerError::Other(format!(
                "Emulator ListOperations protocol error: nextPageToken must be a string; body={}",
                body_excerpt(body)
            ))
        })?,
        None => String::new(),
    };
    Ok(OperationsPage {
        operations,
        next_page_token,
    })
}

fn record_next_page_token(
    seen_tokens: &mut HashSet<String>,
    token: &str,
    page_count: usize,
    max_pages: usize,
    context: &str,
) -> Result<(), SpannerError> {
    if token.is_empty() {
        return Ok(());
    }
    if !seen_tokens.insert(token.to_string()) {
        return Err(SpannerError::Other(format!(
            "{context} protocol error: repeated next page token after page {page_count}: {token}"
        )));
    }
    if page_count >= max_pages {
        return Err(SpannerError::Other(format!(
            "{context} exceeded the {max_pages} page limit; last next page token: {token}"
        )));
    }
    Ok(())
}

fn admin_endpoint_url(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    let endpoint = if endpoint.ends_with(":9010") {
        format!("{}:9020", endpoint.trim_end_matches(":9010"))
    } else {
        endpoint.to_string()
    };

    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    }
}

fn database_operation_prefix(database_path: &str) -> String {
    format!("{database_path}/operations/")
}

fn instance_path_from_database_path(database_path: &str) -> Result<&str, SpannerError> {
    database_path
        .rsplit_once("/databases/")
        .map(|(prefix, _)| prefix)
        .ok_or_else(|| {
            SpannerError::Other(format!(
                "Invalid database path for operations listing: {database_path}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ffi::CString;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::{sync_channel, SyncSender};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use duckdb::ffi::{
        duckdb_create_int64, duckdb_create_list_value, duckdb_create_logical_type,
        duckdb_create_null_value, duckdb_create_varchar, duckdb_destroy_logical_type,
        duckdb_destroy_value, duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    };
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_longrunning::model::Operation as InternalOperation;
    use reqwest::{Method, StatusCode};

    use crate::error::SpannerError;
    use crate::runtime::test_support::TestRuntimeOwner;

    use super::{
        admin_endpoint_url, admin_request_is_ambiguous, cached_init_result,
        database_operation_prefix, ddl_operation_error, ddl_statements_from_values,
        fetch_emulator_operation, gax_submission_signals_are_ambiguous,
        get_or_create_cached_client, google_error_code_and_message,
        instance_path_from_database_path, is_emulator_replay_conflict,
        is_emulator_schema_change_rejection, list_emulator_database_operations_with,
        lookup_cached_client, operation_from_json, recover_ambiguous_submission,
        spawn_client_initialization, spawn_client_initialization_with,
        update_emulator_database_ddl_with, wait_emulator_operation_with,
        wait_with_operation_deadline, AdminRequestError, ClientCache, ClientCacheLookup,
        ClientFlight, DdlTimeouts, EmulatorResponse, EmulatorTransport, FlightCleanupGuard,
        HttpFuture,
    };

    struct DropSignal(Option<SyncSender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn test_ddl_statements_single_text_preserves_semicolons() {
        let sql = varchar_value("CREATE TABLE T (S STRING(MAX) DEFAULT ('a;b')) PRIMARY KEY (S)");
        assert_eq!(
            ddl_statements_from_values(&sql, None).unwrap(),
            vec!["CREATE TABLE T (S STRING(MAX) DEFAULT ('a;b')) PRIMARY KEY (S)".to_string()]
        );
    }

    #[test]
    fn test_ddl_statements_list() {
        let sql = null_value();
        let statements = varchar_list_value(&[
            "ALTER TABLE T ADD COLUMN A INT64",
            "ALTER TABLE T ADD COLUMN B STRING(MAX) DEFAULT ('a;b')",
        ]);
        assert_eq!(
            ddl_statements_from_values(&sql, Some(&statements)).unwrap(),
            vec![
                "ALTER TABLE T ADD COLUMN A INT64".to_string(),
                "ALTER TABLE T ADD COLUMN B STRING(MAX) DEFAULT ('a;b')".to_string(),
            ]
        );
    }

    #[test]
    fn test_ddl_statements_rejects_missing_or_ambiguous_inputs() {
        let sql = null_value();
        assert!(ddl_statements_from_values(&sql, None).is_err());

        let sql = varchar_value("CREATE TABLE T (Id INT64) PRIMARY KEY (Id)");
        let statements = varchar_list_value(&["ALTER TABLE T ADD COLUMN A INT64"]);
        assert!(ddl_statements_from_values(&sql, Some(&statements)).is_err());
    }

    #[test]
    fn test_ddl_statements_rejects_empty_or_non_varchar_list_entries() {
        let sql = null_value();
        let statements = varchar_list_value(&["ALTER TABLE T ADD COLUMN A INT64", "  "]);
        assert!(ddl_statements_from_values(&sql, Some(&statements)).is_err());

        let statements = bigint_list_value(&[1]);
        assert!(ddl_statements_from_values(&sql, Some(&statements)).is_err());

        let sql = bigint_value(1);
        assert!(ddl_statements_from_values(&sql, None).is_err());
    }

    #[test]
    fn test_cached_init_result_replays_success_without_rerun() {
        let cache = Mutex::new(None);
        let calls = AtomicUsize::new(0);

        let first = cached_init_result(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("operation-1".to_string())
        })
        .unwrap();
        let second = cached_init_result(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("operation-2".to_string())
        })
        .unwrap();

        assert_eq!(first, "operation-1");
        assert_eq!(second, "operation-1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cached_init_result_replays_error_without_rerun() {
        let cache = Mutex::new(None);
        let calls = AtomicUsize::new(0);

        let first = cached_init_result::<String>(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err("first init failed".into())
        })
        .unwrap_err()
        .to_string();
        let second = cached_init_result::<String>(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("operation-2".to_string())
        })
        .unwrap_err()
        .to_string();

        assert_eq!(first, "first init failed");
        assert_eq!(second, "first init failed");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_admin_cache_is_single_flight_during_completed_lru_churn() {
        let cache = Arc::new(Mutex::new(ClientCache::new(1, 2)));
        let constructions = Arc::new(AtomicUsize::new(0));

        let first = initialize_test_cached_client(
            Arc::clone(&cache),
            "shared-admin",
            Arc::clone(&constructions),
            Duration::from_millis(30),
            Duration::from_millis(200),
            42,
        );
        let churn = initialize_test_cached_client(
            Arc::clone(&cache),
            "other-admin",
            Arc::new(AtomicUsize::new(0)),
            Duration::ZERO,
            Duration::from_millis(200),
            7,
        );
        let second = initialize_test_cached_client(
            Arc::clone(&cache),
            "shared-admin",
            Arc::clone(&constructions),
            Duration::from_millis(30),
            Duration::from_millis(200),
            99,
        );
        let (first, _, second) = tokio::join!(first, churn, second);
        let first = first.unwrap();
        let second = second.unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(constructions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_admin_cache_wait_timeout_does_not_restart_initialization() {
        let cache = Arc::new(Mutex::new(ClientCache::new(1, 1)));
        let constructions = Arc::new(AtomicUsize::new(0));
        let first = initialize_test_cached_client(
            Arc::clone(&cache),
            "shared-admin",
            Arc::clone(&constructions),
            Duration::from_millis(30),
            Duration::from_millis(5),
            42,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(first.contains("Timed out waiting"), "{first}");

        let second = initialize_test_cached_client(
            Arc::clone(&cache),
            "shared-admin",
            Arc::clone(&constructions),
            Duration::from_millis(30),
            Duration::from_millis(200),
            99,
        )
        .await
        .unwrap();

        assert_eq!(*second, 42);
        assert_eq!(constructions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_admin_cache_completed_entries_remain_lru_bounded() {
        let cache = Arc::new(Mutex::new(ClientCache::new(1, 1)));
        let constructions = Arc::new(AtomicUsize::new(0));

        for (key, value) in [("admin-a", 1), ("admin-b", 2), ("admin-a", 3)] {
            let client = initialize_test_cached_client(
                Arc::clone(&cache),
                key,
                Arc::clone(&constructions),
                Duration::ZERO,
                Duration::from_millis(200),
                value,
            )
            .await
            .unwrap();
            assert_eq!(*client, value);
        }

        assert_eq!(constructions.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_stale_cleanup_guard_cannot_remove_newer_flight_generation() {
        let cache = Arc::new(Mutex::new(ClientCache::<usize>::new(1, 1)));
        let cache_key = "shared-admin".to_string();
        let (sender, generation, permit) = match lookup_cached_client(&cache, &cache_key).unwrap() {
            ClientCacheLookup::Start {
                sender,
                generation,
                permit,
                ..
            } => (sender, generation, permit),
            _ => panic!("first lookup must start a flight"),
        };
        let stale_guard = FlightCleanupGuard::new(
            Arc::clone(&cache),
            cache_key.clone(),
            generation,
            sender,
            permit,
        );

        let (_replacement_sender, replacement_receiver) = tokio::sync::watch::channel(None);
        let replacement_generation = {
            let mut cache = cache.lock().unwrap_or_else(|error| error.into_inner());
            let replacement_generation = cache.allocate_generation();
            cache.in_flight.insert(
                cache_key.clone(),
                ClientFlight {
                    generation: replacement_generation,
                    receiver: replacement_receiver,
                },
            );
            replacement_generation
        };

        drop(stale_guard);
        assert_eq!(
            cache
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .in_flight
                .get(&cache_key)
                .map(|flight| flight.generation),
            Some(replacement_generation)
        );
    }

    #[tokio::test]
    async fn test_admin_cache_panic_cleans_flight_and_allows_retry() {
        let cache = Arc::new(Mutex::new(ClientCache::new(1, 1)));
        let attempts = Arc::new(AtomicUsize::new(0));
        let panic_attempts = Arc::clone(&attempts);
        let error = get_or_create_cached_client(
            Arc::clone(&cache),
            "panic-admin".to_string(),
            Duration::from_millis(200),
            move || async move {
                if panic_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("simulated admin initialization panic");
                }
                Ok(Arc::new(1_usize))
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("ended without a result"), "{error}");

        let client = get_or_create_cached_client(
            Arc::clone(&cache),
            "panic-admin".to_string(),
            Duration::from_millis(200),
            move || async move { Ok(Arc::new(42_usize)) },
        )
        .await
        .unwrap();

        assert_eq!(*client, 42);
        assert!(cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .in_flight
            .is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_admin_cache_abort_before_first_poll_cleans_flight_and_allows_retry() {
        let cache = Arc::new(Mutex::new(ClientCache::new(1, 1)));
        let starts = Arc::new(AtomicUsize::new(0));
        let (sender, generation, permit) =
            match lookup_cached_client(&cache, &"aborted-admin".to_string()).unwrap() {
                ClientCacheLookup::Start {
                    sender,
                    generation,
                    permit,
                    ..
                } => (sender, generation, permit),
                _ => panic!("first lookup must start a flight"),
            };
        let task_starts = Arc::clone(&starts);
        let abort_handle = spawn_client_initialization_with(
            Arc::clone(&cache),
            "aborted-admin".to_string(),
            generation,
            permit,
            sender,
            move || async move {
                task_starts.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<Arc<usize>, SpannerError>>().await
            },
            |future| {
                let task = tokio::spawn(future);
                let abort_handle = task.abort_handle();
                drop(task);
                Ok(abort_handle)
            },
        )
        .unwrap();
        // The current-thread runtime cannot poll the spawned task before this
        // synchronous abort, exercising the pre-first-poll cancellation path.
        abort_handle.abort();
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                let flight_exists = cache
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .in_flight
                    .contains_key("aborted-admin");
                if !flight_exists {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 0);

        let client = get_or_create_cached_client(
            Arc::clone(&cache),
            "aborted-admin".to_string(),
            Duration::from_millis(200),
            move || async move { Ok(Arc::new(43_usize)) },
        )
        .await
        .unwrap();

        assert_eq!(*client, 43);
        assert!(cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .in_flight
            .is_empty());
    }

    #[test]
    fn test_admin_initialization_is_tracked_by_runtime_owner_shutdown() {
        let cache = Arc::new(Mutex::new(ClientCache::new(1, 1)));
        let cache_key = "tracked-admin".to_string();
        let (receiver, sender, generation, permit) =
            match lookup_cached_client(&cache, &cache_key).unwrap() {
                ClientCacheLookup::Start {
                    receiver,
                    sender,
                    generation,
                    permit,
                } => (receiver, sender, generation, permit),
                _ => panic!("first lookup must start a flight"),
            };
        let owner = Arc::new(TestRuntimeOwner::start(1).unwrap());
        let spawn_owner = Arc::clone(&owner);
        let (started_tx, started_rx) = sync_channel(1);
        let (dropped_tx, dropped_rx) = sync_channel(1);
        let _abort_handle = spawn_client_initialization_with(
            Arc::clone(&cache),
            cache_key.clone(),
            generation,
            permit,
            sender,
            move || async move {
                let _drop_signal = DropSignal(Some(dropped_tx));
                started_tx.send(()).unwrap();
                std::future::pending::<Result<Arc<usize>, SpannerError>>().await
            },
            move |future| spawn_owner.spawn_detached(future),
        )
        .unwrap();
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("admin initialization did not start");

        owner.shutdown(Duration::from_millis(500)).unwrap();

        dropped_rx
            .try_recv()
            .expect("owner shutdown returned before admin initialization was dropped");
        assert!(receiver.has_changed().is_err());
        let cache = cache.lock().unwrap_or_else(|error| error.into_inner());
        assert!(!cache.in_flight.contains_key(&cache_key));
        assert_eq!(cache.admission.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_admin_cache_rejects_fifth_unique_flight_before_spawning() {
        let cache = Arc::new(Mutex::new(ClientCache::new(8, 4)));
        let mut abort_handles = Vec::new();
        for index in 0..4 {
            let cache_key = format!("admin-{index}");
            let (sender, generation, permit) =
                match lookup_cached_client(&cache, &cache_key).unwrap() {
                    ClientCacheLookup::Start {
                        sender,
                        generation,
                        permit,
                        ..
                    } => (sender, generation, permit),
                    _ => panic!("unique lookup must start a flight"),
                };
            abort_handles.push(
                spawn_client_initialization(
                    Arc::clone(&cache),
                    cache_key,
                    generation,
                    permit,
                    sender,
                    std::future::pending::<Result<Arc<usize>, SpannerError>>,
                )
                .unwrap(),
            );
        }

        let fifth_starts = Arc::new(AtomicUsize::new(0));
        let task_fifth_starts = Arc::clone(&fifth_starts);
        let error = get_or_create_cached_client(
            Arc::clone(&cache),
            "admin-4".to_string(),
            Duration::from_millis(200),
            move || async move {
                task_fifth_starts.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(4_usize))
            },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("capacity is full"), "{error}");
        assert!(error.contains("maximum 4"), "{error}");
        assert_eq!(fifth_starts.load(Ordering::SeqCst), 0);
        assert_eq!(
            cache
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .in_flight
                .len(),
            4
        );

        for abort_handle in abort_handles {
            abort_handle.abort();
        }
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                let in_flight = cache
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .in_flight
                    .len();
                if in_flight == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn test_instance_path_from_database_path() {
        assert_eq!(
            instance_path_from_database_path("projects/p/instances/i/databases/d").unwrap(),
            "projects/p/instances/i"
        );
        assert!(instance_path_from_database_path("projects/p/instances/i").is_err());
    }

    #[test]
    fn test_database_operation_prefix() {
        assert_eq!(
            database_operation_prefix("projects/p/instances/i/databases/d"),
            "projects/p/instances/i/databases/d/operations/"
        );
    }

    #[test]
    fn test_admin_endpoint_url_maps_default_emulator_port_and_trims_slash() {
        assert_eq!(
            admin_endpoint_url("localhost:9010/"),
            "http://localhost:9020"
        );
        assert_eq!(
            admin_endpoint_url("http://127.0.0.1:9010/"),
            "http://127.0.0.1:9020"
        );
        assert_eq!(
            admin_endpoint_url("localhost:19020"),
            "http://localhost:19020"
        );
    }

    #[test]
    fn test_operation_from_json_preserves_error() {
        let op = operation_from_json(
            r#"{"name":"projects/p/instances/i/databases/d/operations/op1","done":true,"error":{"code":3,"message":"bad ddl"}}"#,
        )
        .unwrap();

        let err = ddl_operation_error(&op).unwrap();
        assert!(err.contains("code=3"));
        assert!(err.contains("bad ddl"));
    }

    #[test]
    fn test_operation_from_json_defaults_missing_done_and_rejects_non_boolean_done() {
        let pending =
            operation_from_json(r#"{"name":"projects/p/instances/i/databases/d/operations/op1"}"#)
                .unwrap();
        assert!(!pending.done);

        let error = operation_from_json(
            r#"{"name":"projects/p/instances/i/databases/d/operations/op1","done":"false"}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("non-boolean done"), "{error}");
    }

    #[test]
    fn test_operation_from_json_rejects_malformed_json_and_operation() {
        let error = operation_from_json("{not-json").unwrap_err().to_string();
        assert!(error.contains("protocol error: malformed JSON"), "{error}");

        let error = operation_from_json(
            r#"{"name":"projects/p/instances/i/databases/d/operations/op1","done":false,"error":{"code":3,"message":"premature"}}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unfinished operation"), "{error}");
    }

    #[test]
    fn test_google_status_parser_accepts_wrapped_and_direct_status_json() {
        assert_eq!(
            google_error_code_and_message(r#"{"error":{"code":9,"message":"wrapped rejection"}}"#),
            Some((9, "wrapped rejection".to_string()))
        );
        assert_eq!(
            google_error_code_and_message(r#"{"code":9,"message":"direct rejection"}"#),
            Some((9, "direct rejection".to_string()))
        );
        assert!(is_emulator_schema_change_rejection(
            r#"{"code":9,"message":"Schema change operation rejected because another update is running"}"#
        ));
        assert!(is_emulator_replay_conflict(
            StatusCode::CONFLICT,
            r#"{"code":6,"message":"operation already exists"}"#
        ));
    }

    #[test]
    fn test_real_submission_ambiguity_classification() {
        assert!(admin_request_is_ambiguous(&AdminRequestError::Timeout));
        assert!(admin_request_is_ambiguous(&AdminRequestError::GoogleCloud(
            google_cloud_gax::error::Error::timeout(std::io::Error::other("client timeout"))
        )));
        assert!(admin_request_is_ambiguous(&AdminRequestError::GoogleCloud(
            google_cloud_gax::error::Error::io(std::io::Error::other("response body reset"))
        )));
        assert!(admin_request_is_ambiguous(&AdminRequestError::GoogleCloud(
            google_cloud_gax::error::Error::exhausted(google_cloud_gax::error::Error::service(
                Status::default().set_code(Code::Unavailable),
            ))
        )));
        assert!(!admin_request_is_ambiguous(
            &AdminRequestError::GoogleCloud(google_cloud_gax::error::Error::exhausted(
                "definitive pre-RPC failure"
            ))
        ));

        for code in [
            Code::DeadlineExceeded,
            Code::Unknown,
            Code::Internal,
            Code::Unavailable,
        ] {
            let error = AdminRequestError::GoogleCloud(google_cloud_gax::error::Error::service(
                Status::default().set_code(code),
            ));
            assert!(admin_request_is_ambiguous(&error), "code={code:?}");
        }

        let permission_denied =
            AdminRequestError::GoogleCloud(google_cloud_gax::error::Error::service(
                Status::default().set_code(Code::PermissionDenied),
            ));
        assert!(!admin_request_is_ambiguous(&permission_denied));
    }

    #[test]
    fn test_gax_http_status_precedes_generic_transport_classification() {
        for status in [408, 429, 500, 501, 505, 599] {
            assert!(
                gax_submission_signals_are_ambiguous(
                    Some(status),
                    false,
                    Some(Code::PermissionDenied),
                ),
                "status={status}"
            );
        }
        for status in [400, 401, 403, 404, 409, 422] {
            assert!(
                !gax_submission_signals_are_ambiguous(Some(status), true, Some(Code::Unavailable),),
                "terminal status={status} must override generic transport signals"
            );
        }
        assert!(gax_submission_signals_are_ambiguous(None, true, None));
        assert!(gax_submission_signals_are_ambiguous(
            None,
            false,
            Some(Code::DeadlineExceeded)
        ));
        assert!(!gax_submission_signals_are_ambiguous(
            None,
            false,
            Some(Code::PermissionDenied)
        ));
    }

    #[tokio::test]
    async fn test_ambiguous_submission_recovery_preserves_operation_and_errors() {
        let operation_name = "projects/p/instances/i/databases/d/operations/recover";
        let recovered = recover_ambiguous_submission(
            "Spanner UpdateDatabaseDdl",
            operation_name,
            "deadline exceeded",
            async { Ok::<_, SpannerError>(42) },
        )
        .await
        .unwrap();
        assert_eq!(recovered, 42);

        let error = recover_ambiguous_submission::<usize>(
            "Spanner UpdateDatabaseDdl",
            operation_name,
            "deadline exceeded",
            async { Err(SpannerError::Other("lookup unavailable".to_string())) },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("outcome is ambiguous"), "{error}");
        assert!(error.contains(operation_name), "{error}");
        assert!(error.contains("deadline exceeded"), "{error}");
        assert!(error.contains("lookup unavailable"), "{error}");
    }

    #[tokio::test]
    async fn test_emulator_update_recovers_ambiguous_status_without_second_patch() {
        let database = "projects/p/instances/i/databases/d";
        let operation_id = "test_ambiguous_recovery";
        let operation_name = format!("{database}/operations/{operation_id}");
        let transport = FakeTransport::sequence([
            FakeReply::text(
                StatusCode::SERVICE_UNAVAILABLE,
                r#"{"code":14,"message":"response lost after acceptance"}"#,
            ),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);

        let operation = update_emulator_database_ddl_with(
            &transport,
            "http://emulator",
            database,
            vec!["CREATE TABLE T (Id INT64) PRIMARY KEY (Id)".to_string()],
            operation_id,
            test_timeouts(),
        )
        .await
        .unwrap();

        assert_eq!(operation.name, operation_name);
        assert!(!operation.done);
        assert_eq!(transport.methods(), vec![Method::PATCH, Method::GET]);
    }

    #[tokio::test]
    async fn test_emulator_update_recovers_request_timeout_without_second_patch() {
        let database = "projects/p/instances/i/databases/d";
        let operation_id = "test_request_timeout_recovery";
        let operation_name = format!("{database}/operations/{operation_id}");
        let transport = FakeTransport::sequence([
            FakeReply::text(
                StatusCode::REQUEST_TIMEOUT,
                r#"{"code":4,"message":"request timed out after dispatch"}"#,
            ),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);

        let operation = update_emulator_database_ddl_with(
            &transport,
            "http://emulator",
            database,
            vec!["CREATE TABLE T (Id INT64) PRIMARY KEY (Id)".to_string()],
            operation_id,
            test_timeouts(),
        )
        .await
        .unwrap();

        assert_eq!(operation.name, operation_name);
        assert_eq!(transport.methods(), vec![Method::PATCH, Method::GET]);
    }

    #[tokio::test]
    async fn test_emulator_get_retries_request_timeout_within_attempt_bound() {
        let operation_name = "projects/p/instances/i/databases/d/operations/get-408";
        let transport = FakeTransport::sequence([
            FakeReply::text(
                StatusCode::REQUEST_TIMEOUT,
                r#"{"code":4,"message":"request timed out"}"#,
            ),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);
        let mut timeouts = test_timeouts();
        timeouts.max_attempts = 2;

        let operation =
            fetch_emulator_operation(&transport, "http://emulator", operation_name, timeouts)
                .await
                .unwrap();

        assert_eq!(operation.name, operation_name);
        assert_eq!(transport.methods(), vec![Method::GET, Method::GET]);
    }

    #[tokio::test]
    async fn test_emulator_update_recovers_all_server_error_classes_without_second_patch() {
        for status in [
            StatusCode::NOT_IMPLEMENTED,
            StatusCode::HTTP_VERSION_NOT_SUPPORTED,
        ] {
            let database = "projects/p/instances/i/databases/d";
            let operation_id = format!("test_server_error_{}", status.as_u16());
            let operation_name = format!("{database}/operations/{operation_id}");
            let transport = FakeTransport::sequence([
                FakeReply::text(
                    status,
                    format!(
                        r#"{{"code":{},"message":"server rejected response path"}}"#,
                        status.as_u16()
                    ),
                ),
                FakeReply::text(
                    StatusCode::OK,
                    format!(r#"{{"name":"{operation_name}","done":false}}"#),
                ),
                FakeReply::text(
                    StatusCode::OK,
                    format!(r#"{{"name":"{operation_name}","done":false}}"#),
                ),
            ]);

            let operation = update_emulator_database_ddl_with(
                &transport,
                "http://emulator",
                database,
                vec!["CREATE TABLE T (Id INT64) PRIMARY KEY (Id)".to_string()],
                &operation_id,
                test_timeouts(),
            )
            .await
            .unwrap();

            assert_eq!(operation.name, operation_name);
            assert_eq!(
                transport.methods(),
                vec![Method::PATCH, Method::GET],
                "status={status}"
            );
        }
    }

    #[tokio::test]
    async fn test_emulator_update_reports_ambiguous_status_when_lookup_fails() {
        let database = "projects/p/instances/i/databases/d";
        let operation_id = "test_ambiguous_missing";
        let operation_name = format!("{database}/operations/{operation_id}");
        let transport = FakeTransport::sequence([
            FakeReply::text(
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"code":8,"message":"rate limited after dispatch"}"#,
            ),
            FakeReply::text(
                StatusCode::NOT_FOUND,
                r#"{"code":5,"message":"operation not found"}"#,
            ),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);

        let error = update_emulator_database_ddl_with(
            &transport,
            "http://emulator",
            database,
            vec!["CREATE TABLE T (Id INT64) PRIMARY KEY (Id)".to_string()],
            operation_id,
            test_timeouts(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("outcome is ambiguous"), "{error}");
        assert!(error.contains(&operation_name), "{error}");
        assert!(error.contains("rate limited after dispatch"), "{error}");
        assert!(error.contains("operation not found"), "{error}");
        assert_eq!(transport.methods(), vec![Method::PATCH, Method::GET]);
    }

    #[tokio::test]
    async fn test_emulator_update_recovers_attempt_timeout_without_second_patch() {
        let database = "projects/p/instances/i/databases/d";
        let operation_id = "test_timeout_recovery";
        let operation_name = format!("{database}/operations/{operation_id}");
        let transport = FakeTransport::sequence([
            FakeReply::stalled(StatusCode::OK),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);
        let mut timeouts = test_timeouts();
        timeouts.attempt = Duration::from_millis(2);

        let operation = update_emulator_database_ddl_with(
            &transport,
            "http://emulator",
            database,
            vec!["CREATE TABLE T (Id INT64) PRIMARY KEY (Id)".to_string()],
            operation_id,
            timeouts,
        )
        .await
        .unwrap();

        assert_eq!(operation.name, operation_name);
        assert_eq!(transport.methods(), vec![Method::PATCH, Method::GET]);
    }

    #[tokio::test]
    async fn test_emulator_update_recovers_transport_error_without_second_patch() {
        let database = "projects/p/instances/i/databases/d";
        let operation_id = "test_transport_recovery";
        let operation_name = format!("{database}/operations/{operation_id}");
        let transport = FakeTransport::sequence([
            FakeReply::body_error(StatusCode::OK, "connection closed while reading response"),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);

        let operation = update_emulator_database_ddl_with(
            &transport,
            "http://emulator",
            database,
            vec!["CREATE TABLE T (Id INT64) PRIMARY KEY (Id)".to_string()],
            operation_id,
            test_timeouts(),
        )
        .await
        .unwrap();

        assert_eq!(operation.name, operation_name);
        assert_eq!(transport.methods(), vec![Method::PATCH, Method::GET]);
    }

    #[tokio::test]
    async fn test_emulator_update_preserves_schema_rejection_retry() {
        let database = "projects/p/instances/i/databases/d";
        let operation_id = "test_schema_retry";
        let operation_name = format!("{database}/operations/{operation_id}");
        let transport = FakeTransport::sequence([
            FakeReply::text(
                StatusCode::BAD_REQUEST,
                r#"{"code":9,"message":"Schema change operation rejected because another update is running"}"#,
            ),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);

        update_emulator_database_ddl_with(
            &transport,
            "http://emulator",
            database,
            vec!["ALTER TABLE T ADD COLUMN C INT64".to_string()],
            operation_id,
            test_timeouts(),
        )
        .await
        .unwrap();

        assert_eq!(transport.methods(), vec![Method::PATCH, Method::PATCH]);
    }

    #[tokio::test]
    async fn test_emulator_update_does_not_retry_terminal_error() {
        let database = "projects/p/instances/i/databases/d";
        let operation_id = "test_terminal";
        let operation_name = format!("{database}/operations/{operation_id}");
        let transport = FakeTransport::sequence([
            FakeReply::text(
                StatusCode::FORBIDDEN,
                r#"{"code":7,"message":"permission denied"}"#,
            ),
            FakeReply::text(
                StatusCode::OK,
                format!(r#"{{"name":"{operation_name}","done":false}}"#),
            ),
        ]);

        let error = update_emulator_database_ddl_with(
            &transport,
            "http://emulator",
            database,
            vec!["ALTER TABLE T ADD COLUMN C INT64".to_string()],
            operation_id,
            test_timeouts(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("403 Forbidden"), "{error}");
        assert!(error.contains("permission denied"), "{error}");
        assert!(error.contains(&operation_name), "{error}");
        assert_eq!(transport.methods(), vec![Method::PATCH]);
    }

    #[tokio::test]
    async fn test_emulator_operation_perpetual_not_done_hits_deadline() {
        let operation_name = "projects/p/instances/i/databases/d/operations/never";
        let transport = FakeTransport::repeating(FakeReply::text(
            StatusCode::OK,
            format!(r#"{{"name":"{operation_name}","done":false}}"#),
        ));
        let timeouts = test_timeouts();

        let error = wait_with_operation_deadline(
            operation_name,
            timeouts.operation,
            timeouts.operation,
            wait_emulator_operation_with(&transport, "http://emulator", operation_name, timeouts),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains(operation_name), "{error}");
        assert!(error.contains("20ms overall deadline"), "{error}");
        assert!(transport.calls() > 0);
    }

    #[tokio::test]
    async fn test_emulator_operation_body_stall_hits_attempt_timeout() {
        let operation_name = "projects/p/instances/i/databases/d/operations/stalled";
        let transport = FakeTransport::sequence([FakeReply::stalled(StatusCode::OK)]);
        let mut timeouts = test_timeouts();
        timeouts.max_attempts = 1;
        timeouts.attempt = Duration::from_millis(5);

        let error =
            fetch_emulator_operation(&transport, "http://emulator", operation_name, timeouts)
                .await
                .unwrap_err()
                .to_string();

        assert!(
            error.contains("attempt timed out while sending or reading body"),
            "{error}"
        );
        assert!(error.contains("maximum 1 attempts"), "{error}");
        assert_eq!(transport.calls(), 1);
    }

    #[tokio::test]
    async fn test_emulator_operation_listing_rejects_repeated_page_token() {
        let operation_name = "projects/p/instances/i/databases/d/operations/op1";
        let transport = FakeTransport::sequence([
            FakeReply::text(
                StatusCode::OK,
                format!(
                    r#"{{"operations":[{{"name":"{operation_name}","done":false}}],"nextPageToken":"repeat"}}"#
                ),
            ),
            FakeReply::text(
                StatusCode::OK,
                r#"{"operations":[],"nextPageToken":"repeat"}"#,
            ),
        ]);

        let error = list_emulator_database_operations_with(
            &transport,
            "projects/p/instances/i/databases/d",
            "http://emulator",
            None,
            test_timeouts(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("repeated next page token"), "{error}");
        assert!(error.contains("repeat"), "{error}");
        assert_eq!(transport.calls(), 2);
    }

    #[test]
    fn test_database_operation_prefix_filters_other_databases() {
        let database_path = "projects/p/instances/i/databases/d";
        let prefix = database_operation_prefix(database_path);
        let operations = vec![
            InternalOperation::new().set_name(format!("{database_path}/operations/op-1")),
            InternalOperation::new()
                .set_name("projects/p/instances/i/databases/other/operations/op-2"),
        ];

        let matching_names: Vec<_> = operations
            .into_iter()
            .filter(|op| op.name.starts_with(&prefix))
            .map(|op| op.name)
            .collect();

        assert_eq!(
            matching_names,
            vec![format!("{database_path}/operations/op-1")]
        );
    }

    #[derive(Clone)]
    enum FakeBody {
        Text(String),
        Error(String),
        Stall,
    }

    #[derive(Clone)]
    struct FakeReply {
        status: StatusCode,
        body: FakeBody,
    }

    impl FakeReply {
        fn text(status: StatusCode, body: impl Into<String>) -> Self {
            Self {
                status,
                body: FakeBody::Text(body.into()),
            }
        }

        fn stalled(status: StatusCode) -> Self {
            Self {
                status,
                body: FakeBody::Stall,
            }
        }

        fn body_error(status: StatusCode, error: impl Into<String>) -> Self {
            Self {
                status,
                body: FakeBody::Error(error.into()),
            }
        }
    }

    struct FakeTransport {
        replies: Mutex<VecDeque<FakeReply>>,
        fallback: Option<FakeReply>,
        calls: AtomicUsize,
        methods: Mutex<Vec<Method>>,
    }

    impl FakeTransport {
        fn sequence(replies: impl IntoIterator<Item = FakeReply>) -> Self {
            Self {
                replies: Mutex::new(replies.into_iter().collect()),
                fallback: None,
                calls: AtomicUsize::new(0),
                methods: Mutex::new(Vec::new()),
            }
        }

        fn repeating(reply: FakeReply) -> Self {
            Self {
                replies: Mutex::new(VecDeque::new()),
                fallback: Some(reply),
                calls: AtomicUsize::new(0),
                methods: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn methods(&self) -> Vec<Method> {
            self.methods
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        }
    }

    impl EmulatorTransport for FakeTransport {
        fn send(
            &self,
            request: super::EmulatorRequest,
        ) -> HttpFuture<Result<EmulatorResponse, String>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.methods
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(request.method);
            let reply = self
                .replies
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pop_front()
                .or_else(|| self.fallback.clone());

            Box::pin(async move {
                let reply = reply.ok_or_else(|| "fake transport exhausted".to_string())?;
                let body: HttpFuture<Result<String, String>> = match reply.body {
                    FakeBody::Text(body) => Box::pin(async move { Ok(body) }),
                    FakeBody::Error(error) => Box::pin(async move { Err(error) }),
                    FakeBody::Stall => Box::pin(std::future::pending()),
                };
                Ok(EmulatorResponse {
                    status: reply.status,
                    body,
                })
            })
        }
    }

    fn test_timeouts() -> DdlTimeouts {
        DdlTimeouts {
            connect: Duration::from_millis(10),
            attempt: Duration::from_millis(10),
            request: Duration::from_millis(50),
            operation: Duration::from_millis(20),
            list: Duration::from_millis(50),
            retry_initial: Duration::ZERO,
            retry_max: Duration::ZERO,
            poll_initial: Duration::from_millis(1),
            poll_max: Duration::from_millis(1),
            max_attempts: 3,
            max_pages: 5,
        }
    }

    async fn initialize_test_cached_client(
        cache: Arc<Mutex<ClientCache<usize>>>,
        key: &str,
        constructions: Arc<AtomicUsize>,
        initialization_delay: Duration,
        wait_timeout: Duration,
        value: usize,
    ) -> Result<Arc<usize>, SpannerError> {
        get_or_create_cached_client(cache, key.to_string(), wait_timeout, move || async move {
            constructions.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(initialization_delay).await;
            Ok(Arc::new(value))
        })
        .await
    }

    fn varchar_value(s: &str) -> duckdb::vtab::Value {
        let c_str = CString::new(s).unwrap();
        unsafe { duckdb::vtab::Value::from(duckdb_create_varchar(c_str.as_ptr())) }
    }

    fn null_value() -> duckdb::vtab::Value {
        unsafe { duckdb::vtab::Value::from(duckdb_create_null_value()) }
    }

    fn bigint_value(value: i64) -> duckdb::vtab::Value {
        unsafe { duckdb::vtab::Value::from(duckdb_create_int64(value)) }
    }

    fn varchar_list_value(values: &[&str]) -> duckdb::vtab::Value {
        let c_strings: Vec<_> = values
            .iter()
            .map(|value| CString::new(*value).unwrap())
            .collect();
        let duckdb_values: Vec<_> = c_strings
            .iter()
            .map(|value| unsafe { duckdb_create_varchar(value.as_ptr()) })
            .collect();

        list_value(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, duckdb_values)
    }

    fn bigint_list_value(values: &[i64]) -> duckdb::vtab::Value {
        let duckdb_values: Vec<_> = values
            .iter()
            .map(|value| unsafe { duckdb_create_int64(*value) })
            .collect();

        list_value(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, duckdb_values)
    }

    fn list_value(
        child_type_id: duckdb::ffi::duckdb_type,
        mut values: Vec<duckdb_value>,
    ) -> duckdb::vtab::Value {
        unsafe {
            let mut child_type = duckdb_create_logical_type(child_type_id);
            let value =
                duckdb_create_list_value(child_type, values.as_mut_ptr(), values.len() as u64);
            duckdb_destroy_logical_type(&mut child_type);

            for mut value in values {
                duckdb_destroy_value(&mut value);
            }

            duckdb::vtab::Value::from(value)
        }
    }
}
