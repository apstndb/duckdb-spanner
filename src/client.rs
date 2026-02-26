use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use std::sync::LazyLock;

use crate::error::SpannerError;

// TODO: Add TTL or LRU eviction to prevent unbounded growth when connecting
// to many different databases in a long-running process.
static CLIENT_CACHE: LazyLock<Mutex<HashMap<String, Arc<Client>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get or create a Spanner client.
///
/// - `endpoint: None` → default behavior (uses `SPANNER_EMULATOR_HOST` env var if set, otherwise real Spanner with auth)
/// - `endpoint: Some("host:port")` → connects to the given endpoint without auth (emulator mode)
///
/// Clients are cached by `(database, endpoint)` so both emulator and real connections can coexist.
pub async fn get_or_create_client(
    database: &str,
    endpoint: Option<&str>,
) -> Result<Arc<Client>, SpannerError> {
    let cache_key = match endpoint {
        Some(ep) => format!("{database}@{ep}"),
        None => database.to_string(),
    };

    {
        let cache = CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(client) = cache.get(&cache_key) {
            return Ok(Arc::clone(client));
        }
    }

    let config = match endpoint {
        Some(ep) => {
            // Explicit endpoint: emulator mode (no auth, plain HTTP)
            ClientConfig {
                environment: Environment::Emulator(ep.to_string()),
                ..Default::default()
            }
        }
        None => {
            // Default: respect SPANNER_EMULATOR_HOST env var, or use real auth
            if std::env::var("SPANNER_EMULATOR_HOST").is_ok() {
                ClientConfig::default()
            } else {
                ClientConfig::default()
                    .with_auth()
                    .await
                    .map_err(|e| SpannerError::Other(format!("Auth error: {e}")))?
            }
        }
    };

    let client = Client::new(database, config).await?;
    let client = Arc::new(client);

    let mut cache = CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let entry = cache
        .entry(cache_key)
        .or_insert_with(|| Arc::clone(&client));
    Ok(Arc::clone(entry))
}
