//! Typed connection-target resolution shared by Spanner table functions.
//!
//! This module deliberately resolves endpoint mode, credentials, and transport
//! together. The official client treats `SPANNER_EMULATOR_HOST` process-wide,
//! so passing an endpoint string alone is not enough to identify a connection.

use std::fmt;

/// Transport and credential behavior for a Spanner connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum EndpointMode {
    /// The official Spanner endpoint selected by the SDK, using ADC and TLS.
    Default,
    /// An emulator-compatible endpoint using anonymous credentials.
    Emulator,
    /// An explicitly selected TLS endpoint using ADC.
    Custom,
    /// A Spanner Omni endpoint using anonymous credentials.
    Omni,
}

/// Authentication behavior required by a resolved connection profile.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum AuthenticationMode {
    ApplicationDefaultCredentials,
    Anonymous,
}

/// TLS behavior required by a resolved data endpoint.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TlsMode {
    /// The SDK default endpoint, which uses TLS.
    Default,
    Tls,
    Plaintext,
}

/// A hashable identity for a fully resolved client target.
///
/// Keep this typed instead of keying caches by a bare endpoint string: two
/// modes can share a host while requiring different credentials or transport.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ConnectionIdentity {
    database_path: String,
    endpoint_mode: EndpointMode,
    data_endpoint: Option<String>,
    authentication: AuthenticationMode,
    tls: TlsMode,
    sdk_emulator_mode: bool,
}

#[cfg(test)]
impl ConnectionIdentity {
    pub(crate) fn database_path(&self) -> &str {
        &self.database_path
    }

    pub(crate) fn endpoint_mode(&self) -> EndpointMode {
        self.endpoint_mode
    }

    pub(crate) fn data_endpoint(&self) -> Option<&str> {
        self.data_endpoint.as_deref()
    }

    pub(crate) fn authentication(&self) -> AuthenticationMode {
        self.authentication
    }

    pub(crate) fn tls(&self) -> TlsMode {
        self.tls
    }

    pub(crate) fn sdk_emulator_mode(&self) -> bool {
        self.sdk_emulator_mode
    }
}

/// A resolved Spanner database and endpoint configuration.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ConnectionProfile {
    database_path: String,
    endpoint_mode: EndpointMode,
    data_endpoint: Option<String>,
    explicit_admin_endpoint: Option<String>,
    sdk_emulator_mode: bool,
}

impl ConnectionProfile {
    pub(crate) fn resolve(
        database_path: String,
        endpoint: Option<String>,
        endpoint_mode: Option<&str>,
        admin_endpoint: Option<String>,
        emulator_host: Option<&str>,
    ) -> Result<Self, ConnectionProfileError> {
        let endpoint = nonempty(endpoint);
        let admin_endpoint = nonempty(admin_endpoint);
        // Match the pinned SDK exactly: every non-empty raw value activates
        // its emulator behavior. Whitespace is therefore rejected later as an
        // invalid endpoint instead of being silently treated as unset.
        let emulator_host = emulator_host.filter(|value| !value.is_empty());
        let sdk_emulator_mode = emulator_host.is_some();
        let explicit_mode = nonempty_ref(endpoint_mode);
        let endpoint_mode = parse_endpoint_mode(explicit_mode)?;

        let mode = match endpoint_mode {
            Some(mode) => mode,
            None if endpoint.is_some() || emulator_host.is_some() => EndpointMode::Emulator,
            None => EndpointMode::Default,
        };

        if explicit_mode.is_some() && mode == EndpointMode::Default && endpoint.is_some() {
            return Err(ConnectionProfileError::new(
                "endpoint_mode 'default' cannot be combined with endpoint",
            ));
        }

        let source_endpoint = match mode {
            EndpointMode::Default => None,
            EndpointMode::Emulator => endpoint.or_else(|| emulator_host.map(str::to_owned)),
            EndpointMode::Custom | EndpointMode::Omni => endpoint,
        };

        match mode {
            EndpointMode::Emulator if source_endpoint.is_none() => {
                return Err(ConnectionProfileError::new(
                    "endpoint_mode 'emulator' requires endpoint or SPANNER_EMULATOR_HOST",
                ));
            }
            EndpointMode::Custom | EndpointMode::Omni if source_endpoint.is_none() => {
                return Err(ConnectionProfileError::new(format!(
                    "endpoint_mode '{}' requires endpoint",
                    endpoint_mode_name(mode)
                )));
            }
            EndpointMode::Default | EndpointMode::Custom | EndpointMode::Omni
                if emulator_host.is_some() =>
            {
                return Err(ConnectionProfileError::new(format!(
                    "endpoint_mode '{}' cannot be used while SPANNER_EMULATOR_HOST is set; \\
                     the official client would force anonymous emulator credentials",
                    endpoint_mode_name(mode)
                )));
            }
            _ => {}
        }

        let data_endpoint = source_endpoint
            .as_deref()
            .map(|endpoint| normalize_data_endpoint(mode, endpoint))
            .transpose()?;
        let explicit_admin_endpoint = admin_endpoint
            .as_deref()
            .map(|endpoint| normalize_admin_endpoint(mode, endpoint))
            .transpose()?;

        Ok(Self {
            database_path,
            endpoint_mode: mode,
            data_endpoint,
            explicit_admin_endpoint,
            sdk_emulator_mode,
        })
    }

    pub(crate) fn database_path(&self) -> &str {
        &self.database_path
    }

    pub(crate) fn endpoint_mode(&self) -> EndpointMode {
        self.endpoint_mode
    }

    pub(crate) fn data_endpoint(&self) -> Option<&str> {
        self.data_endpoint.as_deref()
    }

    /// Resolve the endpoint used by the DatabaseAdmin client.
    ///
    /// Custom gateways and Spanner Omni serve data and admin RPCs on the same
    /// endpoint by default. The emulator exposes its REST admin surface on
    /// port 9020 when the data endpoint uses the conventional port 9010.
    pub(crate) fn admin_endpoint(&self) -> Option<String> {
        if let Some(endpoint) = &self.explicit_admin_endpoint {
            return Some(endpoint.clone());
        }

        match self.endpoint_mode {
            EndpointMode::Default => None,
            EndpointMode::Custom | EndpointMode::Omni => self.data_endpoint.clone(),
            EndpointMode::Emulator => self.data_endpoint.as_deref().map(emulator_admin_endpoint),
        }
    }

    pub(crate) fn authentication(&self) -> AuthenticationMode {
        match self.endpoint_mode {
            EndpointMode::Default | EndpointMode::Custom => {
                AuthenticationMode::ApplicationDefaultCredentials
            }
            EndpointMode::Emulator | EndpointMode::Omni => AuthenticationMode::Anonymous,
        }
    }

    pub(crate) fn tls(&self) -> TlsMode {
        match self.data_endpoint.as_deref() {
            None => TlsMode::Default,
            Some(endpoint) if endpoint.starts_with("https://") => TlsMode::Tls,
            Some(_) => TlsMode::Plaintext,
        }
    }

    pub(crate) fn sdk_emulator_mode(&self) -> bool {
        self.sdk_emulator_mode
    }

    pub(crate) fn identity(&self) -> ConnectionIdentity {
        ConnectionIdentity {
            database_path: self.database_path.clone(),
            endpoint_mode: self.endpoint_mode,
            data_endpoint: self.data_endpoint.clone(),
            authentication: self.authentication(),
            tls: self.tls(),
            sdk_emulator_mode: self.sdk_emulator_mode,
        }
    }

    /// A deterministic string key for string-keyed caches during migration.
    /// New caches should retain [`ConnectionIdentity`] directly where possible.
    pub(crate) fn cache_key(&self) -> String {
        let identity = self.identity();
        format!(
            "database={:?};mode={:?};endpoint={:?};auth={:?};tls={:?};sdk_emulator={}",
            identity.database_path,
            identity.endpoint_mode,
            identity.data_endpoint,
            identity.authentication,
            identity.tls,
            identity.sdk_emulator_mode,
        )
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionProfileError(String);

impl ConnectionProfileError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ConnectionProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConnectionProfileError {}

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_owned())
    })
}

fn nonempty_ref(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then_some(value)
    })
}

fn parse_endpoint_mode(
    value: Option<&str>,
) -> Result<Option<EndpointMode>, ConnectionProfileError> {
    value
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "default" => Ok(EndpointMode::Default),
            "emulator" => Ok(EndpointMode::Emulator),
            "custom" => Ok(EndpointMode::Custom),
            "omni" => Ok(EndpointMode::Omni),
            _ => Err(ConnectionProfileError::new(format!(
                "Invalid endpoint_mode '{value}': must be 'default', 'emulator', 'custom', or 'omni'"
            ))),
        })
        .transpose()
}

fn endpoint_mode_name(mode: EndpointMode) -> &'static str {
    match mode {
        EndpointMode::Default => "default",
        EndpointMode::Emulator => "emulator",
        EndpointMode::Custom => "custom",
        EndpointMode::Omni => "omni",
    }
}

fn normalize_data_endpoint(
    mode: EndpointMode,
    endpoint: &str,
) -> Result<String, ConnectionProfileError> {
    let default_scheme = match mode {
        EndpointMode::Emulator => "http",
        EndpointMode::Custom | EndpointMode::Omni => "https",
        EndpointMode::Default => unreachable!("the default mode has no explicit endpoint"),
    };
    let normalized = normalize_endpoint(endpoint, default_scheme)?;
    if mode == EndpointMode::Custom && !normalized.starts_with("https://") {
        return Err(ConnectionProfileError::new(
            "endpoint_mode 'custom' requires an https endpoint",
        ));
    }
    Ok(normalized)
}

fn normalize_admin_endpoint(
    mode: EndpointMode,
    endpoint: &str,
) -> Result<String, ConnectionProfileError> {
    let default_scheme = if mode == EndpointMode::Emulator {
        "http"
    } else {
        "https"
    };
    let normalized = normalize_endpoint(endpoint, default_scheme)?;
    if matches!(mode, EndpointMode::Default | EndpointMode::Custom)
        && !normalized.starts_with("https://")
    {
        return Err(ConnectionProfileError::new(
            "authenticated admin endpoints require https",
        ));
    }
    Ok(normalized)
}

fn emulator_admin_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if let Some(prefix) = endpoint.strip_suffix(":9010") {
        format!("{prefix}:9020")
    } else {
        endpoint.to_owned()
    }
}

fn normalize_endpoint(
    endpoint: &str,
    default_scheme: &str,
) -> Result<String, ConnectionProfileError> {
    let endpoint = endpoint.trim();
    let (scheme, target) = match endpoint.split_once("://") {
        Some((scheme, target)) => match scheme.to_ascii_lowercase().as_str() {
            "http" => ("http", target),
            "https" => ("https", target),
            _ => {
                return Err(ConnectionProfileError::new(format!(
                    "endpoint '{endpoint}' must use http or https"
                )));
            }
        },
        None if endpoint.contains("://") => {
            return Err(ConnectionProfileError::new(format!(
                "endpoint '{endpoint}' must use http or https"
            )));
        }
        None => (default_scheme, endpoint),
    };
    let target = target.trim_end_matches('/');
    if target.is_empty() || target.chars().any(char::is_whitespace) {
        return Err(ConnectionProfileError::new(format!(
            "endpoint '{endpoint}' must include a host without whitespace"
        )));
    }
    Ok(format!("{scheme}://{target}"))
}

#[cfg(test)]
mod tests {
    use super::{AuthenticationMode, ConnectionProfile, EndpointMode, TlsMode};

    fn resolve(
        endpoint: Option<&str>,
        mode: Option<&str>,
        admin_endpoint: Option<&str>,
        emulator_host: Option<&str>,
    ) -> Result<ConnectionProfile, String> {
        ConnectionProfile::resolve(
            "projects/p/instances/i/databases/d".to_owned(),
            endpoint.map(str::to_owned),
            mode,
            admin_endpoint.map(str::to_owned),
            emulator_host,
        )
        .map_err(|error| error.to_string())
    }

    #[test]
    fn infers_default_or_emulator_without_mutating_process_environment() {
        let default = resolve(None, None, None, None).unwrap();
        assert_eq!(default.endpoint_mode(), EndpointMode::Default);
        assert_eq!(default.data_endpoint(), None);
        assert_eq!(
            default.authentication(),
            AuthenticationMode::ApplicationDefaultCredentials
        );
        assert_eq!(default.tls(), TlsMode::Default);

        let emulator = resolve(None, None, None, Some("localhost:9010")).unwrap();
        assert_eq!(emulator.endpoint_mode(), EndpointMode::Emulator);
        assert_eq!(emulator.data_endpoint(), Some("http://localhost:9010"));
        assert_eq!(emulator.authentication(), AuthenticationMode::Anonymous);
        assert_eq!(emulator.tls(), TlsMode::Plaintext);
        assert!(emulator.sdk_emulator_mode());
    }

    #[test]
    fn endpoint_only_preserves_emulator_compatibility() {
        let profile = resolve(Some("https://localhost:9010/"), None, None, None).unwrap();
        assert_eq!(profile.endpoint_mode(), EndpointMode::Emulator);
        assert_eq!(profile.data_endpoint(), Some("https://localhost:9010"));
        assert_eq!(profile.tls(), TlsMode::Tls);
    }

    #[test]
    fn explicit_modes_validate_their_endpoint_and_environment_contracts() {
        assert!(resolve(Some("localhost:9010"), Some("default"), None, None)
            .unwrap_err()
            .contains("cannot be combined"));
        assert!(resolve(None, Some("emulator"), None, None)
            .unwrap_err()
            .contains("requires endpoint or SPANNER_EMULATOR_HOST"));
        assert!(resolve(None, Some("custom"), None, None)
            .unwrap_err()
            .contains("requires endpoint"));
        assert!(resolve(
            Some("localhost:443"),
            Some("omni"),
            None,
            Some("localhost:9010")
        )
        .unwrap_err()
        .contains("cannot be used while SPANNER_EMULATOR_HOST"));
        assert!(resolve(None, Some("default"), None, Some("localhost:9010"))
            .unwrap_err()
            .contains("cannot be used while SPANNER_EMULATOR_HOST"));
        assert!(resolve(None, None, None, Some("   "))
            .unwrap_err()
            .contains("must include a host"));
        assert!(resolve(Some("localhost:443"), Some("custom"), None, Some("")).is_ok());
    }

    #[test]
    fn normalizes_transport_by_mode() {
        let custom = resolve(Some("custom.example:443/"), Some("CuStOm"), None, None).unwrap();
        assert_eq!(custom.data_endpoint(), Some("https://custom.example:443"));
        assert_eq!(
            custom.authentication(),
            AuthenticationMode::ApplicationDefaultCredentials
        );
        assert_eq!(custom.tls(), TlsMode::Tls);
        assert!(
            resolve(Some("http://custom.example"), Some("custom"), None, None)
                .unwrap_err()
                .contains("requires an https endpoint")
        );

        let omni = resolve(Some("omni.example:443/"), Some("OMNI"), None, None).unwrap();
        assert_eq!(omni.data_endpoint(), Some("https://omni.example:443"));
        assert_eq!(omni.authentication(), AuthenticationMode::Anonymous);
        assert_eq!(omni.tls(), TlsMode::Tls);

        let plaintext_omni =
            resolve(Some("http://omni.example"), Some("omni"), None, None).unwrap();
        assert_eq!(plaintext_omni.tls(), TlsMode::Plaintext);
    }

    #[test]
    fn data_identity_ignores_admin_only_configuration() {
        let profile = resolve(
            Some("omni.example:443"),
            Some("omni"),
            Some("admin.example:443/"),
            None,
        )
        .unwrap();
        let identity = profile.identity();
        assert_eq!(
            identity.database_path(),
            "projects/p/instances/i/databases/d"
        );
        assert_eq!(identity.endpoint_mode(), EndpointMode::Omni);
        assert_eq!(identity.data_endpoint(), Some("https://omni.example:443"));
        assert_eq!(identity.authentication(), AuthenticationMode::Anonymous);
        assert_eq!(identity.tls(), TlsMode::Tls);
        assert!(!identity.sdk_emulator_mode());
        assert!(profile.cache_key().contains("mode=Omni"));
        assert!(profile.cache_key().contains("auth=Anonymous"));

        let without_admin = resolve(Some("omni.example:443"), Some("omni"), None, None).unwrap();
        assert_eq!(profile.identity(), without_admin.identity());
        assert_eq!(profile.cache_key(), without_admin.cache_key());

        let sdk_emulator = resolve(
            Some("omni.example:443"),
            Some("emulator"),
            None,
            Some("ignored-by-explicit-endpoint:9010"),
        )
        .unwrap();
        let manual_emulator =
            resolve(Some("omni.example:443"), Some("emulator"), None, None).unwrap();
        assert_ne!(sdk_emulator.identity(), manual_emulator.identity());
    }

    #[test]
    fn resolves_admin_endpoint_by_mode() {
        let emulator = resolve(Some("localhost:9010"), Some("emulator"), None, None).unwrap();
        assert_eq!(
            emulator.admin_endpoint().as_deref(),
            Some("http://localhost:9020")
        );

        let omni = resolve(Some("omni.example:443"), Some("omni"), None, None).unwrap();
        assert_eq!(
            omni.admin_endpoint().as_deref(),
            Some("https://omni.example:443")
        );

        let custom = resolve(
            Some("data.example:443"),
            Some("custom"),
            Some("admin.example:443"),
            None,
        )
        .unwrap();
        assert_eq!(
            custom.admin_endpoint().as_deref(),
            Some("https://admin.example:443")
        );
        let custom_default_admin =
            resolve(Some("data.example:443"), Some("custom"), None, None).unwrap();
        assert_eq!(
            custom_default_admin.admin_endpoint().as_deref(),
            Some("https://data.example:443")
        );
        assert!(resolve(
            Some("data.example:443"),
            Some("custom"),
            Some("http://admin.example"),
            None,
        )
        .unwrap_err()
        .contains("require https"));
    }
}
