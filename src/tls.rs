//! Validated immutable Omni TLS snapshots. File paths and private-key material
//! never participate in cache identity or Debug output.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::sync::Arc;

use google_cloud_spanner::omni::TlsConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject};

use crate::connection::{ConnectionProfile, ConnectionProfileError, EndpointMode, TlsMode};

pub(crate) const ROOT_CA_OPTION: &str = "spanner_tls_root_ca_file";
pub(crate) const CLIENT_CERT_OPTION: &str = "spanner_tls_client_cert_file";
pub(crate) const CLIENT_KEY_OPTION: &str = "spanner_tls_client_key_file";
pub(crate) const SERVER_NAME_OPTION: &str = "spanner_tls_server_name";
const MAX_PEM_BYTES: u64 = 1024 * 1024;

pub(crate) struct TlsOptions {
    root_ca: Option<String>,
    client_cert: Option<String>,
    client_key: Option<String>,
    server_name: Option<String>,
}

impl TlsOptions {
    pub(crate) fn from_settings(mut get: impl FnMut(&str) -> Option<String>) -> Self {
        let mut value = |name| get(name).filter(|value| !value.trim().is_empty());
        Self {
            root_ca: value(ROOT_CA_OPTION),
            client_cert: value(CLIENT_CERT_OPTION),
            client_key: value(CLIENT_KEY_OPTION),
            server_name: value(SERVER_NAME_OPTION).map(|name| name.trim().to_owned()),
        }
    }

    fn supplied(&self) -> bool {
        self.root_ca.is_some()
            || self.client_cert.is_some()
            || self.client_key.is_some()
            || self.server_name.is_some()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TlsIdentity {
    root_set: Option<[u8; 32]>,
    client_chain: Option<[u8; 32]>,
    server_name: Option<String>,
}

#[derive(Clone)]
pub(crate) struct TlsSnapshot {
    identity: TlsIdentity,
    // The upstream config retains PEM key bytes and derives a revealing Debug.
    // Keep it private and redact our own Debug; no zeroization promise is made.
    sdk: TlsConfig,
}

impl fmt::Debug for TlsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsSnapshot")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl PartialEq for TlsSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}
impl Eq for TlsSnapshot {}
impl Hash for TlsSnapshot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

impl TlsSnapshot {
    pub(crate) fn identity(&self) -> &TlsIdentity {
        &self.identity
    }
    pub(crate) fn sdk_config(&self) -> TlsConfig {
        self.sdk.clone()
    }

    pub(crate) fn load(
        profile: &ConnectionProfile,
        options: &TlsOptions,
    ) -> Result<Option<Arc<Self>>, ConnectionProfileError> {
        if !options.supplied() {
            return Ok(None);
        }
        if profile.endpoint_mode() != EndpointMode::Omni || profile.tls() != TlsMode::Tls {
            return Err(error(
                "Custom TLS settings require endpoint_mode='omni' and an HTTPS endpoint",
            ));
        }
        if options.client_cert.is_some() != options.client_key.is_some() {
            return Err(error(
                "spanner_tls_client_cert_file and spanner_tls_client_key_file must be set together",
            ));
        }
        let server_name = options
            .server_name
            .as_ref()
            .map(|name| {
                let parsed = ServerName::try_from(name.clone())
                    .map_err(|_| error("Invalid spanner_tls_server_name"))?;
                Ok::<_, ConnectionProfileError>(parsed.to_str().to_ascii_lowercase())
            })
            .transpose()?;
        let mut sdk = TlsConfig::new();
        if let Some(name) = &server_name {
            sdk = sdk.with_domain_name_override(name);
        }
        let root_set = if let Some(path) = &options.root_ca {
            let pem = read_pem(path, ROOT_CA_OPTION)?;
            let roots = certificates(&pem, ROOT_CA_OPTION)?;
            let mut normalized = roots
                .iter()
                .map(|cert| cert.as_ref().to_vec())
                .collect::<Vec<_>>();
            normalized.sort();
            normalized.dedup();
            sdk = sdk.with_root_certificate_pem(pem);
            Some(digest(normalized.iter().map(Vec::as_slice)))
        } else {
            None
        };
        let client_chain =
            if let (Some(cert), Some(key)) = (&options.client_cert, &options.client_key) {
                let cert_pem = read_pem(cert, CLIENT_CERT_OPTION)?;
                let key_pem = read_pem(key, CLIENT_KEY_OPTION)?;
                let chain = certificates(&cert_pem, CLIENT_CERT_OPTION)?;
                let private = PrivateKeyDer::from_pem_slice(&key_pem)
                    .map_err(|_| error("Invalid PEM in spanner_tls_client_key_file"))?;
                let signing_key = rustls::crypto::ring::default_provider()
                    .key_provider
                    .load_private_key(private)
                    .map_err(|_| error("Invalid Omni client private key"))?;
                let certified = rustls::sign::CertifiedKey::new(chain.clone(), signing_key);
                // from_der also accepts an unknown match; prove it explicitly before
                // deriving a public identity or reusing an existing cached client.
                certified
                    .keys_match()
                    .map_err(|_| error("Omni client certificate and private key do not match"))?;
                let identity = digest(chain.iter().map(|cert| cert.as_ref()));
                sdk = sdk.with_client_certificate_pem(cert_pem, key_pem);
                Some(identity)
            } else {
                None
            };
        Ok(Some(Arc::new(Self {
            identity: TlsIdentity {
                root_set,
                client_chain,
                server_name,
            },
            sdk,
        })))
    }
}

fn error(message: impl Into<String>) -> ConnectionProfileError {
    ConnectionProfileError::new(message)
}

fn read_pem(path: &str, option: &str) -> Result<Vec<u8>, ConnectionProfileError> {
    // Reject directories/devices/FIFOs before opening. Recheck the opened file
    // and cap the read as well, so growth cannot bypass the metadata limit.
    let metadata = std::fs::metadata(path).map_err(|_| error(format!("Cannot read {option}")))?;
    if !metadata.is_file() || metadata.len() > MAX_PEM_BYTES {
        return Err(error(format!(
            "{option} must be a regular PEM file of at most 1 MiB"
        )));
    }
    let file = std::fs::File::open(path).map_err(|_| error(format!("Cannot read {option}")))?;
    if !file
        .metadata()
        .map_err(|_| error(format!("Cannot inspect {option}")))?
        .is_file()
    {
        return Err(error(format!("{option} must be a regular file")));
    }
    let mut bytes = Vec::new();
    file.take(MAX_PEM_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| error(format!("Cannot read {option}")))?;
    if bytes.len() as u64 > MAX_PEM_BYTES {
        return Err(error(format!("{option} exceeds 1 MiB")));
    }
    Ok(bytes)
}

fn certificates(
    pem: &[u8],
    option: &str,
) -> Result<Vec<CertificateDer<'static>>, ConnectionProfileError> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| error(format!("Invalid PEM certificates in {option}")))?;
    if certificates.is_empty() {
        return Err(error(format!("{option} contains no certificates")));
    }
    let mut store = rustls::RootCertStore::empty();
    for cert in &certificates {
        store
            .add(cert.clone())
            .map_err(|_| error(format!("Invalid certificate in {option}")))?;
    }
    Ok(certificates)
}

fn digest<'a>(chunks: impl Iterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
    for chunk in chunks {
        hash.update(&(chunk.len() as u64).to_be_bytes());
        hash.update(chunk);
    }
    hash.finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 digest length")
}

#[cfg(test)]
#[path = "../tests/tls_support/mod.rs"]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use test_support::Certificates;

    fn profile(mode: &str, endpoint: &str) -> ConnectionProfile {
        ConnectionProfile::resolve(
            "projects/p/instances/i/databases/d".into(),
            Some(endpoint.into()),
            Some(mode),
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn tls_settings_reject_wrong_profiles_before_files() {
        for (mode, endpoint) in [
            ("emulator", "http://localhost:9010"),
            ("custom", "https://example.test"),
            ("omni", "http://example.test"),
        ] {
            let p = profile(mode, endpoint);
            assert!(
                p.clone()
                    .with_tls_settings(|_| None)
                    .unwrap()
                    .custom_tls()
                    .is_none()
            );
            let error = p
                .with_tls_settings(|name| (name == ROOT_CA_OPTION).then(|| "/missing".into()))
                .unwrap_err();
            assert!(error.to_string().contains("require endpoint_mode='omni'"));
        }
    }

    #[test]
    fn tls_settings_validate_pairs_names_and_material() {
        let certs = Certificates::new();
        let p = profile("omni", "https://example.test");
        assert!(
            p.clone()
                .with_tls_settings(|name| if name == CLIENT_KEY_OPTION {
                    None
                } else {
                    certs.setting(name)
                })
                .unwrap_err()
                .to_string()
                .contains("set together")
        );
        assert!(
            p.clone()
                .with_tls_settings(|name| if name == SERVER_NAME_OPTION {
                    Some("invalid name".into())
                } else {
                    certs.setting(name)
                })
                .unwrap_err()
                .to_string()
                .contains("Invalid spanner_tls_server_name")
        );
        std::fs::write(certs.directory.join("ca.pem"), "not a certificate").unwrap();
        assert!(
            p.clone()
                .with_tls_settings(|name| certs.setting(name))
                .unwrap_err()
                .to_string()
                .contains("no certificates")
        );
        std::fs::write(certs.directory.join("ca.pem"), &certs.ca).unwrap();
        std::fs::write(certs.directory.join("client.key"), "not a key").unwrap();
        assert!(
            p.with_tls_settings(|name| certs.setting(name))
                .unwrap_err()
                .to_string()
                .contains("Invalid PEM")
        );
    }

    #[test]
    fn tls_settings_bound_files_and_reject_nonregular_inputs() {
        let certs = Certificates::new();
        let p = profile("omni", "https://example.test");
        assert!(
            p.clone()
                .with_tls_settings(|name| (name == ROOT_CA_OPTION).then(|| certs
                    .directory
                    .to_str()
                    .unwrap()
                    .into()))
                .unwrap_err()
                .to_string()
                .contains("regular PEM file")
        );
        let oversized = std::fs::File::create(certs.directory.join("ca.pem")).unwrap();
        oversized.set_len(MAX_PEM_BYTES + 1).unwrap();
        assert!(
            p.with_tls_settings(|name| certs.setting(name))
                .unwrap_err()
                .to_string()
                .contains("at most 1 MiB")
        );
    }

    #[test]
    fn tls_identity_normalizes_roots_and_rotates_without_paths_or_secrets() {
        let certs = Certificates::new();
        let other = Certificates::new();
        let p = profile("omni", "https://example.test");
        let first = p
            .clone()
            .with_tls_settings(|name| certs.setting(name))
            .unwrap();
        std::fs::write(
            certs.directory.join("ca.pem"),
            format!("{}{}", certs.ca, certs.ca),
        )
        .unwrap();
        let duplicated = p
            .clone()
            .with_tls_settings(|name| certs.setting(name))
            .unwrap();
        assert_eq!(first.identity(), duplicated.identity());
        std::fs::write(certs.directory.join("ca.pem"), &other.ca).unwrap();
        let rotated = p
            .clone()
            .with_tls_settings(|name| certs.setting(name))
            .unwrap();
        assert_ne!(first.identity(), rotated.identity());
        assert_ne!(first.cache_key(), rotated.cache_key());
        let text = format!("{first:?} {}", first.cache_key());
        assert!(!text.contains("BEGIN"));
        assert!(!text.contains(&certs.client_key));
        assert!(!text.contains(certs.directory.to_str().unwrap()));
        // A changed key must be validated even when the prior public identity
        // has a cached client. No private-key digest can make a bad pair valid.
        std::fs::write(certs.directory.join("client.key"), &other.client_key).unwrap();
        assert!(
            p.with_tls_settings(|name| certs.setting(name))
                .unwrap_err()
                .to_string()
                .contains("do not match")
        );
        assert!(first.custom_tls().is_some()); // old immutable snapshot still owns its valid bytes
    }

    #[test]
    fn tls_identity_separates_public_chain_and_name_and_admin_fails_closed() {
        let certs = Certificates::new();
        let other = Certificates::new();
        let p = profile("omni", "https://example.test");
        assert!(p.ensure_admin_transport().is_ok());
        let first = p
            .clone()
            .with_tls_settings(|name| certs.setting(name))
            .unwrap();
        assert!(
            first
                .ensure_admin_transport()
                .unwrap_err()
                .to_string()
                .contains("DatabaseAdmin REST")
        );
        let renamed = p
            .clone()
            .with_tls_settings(|name| {
                if name == SERVER_NAME_OPTION {
                    Some("other.test".into())
                } else {
                    certs.setting(name)
                }
            })
            .unwrap();
        assert_ne!(first.identity(), renamed.identity());
        std::fs::write(certs.directory.join("client.pem"), &other.client_cert).unwrap();
        std::fs::write(certs.directory.join("client.key"), &other.client_key).unwrap();
        let rotated = p.with_tls_settings(|name| certs.setting(name)).unwrap();
        assert_ne!(first.identity(), rotated.identity());
    }
}
