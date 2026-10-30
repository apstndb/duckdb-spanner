//! Session options through the public v2 Context API, copied into owned bind snapshots.
use crate::{
    duck::BindInfo,
    streaming::{DEFAULT_STREAM_IDLE_TIMEOUT_SECS, StreamTimeoutPolicy},
};
use duckdb_neo::{
    DuckDBType, Result, ToValue,
    connection::{Context, SettingScope},
    custom_option::{CustomOptionBuilder, CustomOptionCallbacks},
    extension::Extension,
    value::Value,
};
use std::collections::HashMap;
pub(crate) const STREAM_IDLE_TIMEOUT_OPTION: &str = "spanner_stream_idle_timeout_secs";
pub(crate) const STRING_OPTIONS: &[(&str, &str)] = &[
    (
        "spanner_project",
        "Default Google Cloud project ID for Spanner",
    ),
    ("spanner_instance", "Default Spanner instance ID"),
    ("spanner_database", "Default Spanner database ID"),
    (
        "spanner_database_path",
        "Default Spanner database resource path",
    ),
    ("spanner_endpoint", "Default Spanner gRPC endpoint"),
    (
        "spanner_endpoint_mode",
        "Spanner endpoint mode: default, emulator, custom, or omni",
    ),
    (
        "spanner_admin_endpoint",
        "Default Spanner admin REST endpoint",
    ),
    (
        crate::tls::ROOT_CA_OPTION,
        "Experimental Omni root CA PEM file",
    ),
    (
        crate::tls::CLIENT_CERT_OPTION,
        "Experimental Omni client certificate PEM file",
    ),
    (
        crate::tls::CLIENT_KEY_OPTION,
        "Experimental Omni private key PEM file",
    ),
    (
        crate::tls::SERVER_NAME_OPTION,
        "Experimental Omni TLS verification server name",
    ),
];
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    strings: HashMap<String, String>,
    timeout: i64,
}
impl Settings {
    pub(crate) fn read(ctx: &Context) -> Result<Self> {
        let config = ctx.config()?;
        let mut strings = HashMap::new();
        for (name, _) in STRING_OPTIONS {
            if let Some((value, _)) = config.try_get_value(name)?
                && let Some(text) = value.get::<String>()?.filter(|s| !s.is_empty())
            {
                strings.insert(name.to_string(), text);
            }
        }
        let timeout = match config.try_get_value(STREAM_IDLE_TIMEOUT_OPTION)? {
            Some((v, _)) => v.get::<i64>()?.unwrap_or(DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
            None => DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
        };
        Ok(Self { strings, timeout })
    }
    pub(crate) fn string(&self, name: &str) -> Option<String> {
        self.strings.get(name).cloned()
    }
}
struct Update;
impl CustomOptionCallbacks for Update {
    fn update(&self, _: &Context, value: Value, _: SettingScope, _: bool) -> Result<Value> {
        Ok(value)
    }
}
pub(crate) fn register(ext: &Extension) -> Result<()> {
    // Custom options are irreversible. The public registration API rejects duplicate names; config-first
    // ordering makes every later failure detectable on the next attempt.
    // Public v2 has no equivalent function-catalog preflight for temporary macros.
    for (name, description) in STRING_OPTIONS {
        CustomOptionBuilder::new(*name, *description, String::logical_type(ext)?, Update)
            .register(ext)?;
    }
    CustomOptionBuilder::new(
        STREAM_IDLE_TIMEOUT_OPTION,
        "Maximum stream idle seconds; 0 disables",
        i64::logical_type(ext)?,
        Update,
    )
    .default_value(DEFAULT_STREAM_IDLE_TIMEOUT_SECS.value(ext)?)
    .register(ext)
}
pub fn get_config_string(bind: &BindInfo, name: &str) -> Option<String> {
    bind.settings.string(name)
}
pub(crate) fn resolve_stream_timeout_policy(
    bind: &BindInfo,
) -> std::result::Result<StreamTimeoutPolicy, Box<dyn std::error::Error>> {
    StreamTimeoutPolicy::from_seconds(bind.settings.timeout).map_err(Into::into)
}
