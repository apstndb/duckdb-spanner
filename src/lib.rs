#[cfg(all(test, not(feature = "static-extension")))]
compile_error!(
    "Run Rust tests with --features static-extension to exercise the native v2 entrypoint"
);
mod bind_utils;
mod cache;
mod client;
mod config;
mod connection;
mod convert;
mod copy;
mod ddl;
mod duck;
mod error;
mod params;
mod query;
mod replacement;
mod runtime;
mod scalars;
mod scan;
mod schema;
mod streaming;
mod tables;
#[cfg(test)]
mod test_support;
mod tls;
mod types;
mod vector_size;
mod vtab_safety;

pub use ddl::{SpannerDdlAsyncVTab, SpannerDdlVTab, SpannerOperationsVTab};
use duckdb_neo::{
    extension::Extension,
    macro_function::{MacroBody, MacroFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::Any,
};
pub use query::SpannerQueryVTab;
pub use scan::SpannerScanVTab;
use std::fmt;
pub use tables::SpannerTablesVTab;
/// A deterministic extension-registration failure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RegistrationError {
    stage: &'static str,
    name: String,
    detail: String,
}

impl RegistrationError {
    pub(crate) fn new(
        stage: &'static str,
        name: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            stage,
            name: name.into(),
            detail: detail.into(),
        }
    }

    pub fn stage(&self) -> &str {
        self.stage
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "extension registration failed at stage '{}' for '{}': {}",
            self.stage, self.name, self.detail
        )
    }
}

impl std::error::Error for RegistrationError {}

/// Register the native v2 surface. The loading Extension supplies the public
/// factory and registration context; no private connection handles are read.
///
/// Config options are registered first. Native registration cannot roll back
/// a later failure; a retry then fails on the existing first option. Temporary
/// macros may shadow these functions, a documented v2 contract difference.
pub fn register_spanner_extension(extension: &Extension) -> Result<(), RegistrationError> {
    ensure_rustls_crypto_provider();
    config::register(extension)
        .map_err(|e| RegistrationError::new("register config", "spanner", e.to_string()))?;
    copy::register(extension)
        .map_err(|e| RegistrationError::new("register COPY", "spanner", e.to_string()))?;
    scalars::register(extension)
        .map_err(|e| RegistrationError::new("register scalars", "spanner", e.to_string()))?;
    macro_rules! table {
        ($ty:ty,$name:literal) => {
            duck::register::<$ty>(extension, $name)
                .map_err(|e| RegistrationError::new("register table", $name, e.to_string()))?;
        };
    }
    table!(SpannerQueryVTab, "spanner_query_raw");
    table!(SpannerScanVTab, "spanner_scan");
    table!(SpannerTablesVTab, "spanner_tables");
    table!(SpannerDdlVTab, "spanner_ddl_raw");
    table!(SpannerDdlAsyncVTab, "spanner_ddl_async_raw");
    table!(SpannerOperationsVTab, "spanner_operations_raw");
    register_macros(extension)?;
    replacement::register(extension)
        .map_err(|e| RegistrationError::new("register replacement scan", "spanner:", e.to_string()))
}
/// A typed STRUCT NULL preserves omitted params without accepting VARCHAR input.
/// The v2 factory cannot construct an untyped SQLNULL value.
struct OmittedParams;
impl duckdb_neo::ToValue for OmittedParams {
    fn value<C: duckdb_neo::connection::FFILink + ?Sized>(
        &self,
        link: &C,
    ) -> duckdb_neo::Result<duckdb_neo::value::Value> {
        let ty = duckdb_neo::logical_type::LogicalType::from_text(
            link,
            "STRUCT(__spanner_absent VARCHAR)",
        )?;
        duckdb_neo::value::Value::null(link, &ty)
    }
}
fn register_macros(extension: &Extension) -> Result<(), RegistrationError> {
    for declaration in include_str!("macros.sql").split("CREATE MACRO ").skip(1) {
        let (header, body) = declaration.split_once(") AS TABLE").ok_or_else(|| {
            RegistrationError::new("parse macro", "spanner", "expected table macro")
        })?;
        let (name, args) = header
            .split_once('(')
            .ok_or_else(|| RegistrationError::new("parse macro", "spanner", "missing signature"))?;
        let (body, _) = body
            .split_once(';')
            .ok_or_else(|| RegistrationError::new("parse macro", name, "missing terminator"))?;
        let mut signature = SignatureBuilder::new();
        for arg in args.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (name, default) = match arg.split_once(":=") {
                Some((n, d)) => (n.trim(), Some(d.trim())),
                None => (arg, None),
            };
            let mut param = FunctionParameter::new::<Any>(name);
            if let Some(default) = default {
                if default != "NULL" {
                    return Err(RegistrationError::new(
                        "parse macro",
                        name,
                        "only NULL defaults supported",
                    ));
                }
                // The v2 factory cannot create SQLNULL. Give forwarded optional
                // arguments their actual target type: a VARCHAR NULL on an ANY
                // macro parameter cannot implicitly bind to BOOL/INT/LIST in 2.0.
                param = match name {
                    "params" => param.default_value(OmittedParams),
                    "use_parallelism" | "use_data_boost" => {
                        param.default_value(Option::<bool>::None)
                    }
                    "max_parallelism" => param.default_value(Option::<i32>::None),
                    "exact_staleness_secs" | "max_staleness_secs" => {
                        param.default_value(Option::<i64>::None)
                    }
                    "statements" => {
                        param.default_value(Option::<duckdb_neo::types::List<String>>::None)
                    }
                    _ => param.default_value(Option::<String>::None),
                };
            }
            signature = signature.param(param);
        }
        MacroFunctionBuilder::new(name.trim(), signature, MacroBody::Table(body.trim().into()))
            .register(extension)
            .map_err(|e| RegistrationError::new("register macro", name, e.to_string()))?;
    }
    Ok(())
}
fn ensure_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
#[cfg(any(feature = "loadable-extension", feature = "static-extension"))]
#[duckdb_neo::duckdb_entrypoint_c_api_v2(extension_name = "spanner", api_version = "v2.0.0")]
fn entrypoint(extension: &Extension) -> duckdb_neo::Result<()> {
    vtab_safety::guard_callback("spanner", "initialization", || {
        register_spanner_extension(extension).map_err(Into::into)
    })
    .map_err(|e| duckdb_neo::Error::invalid_input(e.to_string()))
}
/// Register the same native v2 entrypoint for an embedded DuckDB 2.0.0 client.
/// Call before opening a DuckDB instance. This does not load a second backend.
#[cfg(feature = "static-extension")]
pub fn register_static_extension() -> duckdb_neo::Result<()> {
    // SAFETY: the descriptor and entrypoint are generated together by Neo.
    unsafe { duckdb_neo::extension::register_static_extension(duckdb_extension_spanner_describe) }
}
