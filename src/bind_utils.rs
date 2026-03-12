use duckdb::vtab::BindInfo;
use google_cloud_googleapis::spanner::v1::request_options::Priority;
use google_cloud_spanner::value::TimestampBound;

use crate::config;

/// Configuration for Spanner read timestamp bounds.
///
/// Constructed from named parameters in bind() and converted to
/// `TimestampBound` at query/read execution time.
#[derive(Clone)]
pub enum TimestampBoundConfig {
    ExactStaleness { secs: u64 },
    MaxStaleness { secs: u64 },
    ReadTimestamp { seconds: i64, nanos: i32 },
    MinReadTimestamp { seconds: i64, nanos: i32 },
}

impl TimestampBoundConfig {
    pub fn to_timestamp_bound(&self) -> TimestampBound {
        use google_cloud_spanner::value::Timestamp;
        match self {
            Self::ExactStaleness { secs } => {
                TimestampBound::exact_staleness(std::time::Duration::from_secs(*secs))
            }
            Self::MaxStaleness { secs } => {
                TimestampBound::max_staleness(std::time::Duration::from_secs(*secs))
            }
            Self::ReadTimestamp { seconds, nanos } => {
                TimestampBound::read_timestamp(Timestamp {
                    seconds: *seconds,
                    nanos: *nanos,
                })
            }
            Self::MinReadTimestamp { seconds, nanos } => {
                TimestampBound::min_read_timestamp(Timestamp {
                    seconds: *seconds,
                    nanos: *nanos,
                })
            }
        }
    }
}

/// Check if a DuckDB Value is SQL NULL.
///
/// `duckdb::vtab::Value` is a C API pointer wrapper (`duckdb_value`), not the
/// `duckdb::types::Value` Rust enum which has a `Null` variant. The vtab Value
/// has no public null-check method, so we call `duckdb_is_null_value` from the
/// C API directly.
///
/// The pointer is extracted via reinterpret cast because `Value::ptr` is
/// `pub(crate)`. The static_assert verifies Value is a single-pointer struct.
///
/// TODO: Replace this entire function with `value.is_null()` once
/// duckdb/duckdb-rs#676 is merged.
fn value_is_null(value: &duckdb::vtab::Value) -> bool {
    // SAFETY: `duckdb::vtab::Value` wraps a single `duckdb_value` (a pointer) as its
    // only field. We reinterpret the reference to read that pointer.
    const _: () = assert!(
        std::mem::size_of::<duckdb::vtab::Value>() == std::mem::size_of::<duckdb::ffi::duckdb_value>(),
        "Value size mismatch — duckdb crate layout may have changed"
    );
    let ptr = unsafe { *(value as *const _ as *const duckdb::ffi::duckdb_value) };
    unsafe { duckdb::ffi::duckdb_is_null_value(ptr) }
}

/// Get a named VARCHAR parameter, returning None if absent, NULL, or empty.
pub fn get_named_string(bind: &BindInfo, name: &str) -> Option<String> {
    bind.get_named_parameter(name)
        .filter(|v| !value_is_null(v))
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty())
}

/// Get a named integer parameter, returning None if absent or NULL.
pub fn get_named_int64(bind: &BindInfo, name: &str) -> Option<i64> {
    bind.get_named_parameter(name)
        .filter(|v| !value_is_null(v))
        .map(|v| v.to_int64())
}

/// Get a named boolean parameter, returning the default if absent or NULL.
pub fn get_named_bool(bind: &BindInfo, name: &str, default: bool) -> bool {
    bind.get_named_parameter(name)
        .filter(|v| !value_is_null(v))
        .map(|v| v.to_int64() != 0)
        .unwrap_or(default)
}

/// Parse a priority string ("low", "medium", "high") into a Spanner Priority enum.
pub fn parse_priority(s: &str) -> Result<Priority, Box<dyn std::error::Error>> {
    match s.to_ascii_lowercase().as_str() {
        "low" => Ok(Priority::Low),
        "medium" => Ok(Priority::Medium),
        "high" => Ok(Priority::High),
        _ => Err(format!(
            "Invalid priority '{s}': must be 'low', 'medium', or 'high'"
        )
        .into()),
    }
}

/// Parse an RFC 3339 timestamp string into (seconds, nanos) for Spanner's Timestamp.
fn parse_rfc3339_timestamp(s: &str) -> Result<(i64, i32), Box<dyn std::error::Error>> {
    let dt = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)?;
    Ok((dt.unix_timestamp(), dt.nanosecond() as i32))
}

/// Resolve timestamp bound parameters into a `TimestampBoundConfig`.
///
/// Returns `Ok(None)` for strong read (default, when no parameter is set).
/// Returns an error if more than one parameter is specified, or if values are invalid.
pub fn resolve_timestamp_bound(
    exact_staleness_secs: Option<i64>,
    max_staleness_secs: Option<i64>,
    read_timestamp: Option<&str>,
    min_read_timestamp: Option<&str>,
) -> Result<Option<TimestampBoundConfig>, Box<dyn std::error::Error>> {
    let count = [
        exact_staleness_secs.is_some(),
        max_staleness_secs.is_some(),
        read_timestamp.is_some(),
        min_read_timestamp.is_some(),
    ]
    .iter()
    .filter(|&&b| b)
    .count();

    if count > 1 {
        return Err(
            "At most one timestamp bound parameter can be specified \
             (exact_staleness_secs, max_staleness_secs, read_timestamp, min_read_timestamp)"
                .into(),
        );
    }

    if let Some(secs) = exact_staleness_secs {
        if secs < 0 {
            return Err(format!("exact_staleness_secs must be non-negative, got {secs}").into());
        }
        return Ok(Some(TimestampBoundConfig::ExactStaleness {
            secs: secs as u64,
        }));
    }

    if let Some(secs) = max_staleness_secs {
        if secs < 0 {
            return Err(format!("max_staleness_secs must be non-negative, got {secs}").into());
        }
        return Ok(Some(TimestampBoundConfig::MaxStaleness {
            secs: secs as u64,
        }));
    }

    if let Some(ts) = read_timestamp {
        let (seconds, nanos) = parse_rfc3339_timestamp(ts)?;
        return Ok(Some(TimestampBoundConfig::ReadTimestamp { seconds, nanos }));
    }

    if let Some(ts) = min_read_timestamp {
        let (seconds, nanos) = parse_rfc3339_timestamp(ts)?;
        return Ok(Some(TimestampBoundConfig::MinReadTimestamp { seconds, nanos }));
    }

    Ok(None)
}

/// Resolve the Spanner database resource path from named args and config options.
///
/// Resolution order (named arg overrides config for each component):
/// 1. project/instance/database (named arg ?? config) — if any component is resolved,
///    all three must be present or error
/// 2. database_path named arg
/// 3. spanner_database_path config
/// 4. error
pub fn resolve_database_path(bind: &BindInfo) -> Result<String, Box<dyn std::error::Error>> {
    let arg_project = get_named_string(bind, "project");
    let arg_instance = get_named_string(bind, "instance");
    let arg_database = get_named_string(bind, "database");

    let cfg_project = config::get_config_string(bind, "spanner_project");
    let cfg_instance = config::get_config_string(bind, "spanner_instance");
    let cfg_database = config::get_config_string(bind, "spanner_database");

    let project = arg_project.or(cfg_project);
    let instance = arg_instance.or(cfg_instance);
    let database = arg_database.or(cfg_database);

    if project.is_some() || instance.is_some() || database.is_some() {
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project := '...' or SET spanner_project = '...'")?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance := '...' or SET spanner_instance = '...'")?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database := '...' or SET spanner_database = '...'")?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = get_named_string(bind, "database_path") {
        return Ok(path);
    }

    if let Some(path) = config::get_config_string(bind, "spanner_database_path") {
        return Ok(path);
    }

    Err("No database specified. Use database_path := '...', or SET spanner_project/spanner_instance/spanner_database, or SET spanner_database_path".into())
}

/// Resolve the Spanner endpoint from named arg or config option.
pub fn resolve_endpoint(bind: &BindInfo) -> Option<String> {
    get_named_string(bind, "endpoint")
        .or_else(|| config::get_config_string(bind, "spanner_endpoint"))
}
