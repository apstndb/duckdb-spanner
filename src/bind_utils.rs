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

/// Get a named string parameter, returning None if absent, NULL, or empty string.
///
/// Empty strings are treated as absent so macro-generated `NULL` literals (`''`) do not
/// override config fallbacks. An explicit `project := ''` is therefore ignored.
pub fn get_named_string(bind: &BindInfo, name: &str) -> Option<String> {
    bind.get_named_parameter(name)
        .filter(|v| !v.is_null())
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty())
}

/// Get a named integer parameter, returning None if absent or NULL.
pub fn get_named_int64(bind: &BindInfo, name: &str) -> Option<i64> {
    bind.get_named_parameter(name)
        .filter(|v| !v.is_null())
        .map(|v| v.to_int64())
}

/// Get a named boolean parameter, returning the default if absent or NULL.
pub fn get_named_bool(bind: &BindInfo, name: &str, default: bool) -> bool {
    bind.get_named_parameter(name)
        .filter(|v| !v.is_null())
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
/// 1. project/instance/database named args, with config fallback for omitted components
/// 2. database_path named arg
/// 3. spanner_project/spanner_instance/spanner_database config
/// 4. spanner_database_path config
/// 5. error
pub fn resolve_database_path(bind: &BindInfo) -> Result<String, Box<dyn std::error::Error>> {
    let arg_project = get_named_string(bind, "project");
    let arg_instance = get_named_string(bind, "instance");
    let arg_database = get_named_string(bind, "database");
    let arg_database_path = get_named_string(bind, "database_path");

    if arg_database_path.is_some()
        && (arg_project.is_some() || arg_instance.is_some() || arg_database.is_some())
    {
        return Err(
            "cannot combine database_path with project, instance, or database named parameters"
                .into(),
        );
    }

    if arg_project.is_some() || arg_instance.is_some() || arg_database.is_some() {
        let project = arg_project.or_else(|| config::get_config_string(bind, "spanner_project"));
        let instance = arg_instance.or_else(|| config::get_config_string(bind, "spanner_instance"));
        let database = arg_database.or_else(|| config::get_config_string(bind, "spanner_database"));
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

    let cfg_project = config::get_config_string(bind, "spanner_project");
    let cfg_instance = config::get_config_string(bind, "spanner_instance");
    let cfg_database = config::get_config_string(bind, "spanner_database");

    if cfg_project.is_some() || cfg_instance.is_some() || cfg_database.is_some() {
        let p = cfg_project.ok_or(
            "project is required when instance or database is specified. \
             Use project := '...' or SET spanner_project = '...'")?;
        let i = cfg_instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance := '...' or SET spanner_instance = '...'")?;
        let d = cfg_database.ok_or(
            "database is required when project or instance is specified. \
             Use database := '...' or SET spanner_database = '...'")?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
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
