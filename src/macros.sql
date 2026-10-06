-- Table macro wrappers registered by the extension at load time.
--
-- User-facing API wrapping the _raw VTab functions.
-- Benefits over calling _raw directly:
--   - params accepts a STRUCT directly (auto-converted to JSON via spanner_params)
--   - Named parameters default to NULL (VTab treats NULL as absent)
--
-- Scalar helpers (spanner_value, spanner_typed, spanner_params, interval_to_iso8601)
-- are registered as native VScalar functions in Rust (see src/scalars.rs).

CREATE MACRO spanner_query(
    sql,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    params := NULL, endpoint := NULL, endpoint_mode := NULL,
    use_parallelism := NULL, parallelism_mode := NULL,
    use_data_boost := NULL, max_parallelism := NULL,
    exact_staleness_secs := NULL, max_staleness_secs := NULL,
    read_timestamp := NULL, min_read_timestamp := NULL,
    priority := NULL
) AS TABLE
SELECT * FROM spanner_query_raw(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    endpoint_mode := endpoint_mode,
    params := spanner_params(params),
    use_parallelism := use_parallelism,
    parallelism_mode := parallelism_mode,
    use_data_boost := use_data_boost,
    max_parallelism := max_parallelism,
    exact_staleness_secs := exact_staleness_secs,
    max_staleness_secs := max_staleness_secs,
    read_timestamp := read_timestamp,
    min_read_timestamp := min_read_timestamp,
    priority := priority
);

-- Explicit representation change: DuckDB casts each native result column.
-- JSON null and SQL NULL become indistinguishable VARIANT nulls. Ordinary
-- strings remain strings; no envelope/JSON sniffing occurs. Keep these casts
-- above the native reader so its type checks, streaming and options are reused.
CREATE MACRO spanner_query_variant(
    sql,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    params := NULL, endpoint := NULL, endpoint_mode := NULL,
    use_parallelism := NULL, parallelism_mode := NULL,
    use_data_boost := NULL, max_parallelism := NULL,
    exact_staleness_secs := NULL, max_staleness_secs := NULL,
    read_timestamp := NULL, min_read_timestamp := NULL,
    priority := NULL
) AS TABLE
SELECT COLUMNS(*)::VARIANT FROM spanner_query(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    endpoint_mode := endpoint_mode,
    params := params,
    use_parallelism := use_parallelism,
    parallelism_mode := parallelism_mode,
    use_data_boost := use_data_boost,
    max_parallelism := max_parallelism,
    exact_staleness_secs := exact_staleness_secs,
    max_staleness_secs := max_staleness_secs,
    read_timestamp := read_timestamp,
    min_read_timestamp := min_read_timestamp,
    priority := priority
);

CREATE MACRO spanner_scan_variant(
    table_name,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, endpoint_mode := NULL, dialect := NULL,
    use_parallelism := NULL, parallelism_mode := NULL,
    use_data_boost := NULL, max_parallelism := NULL, index := NULL,
    exact_staleness_secs := NULL, max_staleness_secs := NULL,
    read_timestamp := NULL, min_read_timestamp := NULL,
    priority := NULL
) AS TABLE
SELECT COLUMNS(*)::VARIANT FROM spanner_scan(
    table_name,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    endpoint_mode := endpoint_mode,
    dialect := dialect,
    use_parallelism := use_parallelism,
    parallelism_mode := parallelism_mode,
    use_data_boost := use_data_boost,
    max_parallelism := max_parallelism,
    index := index,
    exact_staleness_secs := exact_staleness_secs,
    max_staleness_secs := max_staleness_secs,
    read_timestamp := read_timestamp,
    min_read_timestamp := min_read_timestamp,
    priority := priority
);

CREATE MACRO spanner_ddl(
    sql := NULL,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, endpoint_mode := NULL, admin_endpoint := NULL, statements := NULL
) AS TABLE
SELECT * FROM spanner_ddl_raw(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    endpoint_mode := endpoint_mode,
    admin_endpoint := admin_endpoint,
    statements := statements
);

CREATE MACRO spanner_ddl_async(
    sql := NULL,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, endpoint_mode := NULL, admin_endpoint := NULL, statements := NULL
) AS TABLE
SELECT * FROM spanner_ddl_async_raw(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    endpoint_mode := endpoint_mode,
    admin_endpoint := admin_endpoint,
    statements := statements
);

CREATE MACRO spanner_operations(
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, endpoint_mode := NULL, admin_endpoint := NULL, filter := NULL
) AS TABLE
SELECT * FROM spanner_operations_raw(
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    endpoint_mode := endpoint_mode,
    admin_endpoint := admin_endpoint,
    filter := filter
);
