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
    params := NULL, endpoint := NULL, use_parallelism := NULL, parallelism_mode := NULL,
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

CREATE MACRO spanner_ddl(
    sql := NULL,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, admin_endpoint := NULL, statements := NULL
) AS TABLE
SELECT * FROM spanner_ddl_raw(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    admin_endpoint := admin_endpoint,
    statements := statements
);

CREATE MACRO spanner_ddl_async(
    sql := NULL,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, admin_endpoint := NULL, statements := NULL
) AS TABLE
SELECT * FROM spanner_ddl_async_raw(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    admin_endpoint := admin_endpoint,
    statements := statements
);

CREATE MACRO spanner_operations(
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, admin_endpoint := NULL, filter := NULL
) AS TABLE
SELECT * FROM spanner_operations_raw(
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    admin_endpoint := admin_endpoint,
    filter := filter
);
