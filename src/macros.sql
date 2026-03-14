-- Macros registered by the extension at load time.
--
-- Helper macros for constructing Spanner query parameter JSON.

-- spanner_params(s): convert a STRUCT of parameter values to a JSON string.
-- Each field can be a plain value (type inferred), spanner_value() (auto-typed),
-- or spanner_typed() (explicit type).
--
-- Examples:
--   spanner_params({'age': spanner_value(25), 'name': 'Alice'})
--   → '{"age":{"value":25,"type":"INT64"},"name":"Alice"}'
CREATE MACRO spanner_params(s) AS CAST(to_json(s) AS VARCHAR);

-- spanner_typed(val, typ): wrap a value with an explicit Spanner type name.
--   spanner_typed(NULL, 'INT64') → {"value":null,"type":"INT64"}
CREATE MACRO spanner_typed(val, typ) AS json_object('value', val, 'type', typ);

-- interval_to_iso8601(i): convert a DuckDB INTERVAL to ISO 8601 duration string.
--   INTERVAL '1 day'           → 'P1D'
--   INTERVAL '1 year 3 months' → 'P1Y3M'
--   INTERVAL '2 hours 30 min'  → 'PT2H30M'
CREATE MACRO interval_to_iso8601(i) AS
  CASE WHEN datepart('year', i) = 0 AND datepart('month', i) = 0 AND datepart('day', i) = 0
            AND datepart('hour', i) = 0 AND datepart('minute', i) = 0
            AND datepart('microsecond', i) = 0
    THEN 'PT0S'
    ELSE
      'P' ||
      CASE WHEN datepart('year', i) != 0 THEN CAST(datepart('year', i) AS VARCHAR) || 'Y' ELSE '' END ||
      CASE WHEN datepart('month', i) != 0 THEN CAST(datepart('month', i) AS VARCHAR) || 'M' ELSE '' END ||
      CASE WHEN datepart('day', i) != 0 THEN CAST(datepart('day', i) AS VARCHAR) || 'D' ELSE '' END ||
      CASE WHEN datepart('hour', i) != 0 OR datepart('minute', i) != 0
                OR datepart('second', i) != 0 OR datepart('microsecond', i) % 1000000 != 0
        THEN 'T' ||
          CASE WHEN datepart('hour', i) != 0 THEN CAST(datepart('hour', i) AS VARCHAR) || 'H' ELSE '' END ||
          CASE WHEN datepart('minute', i) != 0 THEN CAST(datepart('minute', i) AS VARCHAR) || 'M' ELSE '' END ||
          CASE WHEN datepart('second', i) != 0 OR datepart('microsecond', i) % 1000000 != 0
            THEN CAST(datepart('second', i) AS VARCHAR) ||
              CASE WHEN datepart('microsecond', i) % 1000000 != 0
                THEN '.' || LPAD(CAST(datepart('microsecond', i) % 1000000 AS VARCHAR), 6, '0')
                ELSE '' END || 'S'
            ELSE '' END
        ELSE '' END
  END;

-- _spanner_scalar_type_name(t): map a DuckDB scalar type name to the Spanner type name.
-- Returns NULL for unrecognized types (including DECIMAL, which needs LIKE matching).
-- Internal helper; use _spanner_type_name() which also handles arrays.
CREATE MACRO _spanner_scalar_type_name(t) AS
  CASE t
    WHEN 'BOOLEAN' THEN 'BOOL'
    WHEN 'TINYINT' THEN 'INT64'
    WHEN 'SMALLINT' THEN 'INT64'
    WHEN 'INTEGER' THEN 'INT64'
    WHEN 'BIGINT' THEN 'INT64'
    WHEN 'UTINYINT' THEN 'INT64'
    WHEN 'USMALLINT' THEN 'INT64'
    WHEN 'UINTEGER' THEN 'INT64'
    WHEN 'UBIGINT' THEN 'NUMERIC'
    WHEN 'HUGEINT' THEN 'NUMERIC'
    WHEN 'UHUGEINT' THEN 'NUMERIC'
    WHEN 'FLOAT' THEN 'FLOAT32'
    WHEN 'DOUBLE' THEN 'FLOAT64'
    WHEN 'VARCHAR' THEN 'STRING'
    WHEN 'BLOB' THEN 'BYTES'
    WHEN 'DATE' THEN 'DATE'
    WHEN 'TIMESTAMP' THEN 'TIMESTAMP'
    WHEN 'TIMESTAMP WITH TIME ZONE' THEN 'TIMESTAMP'
    WHEN 'UUID' THEN 'UUID'
    WHEN 'INTERVAL' THEN 'INTERVAL'
    WHEN 'JSON' THEN 'JSON'
    WHEN 'TIME' THEN 'STRING'
    WHEN 'BIT' THEN 'BYTES'
    ELSE NULL
  END;

-- _spanner_type_name(t): map a DuckDB type name (scalar or array) to the Spanner type name.
-- Returns NULL for unrecognized types.
-- Internal helper used by spanner_value(); not intended for direct use.
CREATE MACRO _spanner_type_name(t) AS
  CASE
    WHEN t LIKE 'DECIMAL%[]' THEN 'ARRAY<NUMERIC>'
    WHEN t LIKE 'DECIMAL%' THEN 'NUMERIC'
    WHEN t LIKE '%[]' THEN 'ARRAY<' || _spanner_scalar_type_name(replace(t, '[]', '')) || '>'
    ELSE _spanner_scalar_type_name(t)
  END;

-- spanner_value(val): auto-detect Spanner type from DuckDB type via typeof().
-- Supports scalar types and arrays.
--
-- Format conversions applied where needed:
--   BLOB      → base64 encoded string
--   TIMESTAMP → RFC 3339 format
--   INTERVAL  → ISO 8601 duration
--   HUGEINT/DECIMAL → VARCHAR (preserves precision for NUMERIC)
--   ARRAY<T>  → element-wise conversion matching the scalar rules
--
-- Examples:
--   spanner_value(42::BIGINT)            → {"value":42,"type":"INT64"}
--   spanner_value('2024-01-15'::DATE)    → {"value":"2024-01-15","type":"DATE"}
--   spanner_value('\xDEAD'::BLOB)        → {"value":"3kFE","type":"BYTES"}
--   spanner_value(NULL::BIGINT)          → {"value":null,"type":"INT64"}
--   spanner_value([1, 2, 3])             → {"value":[1,2,3],"type":"ARRAY<INT64>"}
CREATE MACRO spanner_value(val) AS
  CASE
    -- Scalar types needing value conversion (type name still from _spanner_type_name)
    WHEN typeof(val) IN ('BLOB', 'BIT')
      THEN json_object('value', base64(TRY_CAST(val AS BLOB)), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) = 'TIMESTAMP'
      THEN json_object('value', strftime(TRY_CAST(val AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S.%fZ'), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) = 'TIMESTAMP WITH TIME ZONE'
      THEN json_object('value', strftime(TRY_CAST(val AS TIMESTAMPTZ) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ'), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) = 'INTERVAL'
      THEN json_object('value', interval_to_iso8601(TRY_CAST(val AS INTERVAL)), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) IN ('UBIGINT', 'HUGEINT', 'UHUGEINT')
      THEN json_object('value', CAST(val AS VARCHAR), 'type', _spanner_type_name(typeof(val)))
    -- Array types needing element-wise value conversion.
    -- TRY_CAST forces the target list type so list_transform binds correctly
    -- even when val is a scalar (TRY_CAST returns NULL, branch is never reached).
    WHEN typeof(val) IN ('UBIGINT[]', 'HUGEINT[]', 'UHUGEINT[]')
      THEN json_object('value', TRY_CAST(val AS VARCHAR[]), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) = 'BLOB[]'
      THEN json_object('value', list_transform(TRY_CAST(val AS BLOB[]), x -> base64(x)), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) = 'TIMESTAMP[]'
      THEN json_object('value', list_transform(TRY_CAST(val AS TIMESTAMP[]), x -> strftime(x, '%Y-%m-%dT%H:%M:%S.%fZ')), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) = 'TIMESTAMP WITH TIME ZONE[]'
      THEN json_object('value', list_transform(TRY_CAST(val AS TIMESTAMPTZ[]), x -> strftime(x AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ')), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) = 'INTERVAL[]'
      THEN json_object('value', list_transform(TRY_CAST(val AS INTERVAL[]), x -> interval_to_iso8601(x)), 'type', _spanner_type_name(typeof(val)))
    -- DECIMAL types need LIKE matching and VARCHAR conversion
    WHEN typeof(val) LIKE 'DECIMAL%[]'
      THEN json_object('value', TRY_CAST(val AS VARCHAR[]), 'type', _spanner_type_name(typeof(val)))
    WHEN typeof(val) LIKE 'DECIMAL%'
      THEN json_object('value', CAST(val AS VARCHAR), 'type', _spanner_type_name(typeof(val)))
    -- All other types: no value conversion needed
    WHEN _spanner_type_name(typeof(val)) IS NOT NULL
      THEN json_object('value', val, 'type', _spanner_type_name(typeof(val)))
    -- Fallback: pass value and type name as-is
    ELSE json_object('value', val, 'type', typeof(val))
  END;

-- ─── Table macro wrappers ──────────────────────────────────────────────────
--
-- User-facing API wrapping the _raw VTab functions.
-- Benefits over calling _raw directly:
--   - params accepts a STRUCT directly (auto-converted to JSON via spanner_params)
--   - Named parameters default to NULL (VTab treats NULL as absent)

CREATE MACRO spanner_query(
    sql,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    params := NULL, endpoint := NULL, use_parallelism := NULL,
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
    use_data_boost := use_data_boost,
    max_parallelism := max_parallelism,
    exact_staleness_secs := exact_staleness_secs,
    max_staleness_secs := max_staleness_secs,
    read_timestamp := read_timestamp,
    min_read_timestamp := min_read_timestamp,
    priority := priority
);

CREATE MACRO spanner_ddl(
    sql,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL
) AS TABLE
SELECT * FROM spanner_ddl_raw(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint
);

CREATE MACRO spanner_ddl_async(
    sql,
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL
) AS TABLE
SELECT * FROM spanner_ddl_async_raw(
    sql,
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint
);

CREATE MACRO spanner_operations(
    database_path := NULL, project := NULL, instance := NULL, database := NULL,
    endpoint := NULL, filter := NULL
) AS TABLE
SELECT * FROM spanner_operations_raw(
    database_path := database_path,
    project := project,
    instance := instance,
    database := database,
    endpoint := endpoint,
    filter := filter
);

