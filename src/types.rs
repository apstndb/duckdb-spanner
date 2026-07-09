use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use google_cloud_spanner::model;
use google_cloud_spanner::types::{self as spanner_types, Type, TypeCode};

/// Convert a Spanner Type to a DuckDB LogicalTypeHandle.
pub fn spanner_type_to_logical(spanner_type: &Type) -> LogicalTypeHandle {
    match spanner_type.code() {
        TypeCode::Bool => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        TypeCode::Int64 => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        TypeCode::Float32 => LogicalTypeHandle::from(LogicalTypeId::Float),
        TypeCode::Float64 => LogicalTypeHandle::from(LogicalTypeId::Double),
        TypeCode::Numeric => LogicalTypeHandle::decimal(38, 9),
        TypeCode::String => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        TypeCode::Json => {
            let mut lt = LogicalTypeHandle::from(LogicalTypeId::Varchar);
            lt.set_alias("JSON");
            lt
        }
        TypeCode::Bytes | TypeCode::Proto => LogicalTypeHandle::from(LogicalTypeId::Blob),
        TypeCode::Date => LogicalTypeHandle::from(LogicalTypeId::Date),
        TypeCode::Timestamp => LogicalTypeHandle::from(LogicalTypeId::TimestampTZ),
        TypeCode::Enum => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        TypeCode::Uuid => LogicalTypeHandle::from(LogicalTypeId::Uuid),
        TypeCode::Interval => LogicalTypeHandle::from(LogicalTypeId::Interval),
        TypeCode::Array => {
            if let Some(elem_type) = spanner_type.array_element_type() {
                let child = spanner_type_to_logical(&elem_type);
                LogicalTypeHandle::list(&child)
            } else {
                LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar))
            }
        }
        TypeCode::Struct => {
            if let Some(struct_type) = spanner_type.struct_type() {
                let fields: Vec<(&str, LogicalTypeHandle)> = struct_type
                    .fields
                    .iter()
                    .map(|f| {
                        let lt = f
                            .r#type
                            .as_ref()
                            .map(|t| {
                                let t: Type = (**t).clone().into();
                                spanner_type_to_logical(&t)
                            })
                            .unwrap_or_else(|| LogicalTypeHandle::from(LogicalTypeId::Varchar));
                        (f.name.as_str(), lt)
                    })
                    .collect();
                LogicalTypeHandle::struct_type(&fields)
            } else {
                LogicalTypeHandle::from(LogicalTypeId::Varchar)
            }
        }
        other => {
            eprintln!(
                "[duckdb-spanner] Unsupported Spanner TypeCode {other:?}, falling back to VARCHAR"
            );
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
    }
}

/// Parse a PG dialect SPANNER_TYPE string from INFORMATION_SCHEMA.COLUMNS.
///
/// PG dialect returns DDL-compatible type names like "boolean", "bigint",
/// "character varying(256)", "timestamp with time zone", "bigint[]", etc.
pub fn parse_pg_spanner_type(s: &str) -> Type {
    let s = s.trim();

    if let Some(base) = s.strip_suffix("[]") {
        let elem = parse_pg_spanner_type(base);
        return spanner_types::array(elem);
    }

    let lower = s.to_lowercase();
    let base = if let Some(idx) = lower.find('(') {
        lower[..idx].trim()
    } else {
        lower.trim()
    };

    match base {
        "boolean" | "bool" => spanner_types::bool(),
        "bigint" | "int8" => spanner_types::int64(),
        "real" | "float4" => spanner_types::float32(),
        "double precision" | "float8" => spanner_types::float64(),
        "numeric" => spanner_types::pg_numeric(),
        "character varying" | "varchar" | "text" => spanner_types::string(),
        "bytea" => spanner_types::bytes(),
        "date" => spanner_types::date(),
        "timestamp with time zone" | "timestamptz" => spanner_types::timestamp(),
        "jsonb" => spanner_types::pg_jsonb(),
        "oid" => spanner_types::pg_oid(),
        "interval" => spanner_types::interval(),
        "uuid" => spanner_types::uuid(),
        other => {
            eprintln!("[duckdb-spanner] Unknown PG Spanner type '{other}', falling back to STRING");
            spanner_types::string()
        }
    }
}

/// Parse a SPANNER_TYPE string from INFORMATION_SCHEMA.COLUMNS.
///
/// Examples: "BOOL", "INT64", "STRING(MAX)", "ARRAY<INT64>",
/// "STRUCT<name STRING(MAX), age INT64>", "PROTO<my.pkg.Msg>".
pub fn parse_spanner_type(s: &str) -> Type {
    let s = s.trim();
    SpannerTypeParser::new(s).parse_type()
}

fn type_from_code(code: TypeCode) -> Type {
    let code: i32 = code.into();
    model::Type::new()
        .set_code(model::TypeCode::from(code))
        .into()
}

fn array_type(elem: Type) -> Type {
    let elem: model::Type = elem.into();
    model::Type::new()
        .set_code(model::TypeCode::Array)
        .set_array_element_type(elem)
        .into()
}

fn struct_type(fields: Vec<model::struct_type::Field>) -> Type {
    model::Type::new()
        .set_code(model::TypeCode::Struct)
        .set_struct_type(model::StructType::new().set_fields(fields))
        .into()
}

fn proto_type(code: model::TypeCode, fqn: &str) -> Type {
    model::Type::new()
        .set_code(code)
        .set_proto_type_fqn(fqn)
        .into()
}

struct SpannerTypeParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> SpannerTypeParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.pos).copied()
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn parse_ident(&mut self) -> &'a str {
        let start = self.pos;
        while self.pos < self.input.len() {
            let ch = self.input.as_bytes()[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        &self.input[start..self.pos]
    }

    fn parse_fqn(&mut self) -> &'a str {
        let start = self.pos;
        while self.pos < self.input.len() {
            let ch = self.input.as_bytes()[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'.' {
                self.pos += 1;
            } else {
                break;
            }
        }
        &self.input[start..self.pos]
    }

    fn parse_type(&mut self) -> Type {
        self.skip_whitespace();
        let ident = self.parse_ident().to_uppercase();

        let skip_length = |this: &mut Self| {
            this.skip_whitespace();
            if this.peek() == Some(b'(') {
                while this.pos < this.input.len() && this.input.as_bytes()[this.pos] != b')' {
                    this.pos += 1;
                }
                if this.pos < this.input.len() {
                    this.pos += 1;
                }
            }
        };

        match ident.as_str() {
            "BOOL" => type_from_code(TypeCode::Bool),
            "INT64" => type_from_code(TypeCode::Int64),
            "FLOAT32" => type_from_code(TypeCode::Float32),
            "FLOAT64" => type_from_code(TypeCode::Float64),
            "NUMERIC" => type_from_code(TypeCode::Numeric),
            "STRING" => {
                skip_length(self);
                type_from_code(TypeCode::String)
            }
            "BYTES" => {
                skip_length(self);
                type_from_code(TypeCode::Bytes)
            }
            "DATE" => type_from_code(TypeCode::Date),
            "TIMESTAMP" => type_from_code(TypeCode::Timestamp),
            "JSON" => type_from_code(TypeCode::Json),
            "INTERVAL" => type_from_code(TypeCode::Interval),
            "UUID" => type_from_code(TypeCode::Uuid),
            "ARRAY" => {
                self.skip_whitespace();
                self.consume(b'<');
                let elem = self.parse_type();
                self.skip_whitespace();
                self.consume(b'>');
                array_type(elem)
            }
            "STRUCT" => {
                self.skip_whitespace();
                self.consume(b'<');
                let fields = self.parse_struct_fields();
                self.skip_whitespace();
                self.consume(b'>');
                struct_type(fields)
            }
            "PROTO" => {
                self.skip_whitespace();
                self.consume(b'<');
                self.skip_whitespace();
                let fqn = self.parse_fqn().to_string();
                self.skip_whitespace();
                self.consume(b'>');
                proto_type(model::TypeCode::Proto, &fqn)
            }
            "ENUM" => {
                self.skip_whitespace();
                self.consume(b'<');
                self.skip_whitespace();
                let fqn = self.parse_fqn().to_string();
                self.skip_whitespace();
                self.consume(b'>');
                proto_type(model::TypeCode::Enum, &fqn)
            }
            other => {
                eprintln!(
                    "[duckdb-spanner] Unknown Spanner type '{other}', falling back to STRING"
                );
                type_from_code(TypeCode::String)
            }
        }
    }

    fn parse_struct_fields(&mut self) -> Vec<model::struct_type::Field> {
        let mut fields = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(b'>') || self.pos >= self.input.len() {
                break;
            }

            let name = self.parse_ident().to_string();
            if name.is_empty() {
                break;
            }

            self.skip_whitespace();
            let field_type = self.parse_type();
            let field_type: model::Type = field_type.into();

            fields.push(
                model::struct_type::Field::new()
                    .set_name(name)
                    .set_type(field_type),
            );

            self.skip_whitespace();
            self.consume(b',');
        }
        fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_type(t: &Type) -> model::Type {
        t.clone().into()
    }

    fn field_code(field: &model::struct_type::Field) -> TypeCode {
        let t: Type = (**field.r#type.as_ref().unwrap()).clone().into();
        t.code()
    }

    #[test]
    fn test_parse_scalar_types() {
        assert_eq!(parse_spanner_type("BOOL").code(), TypeCode::Bool);
        assert_eq!(parse_spanner_type("INT64").code(), TypeCode::Int64);
        assert_eq!(parse_spanner_type("FLOAT32").code(), TypeCode::Float32);
        assert_eq!(parse_spanner_type("FLOAT64").code(), TypeCode::Float64);
        assert_eq!(parse_spanner_type("NUMERIC").code(), TypeCode::Numeric);
        assert_eq!(parse_spanner_type("DATE").code(), TypeCode::Date);
        assert_eq!(parse_spanner_type("TIMESTAMP").code(), TypeCode::Timestamp);
        assert_eq!(parse_spanner_type("JSON").code(), TypeCode::Json);
        assert_eq!(parse_spanner_type("UUID").code(), TypeCode::Uuid);
        assert_eq!(parse_spanner_type("INTERVAL").code(), TypeCode::Interval);
    }

    #[test]
    fn test_parse_string_with_length() {
        assert_eq!(parse_spanner_type("STRING(MAX)").code(), TypeCode::String);
        assert_eq!(parse_spanner_type("STRING(100)").code(), TypeCode::String);
        assert_eq!(parse_spanner_type("BYTES(MAX)").code(), TypeCode::Bytes);
    }

    #[test]
    fn test_parse_array() {
        let t = parse_spanner_type("ARRAY<INT64>");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Int64);
    }

    #[test]
    fn test_parse_array_of_string() {
        let t = parse_spanner_type("ARRAY<STRING(MAX)>");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::String);
    }

    #[test]
    fn test_parse_struct() {
        let t = parse_spanner_type("STRUCT<name STRING(MAX), age INT64>");
        assert_eq!(t.code(), TypeCode::Struct);
        let st = t.struct_type().unwrap();
        assert_eq!(st.fields.len(), 2);
        assert_eq!(st.fields[0].name, "name");
        assert_eq!(field_code(&st.fields[0]), TypeCode::String);
        assert_eq!(st.fields[1].name, "age");
        assert_eq!(field_code(&st.fields[1]), TypeCode::Int64);
    }

    #[test]
    fn test_parse_proto_enum() {
        let t = parse_spanner_type("PROTO<my.package.Message>");
        assert_eq!(t.code(), TypeCode::Proto);
        assert_eq!(raw_type(&t).proto_type_fqn, "my.package.Message");

        let t = parse_spanner_type("ENUM<my.package.MyEnum>");
        assert_eq!(t.code(), TypeCode::Enum);
        assert_eq!(raw_type(&t).proto_type_fqn, "my.package.MyEnum");
    }

    #[test]
    fn test_parse_pg_scalar_types() {
        assert_eq!(parse_pg_spanner_type("boolean").code(), TypeCode::Bool);
        assert_eq!(parse_pg_spanner_type("bool").code(), TypeCode::Bool);
        assert_eq!(parse_pg_spanner_type("bigint").code(), TypeCode::Int64);
        assert_eq!(parse_pg_spanner_type("int8").code(), TypeCode::Int64);
        assert_eq!(parse_pg_spanner_type("real").code(), TypeCode::Float32);
        assert_eq!(parse_pg_spanner_type("float4").code(), TypeCode::Float32);
        assert_eq!(
            parse_pg_spanner_type("double precision").code(),
            TypeCode::Float64
        );
        assert_eq!(parse_pg_spanner_type("float8").code(), TypeCode::Float64);
        assert_eq!(parse_pg_spanner_type("numeric").code(), TypeCode::Numeric);
        assert_eq!(parse_pg_spanner_type("date").code(), TypeCode::Date);
        assert_eq!(
            parse_pg_spanner_type("timestamp with time zone").code(),
            TypeCode::Timestamp
        );
        assert_eq!(
            parse_pg_spanner_type("timestamptz").code(),
            TypeCode::Timestamp
        );
        assert_eq!(parse_pg_spanner_type("jsonb").code(), TypeCode::Json);
        assert_eq!(parse_pg_spanner_type("oid").code(), TypeCode::Int64);
        assert_eq!(parse_pg_spanner_type("interval").code(), TypeCode::Interval);
        assert_eq!(parse_pg_spanner_type("uuid").code(), TypeCode::Uuid);
    }

    #[test]
    fn test_parse_pg_type_annotations() {
        let numeric = raw_type(&parse_pg_spanner_type("numeric"));
        assert_eq!(
            numeric.type_annotation,
            model::TypeAnnotationCode::PgNumeric
        );

        let jsonb = raw_type(&parse_pg_spanner_type("jsonb"));
        assert_eq!(jsonb.type_annotation, model::TypeAnnotationCode::PgJsonb);

        let oid = raw_type(&parse_pg_spanner_type("oid"));
        assert_eq!(oid.type_annotation, model::TypeAnnotationCode::PgOid);

        let boolean = raw_type(&parse_pg_spanner_type("boolean"));
        assert_eq!(
            boolean.type_annotation,
            model::TypeAnnotationCode::Unspecified
        );
    }

    #[test]
    fn test_parse_pg_string_types() {
        assert_eq!(
            parse_pg_spanner_type("character varying").code(),
            TypeCode::String
        );
        assert_eq!(
            parse_pg_spanner_type("character varying(256)").code(),
            TypeCode::String
        );
        assert_eq!(parse_pg_spanner_type("varchar").code(), TypeCode::String);
        assert_eq!(parse_pg_spanner_type("text").code(), TypeCode::String);
    }

    #[test]
    fn test_parse_pg_bytes() {
        assert_eq!(parse_pg_spanner_type("bytea").code(), TypeCode::Bytes);
    }

    #[test]
    fn test_parse_pg_array_types() {
        let t = parse_pg_spanner_type("bigint[]");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Int64);

        let t = parse_pg_spanner_type("character varying[]");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::String);

        let t = parse_pg_spanner_type("boolean[]");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Bool);

        let t = parse_pg_spanner_type("double precision[]");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Float64);

        let t = parse_pg_spanner_type("jsonb[]");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Json);

        let t = parse_pg_spanner_type("numeric[]");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Numeric);

        let t = parse_pg_spanner_type("oid[]");
        assert_eq!(t.code(), TypeCode::Array);
        let elem = t.array_element_type().unwrap();
        assert_eq!(elem.code(), TypeCode::Int64);
        assert_eq!(
            raw_type(&elem).type_annotation,
            model::TypeAnnotationCode::PgOid
        );

        let t = parse_pg_spanner_type("interval[]");
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Interval);
    }
}
