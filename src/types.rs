use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use google_cloud_googleapis::spanner::v1::{StructType, Type, TypeCode};

/// Convert a Spanner Type proto to a DuckDB LogicalTypeHandle.
pub fn spanner_type_to_logical(spanner_type: &Type) -> LogicalTypeHandle {
    let type_code = TypeCode::try_from(spanner_type.code).unwrap_or(TypeCode::Unspecified);

    match type_code {
        TypeCode::Bool => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        TypeCode::Int64 => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        TypeCode::Float32 => LogicalTypeHandle::from(LogicalTypeId::Float),
        TypeCode::Float64 => LogicalTypeHandle::from(LogicalTypeId::Double),
        TypeCode::Numeric => LogicalTypeHandle::decimal(38, 9),
        TypeCode::String => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        TypeCode::Json => {
            let lt = LogicalTypeHandle::from(LogicalTypeId::Varchar);
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
            if let Some(elem_type) = &spanner_type.array_element_type {
                let child = spanner_type_to_logical(elem_type);
                LogicalTypeHandle::list(&child)
            } else {
                // Fallback: untyped array → LIST(VARCHAR)
                LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar))
            }
        }
        TypeCode::Struct => {
            if let Some(struct_type) = &spanner_type.struct_type {
                let fields: Vec<(&str, LogicalTypeHandle)> = struct_type
                    .fields
                    .iter()
                    .map(|f| {
                        let lt = f
                            .r#type
                            .as_ref()
                            .map(spanner_type_to_logical)
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
            eprintln!("[duckdb-spanner] Unsupported Spanner TypeCode {other:?}, falling back to VARCHAR");
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
    }
}

/// Parse a SPANNER_TYPE string from INFORMATION_SCHEMA.COLUMNS into a Type proto.
///
/// Examples: "BOOL", "INT64", "STRING(MAX)", "ARRAY<INT64>",
///           "STRUCT<name STRING(MAX), age INT64>", "PROTO<my.pkg.Msg>"
pub fn parse_spanner_type(s: &str) -> Type {
    let s = s.trim();
    SpannerTypeParser::new(s).parse_type()
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
        while self.pos < self.input.len()
            && self.input.as_bytes()[self.pos].is_ascii_whitespace()
        {
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

    // Parse a fully qualified name like "my.package.MessageName"
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

        // Skip optional length specifier like (MAX) or (100)
        let skip_length = |this: &mut Self| {
            this.skip_whitespace();
            if this.peek() == Some(b'(') {
                while this.pos < this.input.len() && this.input.as_bytes()[this.pos] != b')' {
                    this.pos += 1;
                }
                if this.pos < this.input.len() {
                    this.pos += 1; // skip ')'
                }
            }
        };

        match ident.as_str() {
            "BOOL" => Type { code: TypeCode::Bool as i32, ..Default::default() },
            "INT64" => Type { code: TypeCode::Int64 as i32, ..Default::default() },
            "FLOAT32" => Type { code: TypeCode::Float32 as i32, ..Default::default() },
            "FLOAT64" => Type { code: TypeCode::Float64 as i32, ..Default::default() },
            "NUMERIC" => Type { code: TypeCode::Numeric as i32, ..Default::default() },
            "STRING" => {
                skip_length(self);
                Type { code: TypeCode::String as i32, ..Default::default() }
            }
            "BYTES" => {
                skip_length(self);
                Type { code: TypeCode::Bytes as i32, ..Default::default() }
            }
            "DATE" => Type { code: TypeCode::Date as i32, ..Default::default() },
            "TIMESTAMP" => Type { code: TypeCode::Timestamp as i32, ..Default::default() },
            "JSON" => Type { code: TypeCode::Json as i32, ..Default::default() },
            "INTERVAL" => Type { code: TypeCode::Interval as i32, ..Default::default() },
            "UUID" => Type { code: TypeCode::Uuid as i32, ..Default::default() },
            "ARRAY" => {
                self.skip_whitespace();
                self.consume(b'<');
                let elem = self.parse_type();
                self.skip_whitespace();
                self.consume(b'>');
                Type {
                    code: TypeCode::Array as i32,
                    array_element_type: Some(Box::new(elem)),
                    ..Default::default()
                }
            }
            "STRUCT" => {
                self.skip_whitespace();
                self.consume(b'<');
                let fields = self.parse_struct_fields();
                self.skip_whitespace();
                self.consume(b'>');
                Type {
                    code: TypeCode::Struct as i32,
                    struct_type: Some(StructType { fields }),
                    ..Default::default()
                }
            }
            "PROTO" => {
                self.skip_whitespace();
                self.consume(b'<');
                self.skip_whitespace();
                let fqn = self.parse_fqn();
                self.skip_whitespace();
                self.consume(b'>');
                Type {
                    code: TypeCode::Proto as i32,
                    proto_type_fqn: fqn.to_string(),
                    ..Default::default()
                }
            }
            "ENUM" => {
                self.skip_whitespace();
                self.consume(b'<');
                self.skip_whitespace();
                let fqn = self.parse_fqn();
                self.skip_whitespace();
                self.consume(b'>');
                Type {
                    code: TypeCode::Enum as i32,
                    proto_type_fqn: fqn.to_string(),
                    ..Default::default()
                }
            }
            other => {
                eprintln!("[duckdb-spanner] Unknown Spanner type '{other}', falling back to STRING");
                Type { code: TypeCode::String as i32, ..Default::default() }
            }
        }
    }

    fn parse_struct_fields(&mut self) -> Vec<google_cloud_googleapis::spanner::v1::struct_type::Field> {
        let mut fields = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(b'>') || self.pos >= self.input.len() {
                break;
            }

            // Parse field name
            let name = self.parse_ident().to_string();
            if name.is_empty() {
                break;
            }

            // Parse field type
            self.skip_whitespace();
            let field_type = self.parse_type();

            fields.push(google_cloud_googleapis::spanner::v1::struct_type::Field {
                name,
                r#type: Some(field_type),
            });

            self.skip_whitespace();
            // Consume comma separator if present
            self.consume(b',');
        }
        fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_scalar_types() {
        assert_eq!(parse_spanner_type("BOOL").code, TypeCode::Bool as i32);
        assert_eq!(parse_spanner_type("INT64").code, TypeCode::Int64 as i32);
        assert_eq!(parse_spanner_type("FLOAT32").code, TypeCode::Float32 as i32);
        assert_eq!(parse_spanner_type("FLOAT64").code, TypeCode::Float64 as i32);
        assert_eq!(parse_spanner_type("NUMERIC").code, TypeCode::Numeric as i32);
        assert_eq!(parse_spanner_type("DATE").code, TypeCode::Date as i32);
        assert_eq!(parse_spanner_type("TIMESTAMP").code, TypeCode::Timestamp as i32);
        assert_eq!(parse_spanner_type("JSON").code, TypeCode::Json as i32);
        assert_eq!(parse_spanner_type("UUID").code, TypeCode::Uuid as i32);
        assert_eq!(parse_spanner_type("INTERVAL").code, TypeCode::Interval as i32);
    }

    #[test]
    fn test_parse_string_with_length() {
        assert_eq!(parse_spanner_type("STRING(MAX)").code, TypeCode::String as i32);
        assert_eq!(parse_spanner_type("STRING(100)").code, TypeCode::String as i32);
        assert_eq!(parse_spanner_type("BYTES(MAX)").code, TypeCode::Bytes as i32);
    }

    #[test]
    fn test_parse_array() {
        let t = parse_spanner_type("ARRAY<INT64>");
        assert_eq!(t.code, TypeCode::Array as i32);
        let elem = t.array_element_type.unwrap();
        assert_eq!(elem.code, TypeCode::Int64 as i32);
    }

    #[test]
    fn test_parse_array_of_string() {
        let t = parse_spanner_type("ARRAY<STRING(MAX)>");
        assert_eq!(t.code, TypeCode::Array as i32);
        let elem = t.array_element_type.unwrap();
        assert_eq!(elem.code, TypeCode::String as i32);
    }

    #[test]
    fn test_parse_struct() {
        let t = parse_spanner_type("STRUCT<name STRING(MAX), age INT64>");
        assert_eq!(t.code, TypeCode::Struct as i32);
        let st = t.struct_type.unwrap();
        assert_eq!(st.fields.len(), 2);
        assert_eq!(st.fields[0].name, "name");
        assert_eq!(st.fields[0].r#type.as_ref().unwrap().code, TypeCode::String as i32);
        assert_eq!(st.fields[1].name, "age");
        assert_eq!(st.fields[1].r#type.as_ref().unwrap().code, TypeCode::Int64 as i32);
    }

    #[test]
    fn test_parse_proto_enum() {
        let t = parse_spanner_type("PROTO<my.package.Message>");
        assert_eq!(t.code, TypeCode::Proto as i32);
        assert_eq!(t.proto_type_fqn, "my.package.Message");

        let t = parse_spanner_type("ENUM<my.package.MyEnum>");
        assert_eq!(t.code, TypeCode::Enum as i32);
        assert_eq!(t.proto_type_fqn, "my.package.MyEnum");
    }
}
