use std::fmt;
use std::sync::LazyLock;

use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use google_cloud_spanner::model;
use google_cloud_spanner::types::{self as spanner_types, Type, TypeCode};

use crate::error::SpannerError;

static PG_NUMERIC_TYPE: LazyLock<Type> = LazyLock::new(spanner_types::pg_numeric);

const MAX_TYPE_INPUT_BYTES: usize = 64 * 1024;
const MAX_TYPE_NESTING_DEPTH: usize = 64;
const MAX_STRUCT_FIELDS: usize = 1024;
const MAX_ERROR_INPUT_PREVIEW_BYTES: usize = 256;

const GOOGLESQL_STRING_MAX_LENGTH: u64 = 2_621_440;
const GOOGLESQL_BYTES_MAX_LENGTH: u64 = 10_485_760;
const PG_VARCHAR_MAX_LENGTH: u64 = 2_621_440;
const PG_NUMERIC_MAX_PRECISION: u64 = 147_455;
const PG_NUMERIC_MAX_SCALE: u64 = 16_383;
const PG_DATETIME_MAX_PRECISION: u64 = 6;

/// GoogleSQL reserved keywords from the Cloud Spanner lexical reference:
/// <https://cloud.google.com/spanner/docs/reference/standard-sql/lexical#reserved_keywords>.
///
/// Keywords are case-insensitive and cannot be used as unquoted identifiers.
const GOOGLESQL_RESERVED_KEYWORDS: &[&str] = &[
    "ALL",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASSERT_ROWS_MODIFIED",
    "AT",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COLLATE",
    "CONTAINS",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "DEFAULT",
    "DEFINE",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "ENUM",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDE",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FOLLOWING",
    "FOR",
    "FROM",
    "FULL",
    "GRAPH_TABLE",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HASH",
    "HAVING",
    "IF",
    "IGNORE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOOKUP",
    "MERGE",
    "NATURAL",
    "NEW",
    "NO",
    "NOT",
    "NULL",
    "NULLS",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRECEDING",
    "PROTO",
    "RANGE",
    "RECURSIVE",
    "RESPECT",
    "RIGHT",
    "ROLLUP",
    "ROWS",
    "SELECT",
    "SET",
    "SOME",
    "STRUCT",
    "TABLESAMPLE",
    "THEN",
    "TO",
    "TREAT",
    "TRUE",
    "UNBOUNDED",
    "UNION",
    "UNNEST",
    "USING",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "WITHIN",
];

/// Convert a Spanner Type to a DuckDB LogicalTypeHandle.
pub fn spanner_type_to_logical(spanner_type: &Type) -> Result<LogicalTypeHandle, SpannerError> {
    let logical_type = match spanner_type.code() {
        TypeCode::Bool => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        TypeCode::Int64 => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        TypeCode::Float32 => LogicalTypeHandle::from(LogicalTypeId::Float),
        TypeCode::Float64 => LogicalTypeHandle::from(LogicalTypeId::Double),
        // PostgreSQL `numeric` has arbitrary precision and admits `NaN`, while
        // DuckDB DECIMAL is limited to a fixed precision and scale. Preserve
        // the wire string rather than truncating, overflowing, or rejecting it.
        TypeCode::Numeric if is_pg_numeric(spanner_type) => {
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
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
            let element_type = spanner_type.array_element_type().ok_or_else(|| {
                SpannerError::Other(
                    "ARRAY result type is missing element type metadata".to_string(),
                )
            })?;
            let child = spanner_type_to_logical(&element_type).map_err(|error| {
                SpannerError::Other(format!(
                    "Invalid ARRAY element result type metadata: {error}"
                ))
            })?;
            LogicalTypeHandle::list(&child)
        }
        TypeCode::Struct => {
            let struct_type = spanner_type.struct_type().ok_or_else(|| {
                SpannerError::Other("STRUCT result type is missing struct metadata".to_string())
            })?;
            let mut fields = Vec::with_capacity(struct_type.fields.len());
            for (index, field) in struct_type.fields.iter().enumerate() {
                let field_type = field.r#type.as_ref().ok_or_else(|| {
                    SpannerError::Other(format!(
                        "STRUCT result type field {index} ({:?}) is missing type metadata",
                        field.name
                    ))
                })?;
                let field_type: Type = (**field_type).clone().into();
                let logical_type = spanner_type_to_logical(&field_type).map_err(|error| {
                    SpannerError::Other(format!(
                        "Invalid STRUCT result type field {index} ({:?}) metadata: {error}",
                        field.name
                    ))
                })?;
                fields.push((field.name.as_str(), logical_type));
            }
            LogicalTypeHandle::struct_type(&fields)
        }
        TypeCode::Unspecified => {
            return Err(SpannerError::Other(
                "Spanner result type is unspecified".to_string(),
            ));
        }
        TypeCode::Unknown(code) => {
            return Err(SpannerError::Other(format!(
                "Unsupported Spanner result type code {code}"
            )));
        }
        other => {
            return Err(SpannerError::Other(format!(
                "Unsupported Spanner result type code {other:?}"
            )));
        }
    };

    Ok(logical_type)
}

/// Whether a NUMERIC Type uses PostgreSQL semantics.
///
/// The official client exposes the dialect distinction through the generated
/// type annotation, while both dialects share `TypeCode::Numeric`.
pub fn is_pg_numeric(spanner_type: &Type) -> bool {
    spanner_type == &*PG_NUMERIC_TYPE
}

/// A syntax error returned while parsing an INFORMATION_SCHEMA `SPANNER_TYPE`
/// string or an explicit parameter type.
///
/// `type_text` is the unmodified original text for inputs within the parser's
/// size limit. Oversized inputs use a bounded UTF-8-safe preview. `position` is
/// always a byte offset into the original input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeParseError {
    pub dialect: &'static str,
    pub type_text: String,
    pub position: usize,
    pub context: String,
}

impl fmt::Display for TypeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid {} type {:?} at byte {}: {}",
            self.dialect, self.type_text, self.position, self.context
        )
    }
}

impl std::error::Error for TypeParseError {}

/// Parse a PG dialect SPANNER_TYPE string from INFORMATION_SCHEMA.COLUMNS.
///
/// PG dialect returns DDL-compatible type names like "boolean", "bigint",
/// "character varying(256)", "timestamp with time zone", "bigint[]", etc.
/// Supported type modifiers are validated and deliberately ignored after
/// parsing; unknown type names and trailing text are errors.
pub fn parse_pg_spanner_type(input: &str) -> Result<Type, TypeParseError> {
    validate_type_input_size(input, "PostgreSQL Spanner")?;
    PgTypeParser::new(input).parse()
}

/// Parse a GoogleSQL SPANNER_TYPE string from INFORMATION_SCHEMA.COLUMNS or
/// an explicit parameter type.
///
/// Examples: "BOOL", "INT64", "STRING(MAX)", "ARRAY<INT64>",
/// "STRUCT<name STRING(MAX), age INT64>", "PROTO<my.pkg.Msg>".
///
/// This parser is intentionally fail-closed: a type is returned only when the
/// entire input is valid. In particular, it never converts an unknown type to
/// STRING because that would change the wire contract of a parameter or schema.
pub fn parse_spanner_type(input: &str) -> Result<Type, TypeParseError> {
    validate_type_input_size(input, "GoogleSQL Spanner")?;
    SpannerTypeParser::new(input).parse()
}

fn validate_type_input_size(input: &str, dialect: &'static str) -> Result<(), TypeParseError> {
    if input.len() <= MAX_TYPE_INPUT_BYTES {
        return Ok(());
    }

    let mut preview_end = MAX_ERROR_INPUT_PREVIEW_BYTES.min(input.len());
    while !input.is_char_boundary(preview_end) {
        preview_end -= 1;
    }
    let mut type_text = input[..preview_end].to_string();
    type_text.push_str("...[truncated]");

    Err(TypeParseError {
        dialect,
        type_text,
        position: input.len(),
        context: format!(
            "type input is {} bytes; maximum is {MAX_TYPE_INPUT_BYTES} bytes",
            input.len()
        ),
    })
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
    depth: usize,
}

impl<'a> SpannerTypeParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            depth: 0,
        }
    }

    fn parse(&mut self) -> Result<Type, TypeParseError> {
        let parsed = self.parse_type()?;
        self.finish()?;
        Ok(parsed)
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

    fn error(&self, position: usize, context: impl Into<String>) -> TypeParseError {
        TypeParseError {
            dialect: "GoogleSQL Spanner",
            type_text: self.input.to_string(),
            position,
            context: context.into(),
        }
    }

    fn finish(&mut self) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        if self.pos == self.input.len() {
            return Ok(());
        }

        Err(self.error(
            self.pos,
            format!(
                "unexpected trailing input starting at {:?}",
                self.remaining_context()
            ),
        ))
    }

    fn remaining_context(&self) -> String {
        self.input[self.pos..].chars().take(24).collect()
    }

    fn expect(&mut self, expected: u8, context: &str) -> Result<(), TypeParseError> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(self.error(self.pos, context))
        }
    }

    fn parse_ident(&mut self) -> Option<&'a str> {
        if !self
            .peek()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == b'_')
        {
            return None;
        }

        let start = self.pos;
        while self.pos < self.input.len() {
            let ch = self.input.as_bytes()[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        Some(&self.input[start..self.pos])
    }

    fn parse_fqn(&mut self) -> Result<&'a str, TypeParseError> {
        let start = self.pos;
        self.parse_ident().ok_or_else(|| {
            self.error(
                self.pos,
                "expected a qualified PROTO or ENUM name after '<'",
            )
        })?;

        while self.consume(b'.') {
            let component_pos = self.pos;
            self.parse_ident().ok_or_else(|| {
                self.error(
                    component_pos,
                    "expected an identifier after '.' in qualified PROTO or ENUM name",
                )
            })?;
        }

        Ok(&self.input[start..self.pos])
    }

    fn parse_length_modifier(
        &mut self,
        type_name: &str,
        max_length: u64,
    ) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        if !self.consume(b'(') {
            return Ok(());
        }

        self.skip_whitespace();
        let annotation_pos = self.pos;
        if let Some(word) = self.parse_ident() {
            if !word.eq_ignore_ascii_case("MAX") {
                return Err(self.error(
                    annotation_pos,
                    format!("expected MAX or an integer in {type_name} length modifier"),
                ));
            }
        } else {
            let length = self.parse_google_sql_uint(&format!(
                "expected MAX or an integer in {type_name} length modifier"
            ))?;
            if !(1..=max_length).contains(&length) {
                return Err(self.error(
                    annotation_pos,
                    format!("{type_name} length must be between 1 and {max_length}, or MAX"),
                ));
            }
        }

        self.skip_whitespace();
        self.expect(
            b')',
            &format!("expected ')' after {type_name} length modifier"),
        )
    }

    fn parse_google_sql_uint(&mut self, context: &str) -> Result<u64, TypeParseError> {
        if self.peek() == Some(b'0')
            && matches!(
                self.input.as_bytes().get(self.pos + 1).copied(),
                Some(b'x' | b'X')
            )
        {
            self.pos += 2;
            return self.parse_uint_digits(16, context);
        }
        self.parse_uint_digits(10, context)
    }

    fn parse_uint_digits(&mut self, radix: u32, context: &str) -> Result<u64, TypeParseError> {
        let start = self.pos;
        let mut value = 0_u64;
        while let Some(digit) = self.peek().and_then(|ch| (ch as char).to_digit(radix)) {
            value = value
                .checked_mul(u64::from(radix))
                .and_then(|value| value.checked_add(u64::from(digit)))
                .ok_or_else(|| self.error(start, format!("{context}; integer overflow")))?;
            self.pos += 1;
        }
        if self.pos == start {
            Err(self.error(start, context))
        } else {
            Ok(value)
        }
    }

    fn parse_type(&mut self) -> Result<Type, TypeParseError> {
        if self.depth >= MAX_TYPE_NESTING_DEPTH {
            return Err(self.error(
                self.pos,
                format!("type nesting exceeds the limit of {MAX_TYPE_NESTING_DEPTH} levels"),
            ));
        }

        self.depth += 1;
        let parsed = self.parse_type_inner();
        self.depth -= 1;
        parsed
    }

    fn parse_type_inner(&mut self) -> Result<Type, TypeParseError> {
        self.skip_whitespace();
        let type_pos = self.pos;
        let ident = self
            .parse_ident()
            .ok_or_else(|| self.error(type_pos, "expected a type name"))?
            .to_ascii_uppercase();

        match ident.as_str() {
            "BOOL" | "BOOLEAN" => Ok(type_from_code(TypeCode::Bool)),
            "INT64" => Ok(type_from_code(TypeCode::Int64)),
            "FLOAT32" => Ok(type_from_code(TypeCode::Float32)),
            "FLOAT64" => Ok(type_from_code(TypeCode::Float64)),
            "NUMERIC" => Ok(type_from_code(TypeCode::Numeric)),
            "STRING" => {
                self.parse_length_modifier("STRING", GOOGLESQL_STRING_MAX_LENGTH)?;
                Ok(type_from_code(TypeCode::String))
            }
            "BYTES" => {
                self.parse_length_modifier("BYTES", GOOGLESQL_BYTES_MAX_LENGTH)?;
                Ok(type_from_code(TypeCode::Bytes))
            }
            "DATE" => Ok(type_from_code(TypeCode::Date)),
            "TIMESTAMP" => Ok(type_from_code(TypeCode::Timestamp)),
            "JSON" => Ok(type_from_code(TypeCode::Json)),
            "INTERVAL" => Ok(type_from_code(TypeCode::Interval)),
            "UUID" => Ok(type_from_code(TypeCode::Uuid)),
            "DATETIME" => Err(self.error(
                type_pos,
                "DATETIME is unsupported because the Spanner data-plane Type API has no lossless DATETIME representation",
            )),
            "ARRAY" => {
                self.skip_whitespace();
                self.expect(b'<', "expected '<' after ARRAY")?;
                let element_pos = self.pos;
                let elem = self.parse_type()?;
                if elem.code() == TypeCode::Array {
                    return Err(
                        self.error(element_pos, "ARRAY element type cannot be ARRAY directly")
                    );
                }
                self.skip_whitespace();
                self.expect(b'>', "expected '>' to close ARRAY")?;
                if self.parse_vector_length_modifier()?
                    && !matches!(elem.code(), TypeCode::Float32 | TypeCode::Float64)
                {
                    return Err(self.error(
                        element_pos,
                        "vector_length is only supported for ARRAY<FLOAT32> and ARRAY<FLOAT64>",
                    ));
                }
                Ok(array_type(elem))
            }
            "STRUCT" => {
                self.skip_whitespace();
                self.expect(b'<', "expected '<' after STRUCT")?;
                let fields = self.parse_struct_fields()?;
                self.skip_whitespace();
                self.expect(b'>', "expected '>' to close STRUCT")?;
                Ok(struct_type(fields))
            }
            "PROTO" => {
                self.skip_whitespace();
                self.expect(b'<', "expected '<' after PROTO")?;
                self.skip_whitespace();
                let fqn = self.parse_fqn()?.to_string();
                self.skip_whitespace();
                self.expect(b'>', "expected '>' to close PROTO")?;
                Ok(proto_type(model::TypeCode::Proto, &fqn))
            }
            "ENUM" => {
                self.skip_whitespace();
                self.expect(b'<', "expected '<' after ENUM")?;
                self.skip_whitespace();
                let fqn = self.parse_fqn()?.to_string();
                self.skip_whitespace();
                self.expect(b'>', "expected '>' to close ENUM")?;
                Ok(proto_type(model::TypeCode::Enum, &fqn))
            }
            other => Err(self.error(
                type_pos,
                format!("unknown GoogleSQL Spanner type name '{other}'"),
            )),
        }
    }

    /// Parse the GoogleSQL vector-length schema modifier. The data-plane Type
    /// does not encode the length, so successful parsing intentionally returns
    /// the normal ARRAY Type.
    fn parse_vector_length_modifier(&mut self) -> Result<bool, TypeParseError> {
        self.skip_whitespace();
        if !self.consume(b'(') {
            return Ok(false);
        }

        self.skip_whitespace();
        let name_pos = self.pos;
        let name = self.parse_ident().ok_or_else(|| {
            self.error(name_pos, "expected vector_length in ARRAY vector modifier")
        })?;
        if !name.eq_ignore_ascii_case("vector_length") {
            return Err(self.error(name_pos, "expected vector_length in ARRAY vector modifier"));
        }

        self.skip_whitespace();
        self.expect(b'=', "expected '=>' after vector_length")?;
        self.skip_whitespace();
        self.expect(b'>', "expected '=>' after vector_length")?;
        self.skip_whitespace();
        let length_pos = self.pos;
        let length =
            self.parse_google_sql_uint("expected a non-negative integer after vector_length=>")?;
        if length > i64::MAX as u64 {
            return Err(self.error(
                length_pos,
                "vector_length must fit in a signed 64-bit integer",
            ));
        }
        self.skip_whitespace();
        self.expect(b')', "expected ')' after ARRAY vector_length modifier")?;

        self.skip_whitespace();
        if self.peek() == Some(b'(') {
            return Err(self.error(self.pos, "duplicate ARRAY vector_length modifier"));
        }
        Ok(true)
    }

    fn parse_struct_fields(&mut self) -> Result<Vec<model::struct_type::Field>, TypeParseError> {
        let mut fields = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(b'>') {
                return Ok(fields);
            }
            if self.pos >= self.input.len() {
                return Err(self.error(self.pos, "expected a STRUCT field or closing '>'"));
            }
            if fields.len() >= MAX_STRUCT_FIELDS {
                return Err(self.error(
                    self.pos,
                    format!("STRUCT field count exceeds the limit of {MAX_STRUCT_FIELDS}"),
                ));
            }

            let field_pos = self.pos;
            let name = if self.peek() == Some(b'`') {
                self.parse_quoted_identifier()?
            } else {
                let first = self
                    .parse_ident()
                    .ok_or_else(|| self.error(field_pos, "expected a STRUCT field name or type"))?;
                let first_end = self.pos;
                self.skip_whitespace();
                let next = self.peek();
                if starts_unnamed_spanner_type(first, next) {
                    self.pos = field_pos;
                    String::new()
                } else if is_googlesql_reserved_keyword(first) {
                    return Err(self.error(
                        field_pos,
                        format!(
                            "unquoted STRUCT field name '{first}' is a reserved GoogleSQL keyword"
                        ),
                    ));
                } else {
                    self.pos = first_end;
                    first.to_string()
                }
            };
            if !name.is_empty() {
                self.skip_whitespace();
            }
            let field_type = self.parse_type()?;
            let field_type: model::Type = field_type.into();

            fields.push(
                model::struct_type::Field::new()
                    .set_name(name)
                    .set_type(field_type),
            );

            self.skip_whitespace();
            if self.consume(b',') {
                self.skip_whitespace();
                if self.peek() == Some(b'>') || self.pos == self.input.len() {
                    return Err(self.error(self.pos, "expected a STRUCT field after ','"));
                }
                continue;
            }
            if self.peek() == Some(b'>') {
                return Ok(fields);
            }
            return Err(self.error(self.pos, "expected ',' or '>' after STRUCT field"));
        }
    }

    fn parse_quoted_identifier(&mut self) -> Result<String, TypeParseError> {
        let quote_pos = self.pos;
        self.expect(b'`', "expected '`' to start quoted STRUCT field name")?;
        // A quoted field often appears near the beginning of a large STRUCT.
        // Keep each retained field name small instead of reserving all input
        // bytes for every field.
        let mut name = String::with_capacity(32);

        loop {
            let Some(ch) = self.peek() else {
                return Err(self.error(quote_pos, "unterminated quoted STRUCT field name"));
            };
            match ch {
                b'`' => {
                    self.pos += 1;
                    if name.is_empty() {
                        return Err(
                            self.error(quote_pos, "quoted STRUCT field name cannot be empty")
                        );
                    }
                    return Ok(name);
                }
                b'\\' => {
                    let escape_pos = self.pos;
                    self.pos += 1;
                    name.push(self.parse_quoted_identifier_escape(escape_pos)?);
                }
                b'\n' | b'\r' => {
                    return Err(
                        self.error(self.pos, "unescaped newline in quoted STRUCT field name")
                    );
                }
                _ => {
                    let character = self.input[self.pos..].chars().next().ok_or_else(|| {
                        self.error(self.pos, "invalid UTF-8 in quoted STRUCT field name")
                    })?;
                    self.pos += character.len_utf8();
                    name.push(character);
                }
            }
        }
    }

    fn parse_quoted_identifier_escape(
        &mut self,
        escape_pos: usize,
    ) -> Result<char, TypeParseError> {
        let Some(marker) = self.peek() else {
            return Err(self.error(escape_pos, "unterminated quoted identifier escape sequence"));
        };
        if !marker.is_ascii() {
            return Err(self.error(
                escape_pos,
                "invalid escape sequence in quoted STRUCT field name",
            ));
        }
        self.pos += 1;

        match marker {
            b'a' => Ok('\u{0007}'),
            b'b' => Ok('\u{0008}'),
            b'f' => Ok('\u{000c}'),
            b'n' => Ok('\n'),
            b'r' => Ok('\r'),
            b't' => Ok('\t'),
            b'v' => Ok('\u{000b}'),
            b'\\' => Ok('\\'),
            b'?' => Ok('?'),
            b'"' => Ok('"'),
            b'\'' => Ok('\''),
            b'`' => Ok('`'),
            b'0'..=b'7' => {
                let leading = u32::from(marker - b'0');
                let trailing = self.parse_fixed_escape_digits(
                    2,
                    8,
                    "octal escape must contain exactly 3 digits in the range 0-7",
                )?;
                char::from_u32(leading * 64 + trailing).ok_or_else(|| {
                    self.error(escape_pos, "octal escape is not a valid Unicode character")
                })
            }
            b'x' | b'X' => {
                let value = self.parse_fixed_escape_digits(
                    2,
                    16,
                    "hex escape must contain exactly 2 hexadecimal digits",
                )?;
                char::from_u32(value).ok_or_else(|| {
                    self.error(escape_pos, "hex escape is not a valid Unicode character")
                })
            }
            b'u' => {
                let value = self.parse_fixed_escape_digits(
                    4,
                    16,
                    "\\u escape must contain exactly 4 hexadecimal digits",
                )?;
                self.decode_unicode_escape(escape_pos, value)
            }
            b'U' => {
                let value = self.parse_fixed_escape_digits(
                    8,
                    16,
                    "\\U escape must contain exactly 8 hexadecimal digits",
                )?;
                self.decode_unicode_escape(escape_pos, value)
            }
            _ => Err(self.error(
                escape_pos,
                "invalid escape sequence in quoted STRUCT field name",
            )),
        }
    }

    fn parse_fixed_escape_digits(
        &mut self,
        count: usize,
        radix: u32,
        context: &str,
    ) -> Result<u32, TypeParseError> {
        let mut value = 0_u32;
        for _ in 0..count {
            let digit = self
                .peek()
                .and_then(|ch| (ch as char).to_digit(radix))
                .ok_or_else(|| self.error(self.pos, context))?;
            value = value * radix + digit;
            self.pos += 1;
        }
        Ok(value)
    }

    fn decode_unicode_escape(&self, escape_pos: usize, value: u32) -> Result<char, TypeParseError> {
        if (0xd800..=0xdfff).contains(&value) {
            return Err(self.error(
                escape_pos,
                "Unicode escape cannot encode a surrogate code point",
            ));
        }
        char::from_u32(value).ok_or_else(|| {
            self.error(
                escape_pos,
                "Unicode escape exceeds the maximum code point U+10FFFF",
            )
        })
    }
}

fn is_spanner_type_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "BOOL"
            | "BOOLEAN"
            | "INT64"
            | "FLOAT32"
            | "FLOAT64"
            | "NUMERIC"
            | "STRING"
            | "BYTES"
            | "DATE"
            | "TIMESTAMP"
            | "DATETIME"
            | "JSON"
            | "INTERVAL"
            | "UUID"
            | "ARRAY"
            | "STRUCT"
            | "PROTO"
            | "ENUM"
    )
}

fn starts_unnamed_spanner_type(name: &str, next: Option<u8>) -> bool {
    if !is_spanner_type_name(name) {
        return false;
    }

    // `STRUCT<INT64, STRING>` has unnamed fields, while
    // `STRUCT<INT64 INT64>` names a field INT64. The next token disambiguates
    // the two forms without treating ordinary (non-reserved) names as errors.
    matches!(next, None | Some(b',' | b'>'))
        || matches!(
            name.to_ascii_uppercase().as_str(),
            "STRING" | "BYTES" if next == Some(b'(')
        )
        || matches!(
            name.to_ascii_uppercase().as_str(),
            "ARRAY" | "STRUCT" | "PROTO" | "ENUM" if next == Some(b'<')
        )
}

fn is_googlesql_reserved_keyword(name: &str) -> bool {
    let uppercase = name.to_ascii_uppercase();
    GOOGLESQL_RESERVED_KEYWORDS
        .binary_search(&uppercase.as_str())
        .is_ok()
}

struct PgTypeParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> PgTypeParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(&mut self) -> Result<Type, TypeParseError> {
        let parsed = self.parse_base_type()?;

        self.skip_whitespace();
        let bracket_suffix = self.consume(b'[');
        let array_keyword = !bracket_suffix && self.consume_keyword("array");
        if !bracket_suffix && !array_keyword {
            self.finish()?;
            return Ok(parsed);
        }

        if bracket_suffix {
            self.parse_array_dimension()?;
        } else {
            self.skip_whitespace();
            if self.consume(b'[') {
                self.parse_array_dimension()?;
            }
        }
        if let Some(vector_pos) = self.parse_vector_length_modifier()? {
            if parsed.code() != TypeCode::Float64 {
                return Err(self.error(
                    vector_pos,
                    "VECTOR LENGTH is only supported for float8[] and double precision[]",
                ));
            }
        }
        self.finish()?;
        Ok(spanner_types::array(parsed))
    }

    fn error(&self, position: usize, context: impl Into<String>) -> TypeParseError {
        TypeParseError {
            dialect: "PostgreSQL Spanner",
            type_text: self.input.to_string(),
            position,
            context: context.into(),
        }
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

    fn expect(&mut self, expected: u8, context: &str) -> Result<(), TypeParseError> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(self.error(self.pos, context))
        }
    }

    fn finish(&mut self) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        if self.pos == self.input.len() {
            return Ok(());
        }
        Err(self.error(
            self.pos,
            format!(
                "unexpected trailing input starting at {:?}",
                self.remaining_context()
            ),
        ))
    }

    fn remaining_context(&self) -> String {
        self.input[self.pos..].chars().take(24).collect()
    }

    fn parse_ident(&mut self) -> Option<&'a str> {
        if !self
            .peek()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == b'_')
        {
            return None;
        }

        let start = self.pos;
        self.pos += 1;
        while self
            .peek()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
        {
            self.pos += 1;
        }
        Some(&self.input[start..self.pos])
    }

    fn expect_keyword(&mut self, expected: &str, context: &str) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        let position = self.pos;
        let actual = self
            .parse_ident()
            .ok_or_else(|| self.error(position, context))?;
        if actual.eq_ignore_ascii_case(expected) {
            Ok(())
        } else {
            Err(self.error(position, context))
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        self.skip_whitespace();
        let position = self.pos;
        let Some(actual) = self.parse_ident() else {
            return false;
        };
        if actual.eq_ignore_ascii_case(expected) {
            true
        } else {
            self.pos = position;
            false
        }
    }

    fn parse_array_dimension(&mut self) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        if self.consume(b']') {
            return Ok(());
        }
        self.parse_decimal_value("expected an array size or ']'")?;
        self.skip_whitespace();
        self.expect(b']', "expected ']' to close PostgreSQL array suffix")
    }

    fn parse_decimal_value(&mut self, context: &str) -> Result<u64, TypeParseError> {
        let start = self.pos;
        let mut value = 0_u64;
        while let Some(digit) = self.peek().filter(u8::is_ascii_digit) {
            value = value
                .checked_mul(10)
                .and_then(|value| value.checked_add(u64::from(digit - b'0')))
                .ok_or_else(|| self.error(start, format!("{context}; integer overflow")))?;
            self.pos += 1;
        }
        if self.pos == start {
            Err(self.error(start, context))
        } else {
            Ok(value)
        }
    }

    fn parse_bounded_integer_modifier(
        &mut self,
        name: &str,
        min_value: u64,
        max_value: u64,
    ) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        if !self.consume(b'(') {
            return Ok(());
        }

        self.skip_whitespace();
        let value_pos = self.pos;
        let value =
            self.parse_decimal_value(&format!("expected a decimal integer in {name} modifier"))?;
        if !(min_value..=max_value).contains(&value) {
            return Err(self.error(
                value_pos,
                format!("{name} must be between {min_value} and {max_value}"),
            ));
        }
        self.skip_whitespace();
        self.expect(b')', &format!("expected ')' after {name} modifier"))
    }

    fn parse_numeric_modifier(&mut self) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        if !self.consume(b'(') {
            return Ok(());
        }

        self.skip_whitespace();
        let precision_pos = self.pos;
        let precision = self.parse_decimal_value("expected numeric precision")?;
        if !(1..=PG_NUMERIC_MAX_PRECISION).contains(&precision) {
            return Err(self.error(
                precision_pos,
                format!("numeric precision must be between 1 and {PG_NUMERIC_MAX_PRECISION}"),
            ));
        }

        self.skip_whitespace();
        if self.consume(b',') {
            self.skip_whitespace();
            let scale_pos = self.pos;
            let scale = self.parse_decimal_value("expected numeric scale")?;
            if scale > PG_NUMERIC_MAX_SCALE {
                return Err(self.error(
                    scale_pos,
                    format!("numeric scale must be between 0 and {PG_NUMERIC_MAX_SCALE}"),
                ));
            }
            self.skip_whitespace();
        }

        self.expect(b')', "expected ')' after numeric modifier")
    }

    fn parse_vector_length_modifier(&mut self) -> Result<Option<usize>, TypeParseError> {
        self.skip_whitespace();
        let modifier_pos = self.pos;
        let Some(keyword) = self.parse_ident() else {
            return Ok(None);
        };
        if !keyword.eq_ignore_ascii_case("vector") {
            self.pos = modifier_pos;
            return Ok(None);
        }

        self.expect_keyword("length", "expected 'LENGTH' after 'VECTOR'")?;
        self.skip_whitespace();
        let length_pos = self.pos;
        let length =
            self.parse_decimal_value("expected a non-negative integer after VECTOR LENGTH")?;
        if length > i64::MAX as u64 {
            return Err(self.error(
                length_pos,
                "VECTOR LENGTH must fit in a signed 64-bit integer",
            ));
        }
        Ok(Some(modifier_pos))
    }

    fn parse_interval_qualifier(&mut self) -> Result<(), TypeParseError> {
        self.skip_whitespace();
        let qualifier_pos = self.pos;
        let Some(first) = self.parse_ident() else {
            return self.parse_bounded_integer_modifier(
                "interval precision",
                0,
                PG_DATETIME_MAX_PRECISION,
            );
        };
        if first.eq_ignore_ascii_case("array") {
            self.pos = qualifier_pos;
            return Ok(());
        }
        let first = first.to_ascii_lowercase();
        let first_rank = interval_field_rank(&first).ok_or_else(|| {
            self.error(
                qualifier_pos,
                format!("unknown interval field qualifier '{first}'"),
            )
        })?;

        self.skip_whitespace();
        let after_first = self.pos;
        if let Some(keyword) = self.parse_ident() {
            if keyword.eq_ignore_ascii_case("array") {
                // ARRAY belongs to the outer type grammar, not the interval
                // qualifier. Leave it unconsumed for parse().
                self.pos = after_first;
            } else if !keyword.eq_ignore_ascii_case("to") {
                return Err(self.error(
                    after_first,
                    "expected 'TO', an interval precision modifier, an array suffix, or end of input",
                ));
            } else {
                self.skip_whitespace();
                let second_pos = self.pos;
                let second = self
                    .parse_ident()
                    .ok_or_else(|| self.error(second_pos, "expected interval field after TO"))?;
                let second = second.to_ascii_lowercase();
                let second_rank = interval_field_rank(&second).ok_or_else(|| {
                    self.error(
                        second_pos,
                        format!("unknown interval field qualifier '{second}'"),
                    )
                })?;
                if first_rank >= second_rank {
                    return Err(self.error(
                        second_pos,
                        "interval field after TO must be less significant than the first field",
                    ));
                }
            }
        } else {
            self.pos = after_first;
        }

        self.parse_bounded_integer_modifier("interval precision", 0, PG_DATETIME_MAX_PRECISION)
    }

    fn parse_base_type(&mut self) -> Result<Type, TypeParseError> {
        self.skip_whitespace();
        let type_pos = self.pos;
        let first = self
            .parse_ident()
            .ok_or_else(|| self.error(type_pos, "expected a PostgreSQL type name"))?
            .to_ascii_lowercase();

        match first.as_str() {
            "boolean" | "bool" => Ok(spanner_types::bool()),
            "bigint" | "int8" | "int" | "integer" => Ok(spanner_types::int64()),
            "real" | "float4" => Ok(spanner_types::float32()),
            "float8" => Ok(spanner_types::float64()),
            "double" => {
                self.expect_keyword("precision", "expected 'precision' after 'double'")?;
                Ok(spanner_types::float64())
            }
            "numeric" | "decimal" => {
                self.parse_numeric_modifier()?;
                Ok(spanner_types::pg_numeric())
            }
            "character" => {
                self.expect_keyword("varying", "expected 'varying' after 'character'")?;
                self.parse_bounded_integer_modifier(
                    "character varying length",
                    1,
                    PG_VARCHAR_MAX_LENGTH,
                )?;
                Ok(spanner_types::string())
            }
            "varchar" => {
                self.parse_bounded_integer_modifier("varchar length", 1, PG_VARCHAR_MAX_LENGTH)?;
                Ok(spanner_types::string())
            }
            "text" => Ok(spanner_types::string()),
            "bytea" => Ok(spanner_types::bytes()),
            "date" => Ok(spanner_types::date()),
            "timestamp" => {
                self.parse_bounded_integer_modifier(
                    "timestamp precision",
                    0,
                    PG_DATETIME_MAX_PRECISION,
                )?;
                self.expect_keyword("with", "expected 'with' after timestamp precision")?;
                self.expect_keyword("time", "expected 'time' after 'timestamp with'")?;
                self.expect_keyword("zone", "expected 'zone' after 'timestamp with time'")?;
                Ok(spanner_types::timestamp())
            }
            "timestamptz" => {
                self.parse_bounded_integer_modifier(
                    "timestamptz precision",
                    0,
                    PG_DATETIME_MAX_PRECISION,
                )?;
                Ok(spanner_types::timestamp())
            }
            "jsonb" => Ok(spanner_types::pg_jsonb()),
            "oid" => Ok(spanner_types::pg_oid()),
            "interval" => {
                self.parse_interval_qualifier()?;
                Ok(spanner_types::interval())
            }
            "uuid" => Ok(spanner_types::uuid()),
            _ => Err(self.error(
                type_pos,
                format!("unknown PostgreSQL Spanner type name '{first}'"),
            )),
        }
    }
}

fn interval_field_rank(field: &str) -> Option<u8> {
    match field {
        "year" => Some(0),
        "month" => Some(1),
        "day" => Some(2),
        "hour" => Some(3),
        "minute" => Some(4),
        "second" => Some(5),
        _ => None,
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
        assert_eq!(parse_spanner_type("BOOL").unwrap().code(), TypeCode::Bool);
        assert_eq!(
            parse_spanner_type("BOOLEAN").unwrap().code(),
            TypeCode::Bool
        );
        assert_eq!(parse_spanner_type("INT64").unwrap().code(), TypeCode::Int64);
        assert_eq!(
            parse_spanner_type("FLOAT32").unwrap().code(),
            TypeCode::Float32
        );
        assert_eq!(
            parse_spanner_type("FLOAT64").unwrap().code(),
            TypeCode::Float64
        );
        assert_eq!(
            parse_spanner_type("NUMERIC").unwrap().code(),
            TypeCode::Numeric
        );
        assert_eq!(parse_spanner_type("DATE").unwrap().code(), TypeCode::Date);
        assert_eq!(
            parse_spanner_type("TIMESTAMP").unwrap().code(),
            TypeCode::Timestamp
        );
        assert_eq!(parse_spanner_type("JSON").unwrap().code(), TypeCode::Json);
        assert_eq!(parse_spanner_type("UUID").unwrap().code(), TypeCode::Uuid);
        assert_eq!(
            parse_spanner_type("INTERVAL").unwrap().code(),
            TypeCode::Interval
        );
    }

    #[test]
    fn test_parse_string_with_length() {
        for input in [
            "STRING",
            "STRING(1)",
            "STRING(2621440)",
            "STRING(0x280000)",
            "STRING(MAX)",
            "string(max)",
        ] {
            assert_eq!(parse_spanner_type(input).unwrap().code(), TypeCode::String);
        }
        for input in [
            "BYTES",
            "BYTES(1)",
            "BYTES(10485760)",
            "BYTES(0xA00000)",
            "BYTES(MAX)",
        ] {
            assert_eq!(parse_spanner_type(input).unwrap().code(), TypeCode::Bytes);
        }
    }

    #[test]
    fn test_parse_array() {
        let t = parse_spanner_type("ARRAY<INT64>").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Int64);
    }

    #[test]
    fn test_parse_array_of_string() {
        let t = parse_spanner_type("ARRAY<STRING(MAX)>").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::String);
    }

    #[test]
    fn test_parse_google_sql_vector_arrays() {
        for input in [
            "ARRAY<FLOAT32>(vector_length=>128)",
            " array < float64 > ( VECTOR_LENGTH => 0 ) ",
        ] {
            let t = parse_spanner_type(input).unwrap();
            assert_eq!(t.code(), TypeCode::Array);
            assert!(matches!(
                t.array_element_type().unwrap().code(),
                TypeCode::Float32 | TypeCode::Float64
            ));
        }
    }

    #[test]
    fn test_parse_google_sql_uppercase_hex_integer_literals() {
        for input in [
            "STRING(0X280000)",
            "BYTES(0XA00000)",
            "ARRAY<FLOAT64>(vector_length=>0X7FFFFFFFFFFFFFFF)",
        ] {
            parse_spanner_type(input).unwrap_or_else(|error| {
                panic!("expected {input:?} to parse: {error}");
            });
        }

        assert_parse_errors(
            parse_spanner_type,
            &[
                (
                    "STRING(0X280001)",
                    "STRING length must be between 1 and 2621440",
                ),
                ("STRING(0X10000000000000000)", "integer overflow"),
                (
                    "BYTES(0XA00001)",
                    "BYTES length must be between 1 and 10485760",
                ),
                ("BYTES(0X10000000000000000)", "integer overflow"),
                (
                    "ARRAY<FLOAT64>(vector_length=>0X8000000000000000)",
                    "vector_length must fit in a signed 64-bit integer",
                ),
                (
                    "ARRAY<FLOAT64>(vector_length=>0X10000000000000000)",
                    "integer overflow",
                ),
            ],
        );
    }

    #[test]
    fn test_parse_struct() {
        let t = parse_spanner_type("STRUCT<name STRING(MAX), age INT64>").unwrap();
        assert_eq!(t.code(), TypeCode::Struct);
        let st = t.struct_type().unwrap();
        assert_eq!(st.fields.len(), 2);
        assert_eq!(st.fields[0].name, "name");
        assert_eq!(field_code(&st.fields[0]), TypeCode::String);
        assert_eq!(st.fields[1].name, "age");
        assert_eq!(field_code(&st.fields[1]), TypeCode::Int64);
    }

    #[test]
    fn test_parse_empty_and_unnamed_structs() {
        let t = parse_spanner_type("STRUCT<>").unwrap();
        assert_eq!(t.code(), TypeCode::Struct);
        assert!(t.struct_type().unwrap().fields.is_empty());

        let t = parse_spanner_type("STRUCT<INT64, STRING(MAX), ARRAY<BOOL>>").unwrap();
        let fields = &t.struct_type().unwrap().fields;
        assert_eq!(fields.len(), 3);
        assert!(fields.iter().all(|field| field.name.is_empty()));
        assert_eq!(field_code(&fields[0]), TypeCode::Int64);
        assert_eq!(field_code(&fields[1]), TypeCode::String);
        assert_eq!(field_code(&fields[2]), TypeCode::Array);
    }

    #[test]
    fn test_parse_quoted_struct_field_names() {
        let t = parse_spanner_type(
            r#"STRUCT<`display name` STRING, `ARRAY` INT64, `a\`b` BOOL, `日本語` FLOAT64>"#,
        )
        .unwrap();
        let fields = &t.struct_type().unwrap().fields;
        assert_eq!(fields[0].name, "display name");
        assert_eq!(fields[1].name, "ARRAY");
        assert_eq!(fields[2].name, "a`b");
        assert_eq!(fields[3].name, "日本語");

        let escaped =
            parse_spanner_type(r#"STRUCT<`\101\x42\X43\u0020\u03A9\U0001F642` INT64>"#).unwrap();
        assert_eq!(escaped.struct_type().unwrap().fields[0].name, "ABC Ω🙂");

        let simple_escapes =
            parse_spanner_type(r#"STRUCT<`\a\b\f\n\r\t\v\\\?\"\'\`` INT64>"#).unwrap();
        assert_eq!(
            simple_escapes.struct_type().unwrap().fields[0].name,
            "\u{0007}\u{0008}\u{000c}\n\r\t\u{000b}\\?\"'`"
        );

        let unnamed = parse_spanner_type("STRUCT<INT64, STRING>").unwrap();
        assert!(unnamed
            .struct_type()
            .unwrap()
            .fields
            .iter()
            .all(|field| field.name.is_empty()));
    }

    #[test]
    fn quoted_struct_field_names_do_not_retain_input_sized_capacity() {
        let fields = (0..MAX_STRUCT_FIELDS)
            .map(|index| format!("`field_{index:04}` INT64"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut input = format!("STRUCT<{fields}>");
        input.push_str(&" ".repeat(MAX_TYPE_INPUT_BYTES - input.len()));
        assert_eq!(input.len(), MAX_TYPE_INPUT_BYTES);

        let parsed = parse_spanner_type(&input).unwrap();
        let retained_capacity = parsed
            .struct_type()
            .unwrap()
            .fields
            .iter()
            .map(|field| field.name.capacity())
            .sum::<usize>();

        // The parser retains these names in the returned Type. Reserving the
        // remaining input for every field would retain about 64 MiB here.
        assert!(
            retained_capacity <= input.len() / 2,
            "quoted field names retained {retained_capacity} bytes for a {}-byte input",
            input.len()
        );
    }

    #[test]
    fn unquoted_struct_field_names_reject_googlesql_reserved_keywords() {
        assert!(GOOGLESQL_RESERVED_KEYWORDS
            .windows(2)
            .all(|pair| pair[0] < pair[1]));

        for keyword in [
            "SELECT", "from", "GROUP", "ARRAY", "struct", "PROTO", "enum",
        ] {
            let error = parse_spanner_type(&format!("STRUCT<{keyword} INT64>")).unwrap_err();
            assert!(
                error.context.contains("reserved GoogleSQL keyword"),
                "expected {keyword:?} to be rejected as a reserved keyword, got {error:?}"
            );
        }

        for name in ["field", "int64", "String", "not_a_keyword"] {
            let parsed = parse_spanner_type(&format!("STRUCT<{name} INT64>")).unwrap();
            assert_eq!(parsed.struct_type().unwrap().fields[0].name, name);
        }

        for keyword in ["SELECT", "ARRAY", "STRUCT"] {
            let parsed = parse_spanner_type(&format!("STRUCT<`{keyword}` INT64>")).unwrap();
            assert_eq!(parsed.struct_type().unwrap().fields[0].name, keyword);
        }
    }

    #[test]
    fn test_parse_proto_enum() {
        let t = parse_spanner_type("PROTO<my.package.Message>").unwrap();
        assert_eq!(t.code(), TypeCode::Proto);
        assert_eq!(raw_type(&t).proto_type_fqn, "my.package.Message");

        let t = parse_spanner_type("ENUM<my.package.MyEnum>").unwrap();
        assert_eq!(t.code(), TypeCode::Enum);
        assert_eq!(raw_type(&t).proto_type_fqn, "my.package.MyEnum");
    }

    #[test]
    fn test_parse_pg_scalar_types() {
        assert_eq!(
            parse_pg_spanner_type("boolean").unwrap().code(),
            TypeCode::Bool
        );
        assert_eq!(
            parse_pg_spanner_type("bool").unwrap().code(),
            TypeCode::Bool
        );
        assert_eq!(
            parse_pg_spanner_type("bigint").unwrap().code(),
            TypeCode::Int64
        );
        assert_eq!(
            parse_pg_spanner_type("int8").unwrap().code(),
            TypeCode::Int64
        );
        assert_eq!(
            parse_pg_spanner_type("int").unwrap().code(),
            TypeCode::Int64
        );
        assert_eq!(
            parse_pg_spanner_type("integer").unwrap().code(),
            TypeCode::Int64
        );
        assert_eq!(
            parse_pg_spanner_type("real").unwrap().code(),
            TypeCode::Float32
        );
        assert_eq!(
            parse_pg_spanner_type("float4").unwrap().code(),
            TypeCode::Float32
        );
        assert_eq!(
            parse_pg_spanner_type("double precision").unwrap().code(),
            TypeCode::Float64
        );
        assert_eq!(
            parse_pg_spanner_type("float8").unwrap().code(),
            TypeCode::Float64
        );
        assert_eq!(
            parse_pg_spanner_type("numeric").unwrap().code(),
            TypeCode::Numeric
        );
        assert_eq!(
            parse_pg_spanner_type("decimal").unwrap().code(),
            TypeCode::Numeric
        );
        assert_eq!(
            parse_pg_spanner_type("date").unwrap().code(),
            TypeCode::Date
        );
        assert_eq!(
            parse_pg_spanner_type("timestamp with time zone")
                .unwrap()
                .code(),
            TypeCode::Timestamp
        );
        assert_eq!(
            parse_pg_spanner_type("timestamptz").unwrap().code(),
            TypeCode::Timestamp
        );
        assert_eq!(
            parse_pg_spanner_type("jsonb").unwrap().code(),
            TypeCode::Json
        );
        assert_eq!(
            parse_pg_spanner_type("oid").unwrap().code(),
            TypeCode::Int64
        );
        assert_eq!(
            parse_pg_spanner_type("interval").unwrap().code(),
            TypeCode::Interval
        );
        assert_eq!(
            parse_pg_spanner_type("uuid").unwrap().code(),
            TypeCode::Uuid
        );
    }

    #[test]
    fn test_parse_pg_type_annotations() {
        let numeric = raw_type(&parse_pg_spanner_type("numeric").unwrap());
        assert_eq!(
            numeric.type_annotation,
            model::TypeAnnotationCode::PgNumeric
        );

        let decimal = raw_type(&parse_pg_spanner_type("decimal").unwrap());
        assert_eq!(
            decimal.type_annotation,
            model::TypeAnnotationCode::PgNumeric
        );

        let jsonb = raw_type(&parse_pg_spanner_type("jsonb").unwrap());
        assert_eq!(jsonb.type_annotation, model::TypeAnnotationCode::PgJsonb);

        let oid = raw_type(&parse_pg_spanner_type("oid").unwrap());
        assert_eq!(oid.type_annotation, model::TypeAnnotationCode::PgOid);

        let boolean = raw_type(&parse_pg_spanner_type("boolean").unwrap());
        assert_eq!(
            boolean.type_annotation,
            model::TypeAnnotationCode::Unspecified
        );
    }

    #[test]
    fn pg_numeric_maps_to_varchar_to_preserve_special_and_wide_values() {
        let pg_numeric = parse_pg_spanner_type("numeric").unwrap();
        assert!(is_pg_numeric(&pg_numeric));
        assert_eq!(
            spanner_type_to_logical(&pg_numeric).unwrap().id(),
            LogicalTypeId::Varchar
        );

        let googlesql_numeric = parse_spanner_type("NUMERIC").unwrap();
        assert!(!is_pg_numeric(&googlesql_numeric));
        assert_eq!(
            spanner_type_to_logical(&googlesql_numeric).unwrap().id(),
            LogicalTypeId::Decimal
        );
    }

    #[test]
    fn result_type_conversion_rejects_unknown_and_unspecified_codes() {
        for code in [12, 99] {
            let error = spanner_type_to_logical(&type_from_code(TypeCode::Unknown(code)))
                .unwrap_err()
                .to_string();
            assert!(error.contains(&code.to_string()), "{error}");
        }

        let error = spanner_type_to_logical(&type_from_code(TypeCode::Unspecified))
            .unwrap_err()
            .to_string();
        assert!(error.contains("unspecified"), "{error}");
    }

    #[test]
    fn result_type_conversion_rejects_missing_composite_metadata() {
        let array_without_element: Type =
            model::Type::new().set_code(model::TypeCode::Array).into();
        let error = spanner_type_to_logical(&array_without_element)
            .unwrap_err()
            .to_string();
        assert!(error.contains("ARRAY"), "{error}");
        assert!(error.contains("element type"), "{error}");

        let struct_without_metadata: Type =
            model::Type::new().set_code(model::TypeCode::Struct).into();
        let error = spanner_type_to_logical(&struct_without_metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("STRUCT"), "{error}");
        assert!(error.contains("struct metadata"), "{error}");

        let struct_with_missing_field_type =
            struct_type(vec![model::struct_type::Field::new().set_name("payload")]);
        let error = spanner_type_to_logical(&struct_with_missing_field_type)
            .unwrap_err()
            .to_string();
        assert!(error.contains("field 0"), "{error}");
        assert!(error.contains("payload"), "{error}");
        assert!(error.contains("missing type metadata"), "{error}");
    }

    #[test]
    fn result_type_conversion_preserves_nested_metadata_context() {
        let nested = array_type(struct_type(vec![model::struct_type::Field::new()
            .set_name("payload")
            .set_type(model::Type::new().set_code(model::TypeCode::from(99)))]));

        let error = spanner_type_to_logical(&nested).unwrap_err().to_string();
        assert!(error.contains("ARRAY element"), "{error}");
        assert!(error.contains("STRUCT result type field 0"), "{error}");
        assert!(error.contains("payload"), "{error}");
        assert!(error.contains("99"), "{error}");
    }

    #[test]
    fn test_parse_pg_string_types() {
        assert_eq!(
            parse_pg_spanner_type("character varying").unwrap().code(),
            TypeCode::String
        );
        assert_eq!(
            parse_pg_spanner_type("character varying(256)")
                .unwrap()
                .code(),
            TypeCode::String
        );
        assert_eq!(
            parse_pg_spanner_type("varchar").unwrap().code(),
            TypeCode::String
        );
        assert_eq!(
            parse_pg_spanner_type("text").unwrap().code(),
            TypeCode::String
        );
    }

    #[test]
    fn test_parse_pg_bytes() {
        assert_eq!(
            parse_pg_spanner_type("bytea").unwrap().code(),
            TypeCode::Bytes
        );
    }

    #[test]
    fn test_parse_pg_array_types() {
        for input in [
            "bigint[]",
            "integer[]",
            "integer[3]",
            "integer [ 3 ]",
            "int8[0]",
            "bigint ARRAY",
            "integer ARRAY[]",
            "integer ARRAY[4]",
        ] {
            let t = parse_pg_spanner_type(input).unwrap();
            assert_eq!(t.code(), TypeCode::Array, "{input}");
            assert_eq!(
                t.array_element_type().unwrap().code(),
                TypeCode::Int64,
                "{input}"
            );
        }

        let t = parse_pg_spanner_type("character varying(256)[4]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::String);

        let t = parse_pg_spanner_type("character varying[]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::String);

        let t = parse_pg_spanner_type("boolean[]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Bool);

        let t = parse_pg_spanner_type("double precision[]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Float64);

        let t = parse_pg_spanner_type("jsonb[]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Json);

        let t = parse_pg_spanner_type("numeric[]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Numeric);

        let t = parse_pg_spanner_type("oid[]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        let elem = t.array_element_type().unwrap();
        assert_eq!(elem.code(), TypeCode::Int64);
        assert_eq!(
            raw_type(&elem).type_annotation,
            model::TypeAnnotationCode::PgOid
        );

        let t = parse_pg_spanner_type("interval[]").unwrap();
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Interval);

        for input in [
            "interval ARRAY",
            "interval ARRAY[]",
            "interval day ARRAY[3]",
            "interval day to second ARRAY",
        ] {
            let t = parse_pg_spanner_type(input).unwrap();
            assert_eq!(t.code(), TypeCode::Array, "{input}");
            assert_eq!(
                t.array_element_type().unwrap().code(),
                TypeCode::Interval,
                "{input}"
            );
        }
    }

    #[test]
    fn test_parse_pg_vector_arrays() {
        for input in [
            "FLOAT8 [] vector length 0",
            "double precision[] VeCtOr LeNgTh 128",
            "float8 ARRAY[3] vector length 128",
        ] {
            let t = parse_pg_spanner_type(input).unwrap();
            assert_eq!(t.code(), TypeCode::Array);
            assert_eq!(t.array_element_type().unwrap().code(), TypeCode::Float64);
        }
    }

    #[test]
    fn parse_spanner_type_preserves_nested_valid_types() {
        let t = parse_spanner_type(
            "ARRAY<STRUCT<name STRING(MAX), payload ARRAY<PROTO<my.package.Message>>, nested STRUCT<id INT64>, kind ENUM<my.package.Kind>>>",
        )
        .unwrap();
        let element = t.array_element_type().unwrap();
        let struct_type = element.struct_type().unwrap();
        assert_eq!(struct_type.fields.len(), 4);
        assert_eq!(field_code(&struct_type.fields[0]), TypeCode::String);

        let payload: Type = (**struct_type.fields[1].r#type.as_ref().unwrap())
            .clone()
            .into();
        assert_eq!(payload.code(), TypeCode::Array);
        let proto = payload.array_element_type().unwrap();
        assert_eq!(proto.code(), TypeCode::Proto);
        assert_eq!(raw_type(&proto).proto_type_fqn, "my.package.Message");

        assert_eq!(field_code(&struct_type.fields[2]), TypeCode::Struct);
        let kind: Type = (**struct_type.fields[3].r#type.as_ref().unwrap())
            .clone()
            .into();
        assert_eq!(kind.code(), TypeCode::Enum);
        assert_eq!(raw_type(&kind).proto_type_fqn, "my.package.Kind");

        let nested_array = parse_spanner_type("ARRAY<STRUCT<ARRAY<INT64>>>").unwrap();
        assert_eq!(nested_array.code(), TypeCode::Array);
        let nested_struct = nested_array.array_element_type().unwrap();
        assert_eq!(
            field_code(&nested_struct.struct_type().unwrap().fields[0]),
            TypeCode::Array
        );
    }

    #[test]
    fn parse_pg_spanner_type_preserves_valid_annotations_and_arrays() {
        for input in [
            "character varying(256)",
            "character varying(2621440)",
            "varchar(256)",
            "varchar(2621440)",
            "numeric(38, 9)",
            "numeric(147455, 16383)",
            "timestamp(0) with time zone",
            "timestamp(6) with time zone",
            "timestamptz(6)",
            "interval day to second(6)",
            "numeric(38, 9)[]",
        ] {
            parse_pg_spanner_type(input).unwrap_or_else(|error| {
                panic!("expected {input:?} to parse: {error}");
            });
        }
    }

    #[test]
    fn parser_resource_limits_accept_exact_boundaries() {
        let prefix = "STRUCT<`";
        let suffix = "` INT64>";
        let field_name = "a".repeat(MAX_TYPE_INPUT_BYTES - prefix.len() - suffix.len());
        let google_sql = format!("{prefix}{field_name}{suffix}");
        assert_eq!(google_sql.len(), MAX_TYPE_INPUT_BYTES);
        parse_spanner_type(&google_sql).unwrap();

        let pg = format!(
            "bigint{}",
            " ".repeat(MAX_TYPE_INPUT_BYTES - "bigint".len())
        );
        assert_eq!(pg.len(), MAX_TYPE_INPUT_BYTES);
        parse_pg_spanner_type(&pg).unwrap();

        parse_spanner_type(&nested_struct_type(MAX_TYPE_NESTING_DEPTH)).unwrap();

        let fields = struct_type_with_fields(MAX_STRUCT_FIELDS);
        let parsed = parse_spanner_type(&fields).unwrap();
        assert_eq!(
            parsed.struct_type().unwrap().fields.len(),
            MAX_STRUCT_FIELDS
        );
    }

    #[test]
    fn parser_resource_limits_reject_first_over_limit_value() {
        let mut oversized = "é".repeat(MAX_TYPE_INPUT_BYTES / 2);
        oversized.push('é');
        let error = parse_spanner_type(&oversized).unwrap_err();
        assert_eq!(error.position, oversized.len());
        assert!(error.context.contains("maximum is 65536 bytes"));
        assert!(error.type_text.ends_with("...[truncated]"));
        assert!(error.type_text.len() <= MAX_ERROR_INPUT_PREVIEW_BYTES + 14);

        let pg_oversized = format!(
            "bigint{}",
            " ".repeat(MAX_TYPE_INPUT_BYTES - "bigint".len() + 1)
        );
        let error = parse_pg_spanner_type(&pg_oversized).unwrap_err();
        assert!(error.context.contains("maximum is 65536 bytes"));

        let error =
            parse_spanner_type(&nested_struct_type(MAX_TYPE_NESTING_DEPTH + 1)).unwrap_err();
        assert!(error.context.contains("type nesting exceeds the limit"));

        let error =
            parse_spanner_type(&struct_type_with_fields(MAX_STRUCT_FIELDS + 1)).unwrap_err();
        assert!(error
            .context
            .contains("STRUCT field count exceeds the limit"));
    }

    #[test]
    fn parse_spanner_type_rejects_malformed_input() {
        assert_parse_errors(
            parse_spanner_type,
            &[
                ("", "expected a type name"),
                ("UNKNOWN", "unknown GoogleSQL Spanner type name"),
                ("STRING(", "expected MAX or an integer"),
                ("STRING()", "expected MAX or an integer"),
                ("STRING(0)", "STRING length must be between 1 and 2621440"),
                (
                    "STRING(2621441)",
                    "STRING length must be between 1 and 2621440",
                ),
                ("STRING(-1)", "expected MAX or an integer"),
                ("STRING(18446744073709551616)", "integer overflow"),
                ("BYTES(0)", "BYTES length must be between 1 and 10485760"),
                (
                    "BYTES(10485761)",
                    "BYTES length must be between 1 and 10485760",
                ),
                ("BYTES(0x10000000000000000)", "integer overflow"),
                ("STRING(12", "expected ')' after STRING length modifier"),
                ("STRING(12, 2)", "expected ')' after STRING length modifier"),
                ("ARRAY", "expected '<' after ARRAY"),
                ("ARRAY<>", "expected a type name"),
                ("ARRAY<INT64", "expected '>' to close ARRAY"),
                ("ARRAY<INT64, STRING>", "expected '>' to close ARRAY"),
                (
                    "ARRAY<ARRAY<INT64>>",
                    "ARRAY element type cannot be ARRAY directly",
                ),
                ("ARRAY<INT64>>", "unexpected trailing input"),
                (
                    "ARRAY<INT64>(vector_length=>128)",
                    "vector_length is only supported",
                ),
                (
                    "ARRAY<FLOAT32>(vector_length=128)",
                    "expected '=>' after vector_length",
                ),
                (
                    "ARRAY<FLOAT32>(vector_length=>)",
                    "expected a non-negative integer",
                ),
                (
                    "ARRAY<FLOAT32>(vector_length=>128)(vector_length=>64)",
                    "duplicate ARRAY vector_length modifier",
                ),
                ("STRUCT<,>", "expected a STRUCT field name or type"),
                (
                    "STRUCT<`unterminated INT64>",
                    "unterminated quoted STRUCT field name",
                ),
                (
                    r#"STRUCT<`bad\q` INT64>"#,
                    "invalid escape sequence in quoted STRUCT field name",
                ),
                (
                    r#"STRUCT<`bad\12` INT64>"#,
                    "octal escape must contain exactly 3 digits",
                ),
                (
                    r#"STRUCT<`bad\x4` INT64>"#,
                    "hex escape must contain exactly 2 hexadecimal digits",
                ),
                (
                    r#"STRUCT<`bad\u123` INT64>"#,
                    "\\u escape must contain exactly 4 hexadecimal digits",
                ),
                (r#"STRUCT<`bad\uD800` INT64>"#, "surrogate code point"),
                (
                    r#"STRUCT<`bad\U00110000` INT64>"#,
                    "exceeds the maximum code point U+10FFFF",
                ),
                (
                    r#"STRUCT<`bad\uZZZZ` INT64>"#,
                    "\\u escape must contain exactly 4 hexadecimal digits",
                ),
                (
                    "STRUCT<`bad\nname` INT64>",
                    "unescaped newline in quoted STRUCT field name",
                ),
                (r#"STRUCT<`a``b` INT64>"#, "expected a type name"),
                (
                    "STRUCT<`` INT64>",
                    "quoted STRUCT field name cannot be empty",
                ),
                ("STRUCT<name>", "expected a type name"),
                (
                    "STRUCT<name INT64 name2 STRING>",
                    "expected ',' or '>' after STRUCT field",
                ),
                ("STRUCT<name INT64,>", "expected a STRUCT field after ','"),
                ("PROTO", "expected '<' after PROTO"),
                ("PROTO<>", "expected a qualified PROTO or ENUM name"),
                ("PROTO<my..Message>", "expected an identifier after '.'"),
                ("PROTO<my.Message", "expected '>' to close PROTO"),
                ("PROTO<my.Message> junk", "unexpected trailing input"),
                ("INT64 junk", "unexpected trailing input"),
                (
                    "DATETIME",
                    "DATETIME is unsupported because the Spanner data-plane Type API",
                ),
                (
                    "STRUCT<DATETIME>",
                    "DATETIME is unsupported because the Spanner data-plane Type API",
                ),
            ],
        );
    }

    #[test]
    fn parse_pg_spanner_type_rejects_malformed_input() {
        assert_parse_errors(
            parse_pg_spanner_type,
            &[
                ("", "expected a PostgreSQL type name"),
                ("made_up", "unknown PostgreSQL Spanner type name"),
                ("character varying(", "expected a decimal integer"),
                ("numeric(10,)", "expected numeric scale"),
                ("numeric(10,2,1)", "expected ')' after numeric modifier"),
                (
                    "numeric(0)",
                    "numeric precision must be between 1 and 147455",
                ),
                (
                    "numeric(147456)",
                    "numeric precision must be between 1 and 147455",
                ),
                (
                    "numeric(1,16384)",
                    "numeric scale must be between 0 and 16383",
                ),
                ("numeric(18446744073709551616)", "integer overflow"),
                ("varchar(0)", "varchar length must be between 1 and 2621440"),
                (
                    "varchar(2621441)",
                    "varchar length must be between 1 and 2621440",
                ),
                (
                    "timestamp(7) with time zone",
                    "timestamp precision must be between 0 and 6",
                ),
                (
                    "timestamptz(7)",
                    "timestamptz precision must be between 0 and 6",
                ),
                (
                    "interval second(7)",
                    "interval precision must be between 0 and 6",
                ),
                ("varchar(10) junk", "unexpected trailing input"),
                ("bigint[", "expected an array size or ']'"),
                ("bigint[foo]", "expected an array size or ']'"),
                ("integer[-1]", "expected an array size or ']'"),
                ("integer[18446744073709551616]", "integer overflow"),
                ("integer[3", "expected ']' to close PostgreSQL array suffix"),
                ("bigint[] junk", "unexpected trailing input"),
                ("bigint[][]", "unexpected trailing input"),
                ("integer[3][2]", "unexpected trailing input"),
                ("integer ARRAY[3][2]", "unexpected trailing input"),
                (
                    "bigint[] VECTOR LENGTH 128",
                    "VECTOR LENGTH is only supported",
                ),
                (
                    "float4[] VECTOR LENGTH 128",
                    "VECTOR LENGTH is only supported for float8[]",
                ),
                (
                    "real[] VECTOR LENGTH 128",
                    "VECTOR LENGTH is only supported for float8[]",
                ),
                ("float4[] VECTOR 128", "expected 'LENGTH' after 'VECTOR'"),
                (
                    "float4[] VECTOR LENGTH nope",
                    "expected a non-negative integer",
                ),
                (
                    "float8[] VECTOR LENGTH 128 VECTOR LENGTH 64",
                    "unexpected trailing input",
                ),
                (
                    "timestamp without time zone",
                    "expected 'with' after timestamp precision",
                ),
                ("interval fortnight", "unknown interval field qualifier"),
            ],
        );
    }

    fn assert_parse_errors(
        parser: fn(&str) -> Result<Type, TypeParseError>,
        cases: &[(&str, &str)],
    ) {
        for (input, expected_context) in cases {
            let error = parser(input).unwrap_err();
            assert_eq!(error.type_text, *input);
            assert!(error.position <= input.len());
            assert!(
                error.context.contains(expected_context),
                "expected error for {input:?} to contain {expected_context:?}, got {error:?}"
            );
            assert!(error.to_string().contains(&format!("{input:?}")));
        }
    }

    fn nested_struct_type(levels: usize) -> String {
        let mut type_text = "INT64".to_string();
        for _ in 1..levels {
            type_text = format!("STRUCT<field {type_text}>");
        }
        type_text
    }

    fn struct_type_with_fields(field_count: usize) -> String {
        let fields = (0..field_count)
            .map(|index| format!("field_{index} INT64"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("STRUCT<{fields}>")
    }
}
