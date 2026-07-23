use std::any::Any;
use std::error::Error;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};

use crate::error::SpannerError;

type CallbackResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct CallbackError(String);

impl fmt::Display for CallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for CallbackError {}

/// Run a DuckDB VTab callback without allowing a Rust panic or an interior NUL
/// in an error message to reach duckdb-rs's `extern "C"` callback wrappers.
pub(crate) fn guard_callback<T>(
    vtab_name: &'static str,
    stage: &'static str,
    callback: impl FnOnce() -> CallbackResult<T>,
) -> CallbackResult<T> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        callback().map_err(|error| CallbackError(sanitize_error_message(&error.to_string())))
    }));

    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Box::new(error)),
        Err(payload) => {
            let message = panic_payload_message(payload.as_ref());
            Err(Box::new(CallbackError(format!(
                "{vtab_name} {stage} callback panicked: {}",
                sanitize_error_message(message)
            ))))
        }
    }
}

/// Validate an externally supplied result name before passing it to a
/// duckdb-rs API that converts the name with `CString::new(...).unwrap()`.
pub(crate) fn validate_result_name(name: &str, context: &str) -> Result<(), SpannerError> {
    let Some(byte_index) = name.as_bytes().iter().position(|byte| *byte == 0) else {
        return Ok(());
    };

    Err(SpannerError::Other(format!(
        "{context} name contains an interior NUL at byte {byte_index}: {name:?}"
    )))
}

fn sanitize_error_message(message: &str) -> String {
    message.replace('\0', "\\0")
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else {
        "non-string panic payload"
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
    use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
    use duckdb::Connection;

    use super::*;

    const BIND_PANIC: u8 = 1;
    const INIT_PANIC: u8 = 2;
    const FUNC_PANIC: u8 = 3;
    const BIND_NUL_ERROR: u8 = 4;
    const BIND_NUL_NAME: u8 = 5;
    const UPSTREAM_BIND_PANIC: u8 = 6;

    struct FailingVTab<const FAILURE: u8>;

    impl<const FAILURE: u8> VTab for FailingVTab<FAILURE> {
        type BindData = ();
        type InitData = ();

        fn bind(bind: &BindInfo) -> CallbackResult<Self::BindData> {
            if FAILURE == UPSTREAM_BIND_PANIC {
                panic!("unwrapped table bind callback panic");
            }
            guard_callback("FailingVTab", "bind", || {
                if FAILURE == BIND_PANIC {
                    panic!("deliberate bind panic");
                }
                if FAILURE == BIND_NUL_ERROR {
                    return Err(io::Error::other("bind error\0with NUL").into());
                }
                if FAILURE == BIND_NUL_NAME {
                    let name = "value\0name";
                    validate_result_name(name, "query result column 0")?;
                    bind.add_result_column(name, LogicalTypeHandle::from(LogicalTypeId::Boolean));
                }
                bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Boolean));
                Ok(())
            })
        }

        fn init(_: &InitInfo) -> CallbackResult<Self::InitData> {
            guard_callback("FailingVTab", "init", || {
                if FAILURE == INIT_PANIC {
                    panic!("deliberate init panic");
                }
                Ok(())
            })
        }

        fn func(_: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> CallbackResult<()> {
            guard_callback("FailingVTab", "func", || {
                if FAILURE == FUNC_PANIC {
                    panic!("deliberate func panic");
                }
                output.set_len(0);
                Ok(())
            })
        }
    }

    fn query_error<const FAILURE: u8>(name: &str) -> String {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .register_table_function::<FailingVTab<FAILURE>>(name)
            .unwrap();
        connection
            .execute_batch(&format!("SELECT * FROM {name}()"))
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn callback_panics_become_duckdb_errors() {
        for error in [
            query_error::<BIND_PANIC>("bind_panic"),
            query_error::<INIT_PANIC>("init_panic"),
            query_error::<FUNC_PANIC>("func_panic"),
        ] {
            assert!(error.contains("callback panicked"), "{error}");
            assert!(error.contains("deliberate"), "{error}");
        }
    }

    #[test]
    fn duckdb_rs_contains_unwrapped_callback_panics() {
        let error = query_error::<UPSTREAM_BIND_PANIC>("upstream_bind_panic");
        assert!(
            error.contains("unwrapped table bind callback panic"),
            "{error}"
        );
    }

    #[test]
    fn callback_errors_escape_interior_nul_before_duckdb_set_error() {
        let error = query_error::<BIND_NUL_ERROR>("nul_error");
        assert!(error.contains(r"bind error\0with NUL"), "{error}");
        assert!(!error.contains('\0'), "{error:?}");
    }

    #[test]
    fn result_names_reject_interior_nul() {
        let error = query_error::<BIND_NUL_NAME>("nul_name");
        assert!(error.contains("query result column 0"), "{error}");
        assert!(error.contains("byte 5"), "{error}");
        assert!(!error.contains('\0'), "{error:?}");
    }
}
