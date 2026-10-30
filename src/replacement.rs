//! Public v2 replacement scan; only unresolved spanner: names are claimed.
use duckdb_neo::{
    Result, ToValue,
    connection::Context,
    extension::Extension,
    qualified_name::QualifiedName,
    replacement_scan::{
        ReplacementHandle, ReplacementScanBuilder, ReplacementScanCallbacks, ReplacementType,
    },
};
struct Replacement;
impl ReplacementScanCallbacks for Replacement {
    fn scan(
        &self,
        ctx: &Context,
        name: &QualifiedName,
        handle: ReplacementHandle<'_>,
    ) -> Result<()> {
        crate::vtab_safety::guard_callback("SpannerReplacement", "scan", || {
            let parts = name.parts()?;
            let Some(table) = parts.last().and_then(|n| n.strip_prefix("spanner:")) else {
                return Ok(());
            };
            if table.is_empty() {
                return Err("spanner: replacement scan requires a table name".into());
            }
            handle.set_reference(ReplacementType::Table(QualifiedName::from_parts(&[
                "spanner_scan",
            ])?))?;
            handle.add_parameter(table.value(ctx)?)?;
            Ok(())
        })
        .map_err(|e| duckdb_neo::Error::invalid_input(e.to_string()))
    }
}
pub(crate) fn register(ext: &Extension) -> Result<()> {
    ReplacementScanBuilder::new(Replacement).register(ext)
}
