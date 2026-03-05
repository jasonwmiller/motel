use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::store::SharedStore;

use super::table_provider::OtelTable;

/// Create a DataFusion SessionContext with three registered tables:
/// "traces", "logs", and "metrics". The context is meant to be created
/// once per server lifetime and reused across queries.
pub async fn create_context(store: SharedStore) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();

    let traces_table = Arc::new(OtelTable::new(store.clone(), "traces"));
    let logs_table = Arc::new(OtelTable::new(store.clone(), "logs"));
    let metrics_table = Arc::new(OtelTable::new(store, "metrics"));

    ctx.register_table("traces", traces_table)?;
    ctx.register_table("logs", logs_table)?;
    ctx.register_table("metrics", metrics_table)?;

    Ok(ctx)
}
