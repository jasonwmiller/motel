use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::store::SharedStore;

use super::arrow_convert;
use super::arrow_schema;

/// A DataFusion TableProvider backed by the in-memory OTLP store.
pub struct OtelTable {
    store: SharedStore,
    table_name: String,
    schema: SchemaRef,
}

impl fmt::Debug for OtelTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtelTable")
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl OtelTable {
    pub fn new(store: SharedStore, table_name: &str) -> Self {
        let schema = match table_name {
            "traces" => arrow_schema::traces_schema(),
            "logs" => arrow_schema::logs_schema(),
            "metrics" => arrow_schema::metrics_schema(),
            _ => panic!("Unknown table: {}", table_name),
        };
        Self {
            store,
            table_name: table_name.to_string(),
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for OtelTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Acquire read lock, convert to RecordBatch, then release lock
        let store = self.store.read().await;
        let batch = match self.table_name.as_str() {
            "traces" => {
                let data: Vec<_> = store.traces.iter().cloned().collect();
                drop(store);
                arrow_convert::resource_spans_to_batch(&data)
            }
            "logs" => {
                let data: Vec<_> = store.logs.iter().cloned().collect();
                drop(store);
                arrow_convert::resource_logs_to_batch(&data)
            }
            "metrics" => {
                let data: Vec<_> = store.metrics.iter().cloned().collect();
                drop(store);
                arrow_convert::resource_metrics_to_batch(&data)
            }
            _ => {
                drop(store);
                Err(format!("Unknown table: {}", self.table_name))
            }
        }
        .map_err(datafusion::error::DataFusionError::Execution)?;

        // Wrap in a MemTable and delegate to its scan()
        let batches = vec![if batch.num_rows() > 0 {
            vec![batch]
        } else {
            vec![]
        }];

        let mem_table = MemTable::try_new(self.schema.clone(), batches)?;
        mem_table.scan(state, projection, filters, limit).await
    }
}
