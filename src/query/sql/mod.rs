pub mod convert;

use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::ArrayFormatter;
use datafusion::arrow::util::display::FormatOptions;
use datafusion::prelude::SessionContext;

use crate::query_proto::{Column, Row};

/// Execute a SQL query and return rows with string-formatted values.
pub async fn execute(ctx: &SessionContext, sql: &str) -> Result<Vec<Row>, String> {
    let (_columns, rows) = execute_with_columns(ctx, sql).await?;
    Ok(rows)
}

/// Execute a SQL query and return both column metadata and rows.
pub async fn execute_with_columns(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(Vec<Column>, Vec<Row>), String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;

    // Build column info from the schema of the result batches
    let columns: Vec<Column> = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| Column {
                name: field.name().clone(),
                data_type: format!("{}", field.data_type()),
            })
            .collect()
    } else {
        vec![]
    };

    let format_options = FormatOptions::default().with_null("NULL");

    // Convert each row of each batch to a Row of string values
    let mut rows = Vec::new();
    for batch in &batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        // Create formatters for each column
        let formatters: Vec<ArrayFormatter> = (0..num_cols)
            .map(|col_idx| {
                ArrayFormatter::try_new(batch.column(col_idx).as_ref(), &format_options)
                    .map_err(|e| format!("failed to create formatter for column {col_idx}: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for row_idx in 0..num_rows {
            let values: Vec<String> = formatters
                .iter()
                .enumerate()
                .map(|(col_idx, fmt)| {
                    if batch.column(col_idx).is_null(row_idx) {
                        "NULL".to_string()
                    } else {
                        fmt.value(row_idx).to_string()
                    }
                })
                .collect();
            rows.push(Row { values });
        }
    }

    Ok((columns, rows))
}
