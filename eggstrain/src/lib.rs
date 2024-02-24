use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::Result;
use std::io;
use std::sync::Arc;

pub mod execution;
pub mod scheduler_client;
pub mod storage_client;

use execution::query_dag::build_query_dag;

pub async fn tpch_dataframe() -> Result<DataFrame> {
    let ctx = SessionContext::new();

    let tables = [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ];

    for table_name in tables {
        ctx.register_csv(
            table_name,
            &format!("../data/{}.csv", table_name),
            CsvReadOptions::new().delimiter(b'|'),
        )
        .await?;
    }

    let stdin = io::read_to_string(io::stdin())?;

    ctx.sql(&stdin).await
}

pub async fn run(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    // Parse the execution plan into a DAG of operators
    // where operators are nodes and the edges are broadcasting tokio channels

    let mut root_rx = build_query_dag(plan).unwrap();

    // Once we have the DAG, call .await on the top node and hope that
    // tokio does it job

    let mut all_values = vec![];

    while let Ok(x) = root_rx.recv().await {
        all_values.push(x);
    }

    all_values
}
