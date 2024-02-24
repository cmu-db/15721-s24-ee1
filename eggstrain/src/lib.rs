use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub mod execution;
pub mod scheduler_client;
pub mod storage_client;

use execution::query_dag::build_query_dag;

pub async fn run(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    // Parse the execution plan into a DAG of operators
    // where operators are nodes and the edges are broadcasting tokio channels

    let _root = build_query_dag(plan);

    // Once we have the DAG, call .await on the top node and hope that
    // tokio does it job

    todo!()
}
