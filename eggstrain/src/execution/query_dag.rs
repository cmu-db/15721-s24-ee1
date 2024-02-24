use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{projection::ProjectionExec, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_common::Result;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast::channel;

const BATCH_SIZE: usize = 1024;

#[non_exhaustive]
enum ExecutionPlanNode {
    Project(ProjectionExec),
    // Filter(FilterExec),
    // HashJoin(HashJoinExec),
    // Sort(SortExec),
    // Aggregate(AggregateExec),
    // TableScan(TableScanExec),
}

fn extract_physical_node(plan: Arc<dyn ExecutionPlan>) -> Result<ExecutionPlanNode> {
    plan.as_any()
        .downcast_ref::<ProjectionExec>()
        .expect("Unable to downcast_ref to ProjectionExec")
        .clone();
    todo!()
}

pub fn build_query_dag(plan: Arc<dyn ExecutionPlan>) {
    // let mut queue = VecDeque::new();

    // // Final output is going to be sent to root_rx
    // let (root_tx, root_rx) = channel(BATCH_SIZE);
    // // Children of the root will use root_tx to send to the root
    // queue.push_back((plan.clone(), root_tx));

    // // Do BFS on the DAG, note we don't need to check if we've visited it before since we
    // // know the DAG does not contain cycles
    // while let Some((node, tx)) = queue.pop_front() {
    //     match &node.children {
    //         Children::Zero => {
    //             send_numbers(tx);
    //         }
    //         Children::One(n) => {
    //             // Create the link between the child and the grandchild
    //             let (child_tx, child_rx) = channel(64);

    //             // Store away the grandchild sender for later
    //             queue.push_back((n.clone(), child_tx));

    //             tokio::spawn(async move {
    //                 let exec_node = Forward::new(node.value).into_unary();
    //                 exec_node.execute(child_rx, tx).await;
    //             });
    //         }
    //         Children::Two(a, b) => {
    //             let (left_child_tx, left_child_rx) = channel(64);
    //             let (right_child_tx, right_child_rx) = channel(64);

    //             queue.push_back((a.clone(), left_child_tx));
    //             queue.push_back((b.clone(), right_child_tx));

    //             tokio::spawn(async move {
    //                 let exec_node = Forward::new(node.value).into_binary();
    //                 exec_node.execute(left_child_rx, right_child_rx, tx).await;
    //             });
    //         }
    //     }
    // }

    // root_rx

    todo!()
}
