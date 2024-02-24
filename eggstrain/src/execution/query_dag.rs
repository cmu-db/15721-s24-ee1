use super::operators::project::Project;
use super::operators::UnaryOperator;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{projection::ProjectionExec, ExecutionPlan, Partitioning};
use datafusion_common::{DataFusionError, Result};
use futures::stream::StreamExt;
use std::any::TypeId;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast;

const BATCH_SIZE: usize = 1024;

#[non_exhaustive]
#[derive(Clone)]
pub(crate) enum EggstrainOperator {
    Project(Arc<dyn UnaryOperator<In = RecordBatch, Out = RecordBatch>>),
    // Filter(Arc<dyn UnaryNode>),
    // Sort(Arc<dyn UnaryNode>),
    // Aggregate(Arc<dyn UnaryNode>),
    // TableScan(Arc<dyn UnaryNode>),
    // HashJoin(Arc<dyn BinaryNode>),
}

impl EggstrainOperator {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        match self {
            Self::Project(x) => x.children(),
        }
    }
}

fn extract_df_node(plan: Arc<dyn ExecutionPlan>) -> Result<EggstrainOperator> {
    // Cast the plan as an Any type
    // Match against the type id
    // If it matches a specific one, try to downcast_ref
    // else return an error

    let root = plan.as_any();
    let id = root.type_id();

    if id == TypeId::of::<ProjectionExec>() {
        let projection_plan = root
            .downcast_ref::<ProjectionExec>()
            .expect("Unable to downcast_ref to ProjectionExec");

        let child_schema = projection_plan.children()[0].schema();

        let node = Project::new(child_schema, projection_plan).into_unary();

        Ok(EggstrainOperator::Project(node))
    // } else if id == TypeId::of::<FilterExec>() {
    //     todo!();
    // } else if id == TypeId::of::<HashJoinExec>() {
    //     todo!();
    // } else if id == TypeId::of::<SortExec>() {
    //     todo!();
    // } else if id == TypeId::of::<AggregateExec>() {
    //     todo!();
    } else {
        Err(DataFusionError::NotImplemented(
            "Other operators not implemented".to_string(),
        ))
    }
}

fn df_execute_node(plan: Arc<dyn ExecutionPlan>, tx: broadcast::Sender<RecordBatch>) {
    let partitioning = plan.output_partitioning();
    let partitions = match partitioning {
        Partitioning::RoundRobinBatch(c) => c,
        Partitioning::Hash(_, h) => h,
        Partitioning::UnknownPartitioning(p) => p,
    };

    tokio::spawn(async move {
        for i in 0..partitions {
            let batch_stream = plan.execute(i, Default::default()).unwrap();

            let results = batch_stream.collect::<Vec<_>>().await;

            for batch in results {
                let batch = batch.unwrap();
                if batch.num_rows() == 0 {
                    continue;
                }

                tx.send(batch.clone())
                    .expect("Unable to send rb to project node");
            }
        }
    });
}

pub fn build_query_dag(plan: Arc<dyn ExecutionPlan>) -> Result<broadcast::Receiver<RecordBatch>> {
    let mut queue = VecDeque::new();

    // Final output is going to be sent to root_rx
    let (root_tx, root_rx) = broadcast::channel::<RecordBatch>(BATCH_SIZE);
    // Children of the root will use root_tx to send to the root

    let root = extract_df_node(plan)?;

    queue.push_back((root, root_tx));

    while let Some((node, tx)) = queue.pop_front() {
        for child in node.children() {
            let (child_tx, child_rx) = broadcast::channel::<RecordBatch>(BATCH_SIZE);

            if let Ok(child_node) = extract_df_node(child.clone()) {
                match child_node.clone() {
                    EggstrainOperator::Project(project) => {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            project.execute(child_rx, tx).await;
                        });
                    }
                };
                queue.push_back((child_node, child_tx));
            } else {
                df_execute_node(child.clone(), tx.clone());
            }
        }
    }

    Ok(root_rx)
}
