use super::operators::filter::Filter;
use super::operators::project::Project;
use super::operators::sort::Sort;
use super::operators::{BinaryOperator, UnaryOperator};
use crate::BATCH_SIZE;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_common::{DataFusionError, Result};
use futures::stream::StreamExt;
use std::any::TypeId;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Represents the Operators supported by the `eggstrain` execution engine.
///
/// TODO docs
#[derive(Clone)]
enum EggstrainOperator {
    Project(Arc<dyn UnaryOperator<In = RecordBatch, Out = RecordBatch>>),
    Filter(Arc<dyn UnaryOperator<In = RecordBatch, Out = RecordBatch>>),
    Sort(Arc<dyn UnaryOperator<In = RecordBatch, Out = RecordBatch>>),

    // TODO remove `dead_code` once implemented
    #[allow(dead_code)]
    Aggregate(Arc<dyn UnaryOperator<In = RecordBatch, Out = RecordBatch>>),
    #[allow(dead_code)]
    TableScan(Arc<dyn UnaryOperator<In = RecordBatch, Out = RecordBatch>>),
    #[allow(dead_code)]
    HashJoin(
        Arc<dyn BinaryOperator<InLeft = RecordBatch, InRight = RecordBatch, Out = RecordBatch>>,
    ),
}

impl EggstrainOperator {
    /// Extracts the inner value and calls the `children` method.
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        match self {
            Self::Project(x) => x.children(),
            Self::Filter(x) => x.children(),
            Self::Sort(x) => x.children(),
            _ => unimplemented!(),
        }
    }
}

/// Takes as input a DataFusion `ExecutionPlan` (physical plan), and attempts to parse the root node
/// into an `EggstrainOperator`.
///
/// In order to do this, we make use of the `Any` type to downcast the trait object into a real
/// Rust type.
///
/// TODO docs
fn parse_execution_plan_root(plan: &Arc<dyn ExecutionPlan>) -> Result<EggstrainOperator> {
    let root = plan.as_any();
    let id = root.type_id();

    if id == TypeId::of::<ProjectionExec>() {
        let Some(projection_plan) = root.downcast_ref::<ProjectionExec>() else {
            return Err(DataFusionError::NotImplemented(
                "Unable to downcast DataFusion ExecutionPlan to ProjectionExec".to_string(),
            ));
        };

        let child_schema = projection_plan.children()[0].schema();
        let node = Project::new(child_schema, projection_plan);

        Ok(EggstrainOperator::Project(node.into_unary()))
    } else if id == TypeId::of::<FilterExec>() {
        let Some(filter_plan) = root.downcast_ref::<FilterExec>() else {
            return Err(DataFusionError::NotImplemented(
                "Unable to downcast DataFusion ExecutionPlan to ProjectionExec".to_string(),
            ));
        };

        let predicate = filter_plan.predicate().clone();
        let children = filter_plan.children();
        let node = Filter::new(predicate, children);

        Ok(EggstrainOperator::Filter(node.into_unary()))
    } else if id == TypeId::of::<HashJoinExec>() {
        unimplemented!("HashJoin not implemented");
    } else if id == TypeId::of::<SortExec>() {
        let Some(sort_plan) = root.downcast_ref::<SortExec>() else {
            return Err(DataFusionError::NotImplemented(
                "Unable to downcast DataFusion ExecutionPlan to ProjectionExec".to_string(),
            ));
        };

        let node = Sort::new(sort_plan);

        Ok(EggstrainOperator::Sort(node.into_unary()))
    } else if id == TypeId::of::<AggregateExec>() {
        unimplemented!("Aggregate not implemented");
    } else {
        Err(DataFusionError::NotImplemented(
            "Other operators not implemented".to_string(),
        ))
    }
}

/// Wrapper around DataFusion's `ExecutionPlan::execute` to integrate it with `eggstrain`'s
/// execution architecture.
///
/// TODO docs
fn datafusion_execute(plan: Arc<dyn ExecutionPlan>, tx: broadcast::Sender<RecordBatch>) {
    // DataFusion execution nodes will output multiple streams that are partitioned by the following
    // patterns, so just join them all into one stream
    let partitions = match plan.output_partitioning() {
        Partitioning::RoundRobinBatch(c) => c,
        Partitioning::Hash(_, h) => h,
        Partitioning::UnknownPartitioning(p) => p,
    };

    // In a separate tokio task, send batches to the next operator over the `tx` channel, and make
    // sure to make use of all of the partitions
    tokio::spawn(async move {
        for i in 0..partitions {
            let batch_stream = plan.execute(i, Default::default()).unwrap();

            let results = batch_stream.collect::<Vec<_>>().await;
            for batch in results {
                let batch = batch.unwrap();
                if batch.num_rows() == 0 {
                    continue;
                }

                tx.send(batch).expect("Unable to send rb to project node");
            }
        }
    });
}

/// Sets up a unary operator in the execution DAG.
///
/// TODO docs
fn setup_unary_operator(
    queue: &mut VecDeque<(EggstrainOperator, broadcast::Sender<RecordBatch>)>,
    node: EggstrainOperator,
    tx: broadcast::Sender<RecordBatch>,
) {
    // Defines the edge between operators in the DAG
    let (child_tx, child_rx) = broadcast::channel::<RecordBatch>(BATCH_SIZE);

    // Create the operator's tokio task
    match node.clone() {
        EggstrainOperator::Project(eggnode)
        | EggstrainOperator::Filter(eggnode)
        | EggstrainOperator::Sort(eggnode) => {
            let tx = tx.clone();
            tokio::spawn(async move {
                eggnode.execute(child_rx, tx).await;
            });
        }
        _ => unimplemented!(),
    };

    let child_plan = node.children()[0].clone();

    // If we do not know how to deal with a DataFusion execution plan node, then default to just
    // executing DataFusion for that node
    match parse_execution_plan_root(&child_plan) {
        Ok(val) => {
            queue.push_back((val, child_tx));
        }
        Err(_) => {
            datafusion_execute(child_plan, child_tx);
        }
    }
}

/// Builds the execution DAG.
///
/// Note: Not actually a DAG right now, just a tree.
///
/// TODO docs
pub fn build_execution_dag(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<broadcast::Receiver<RecordBatch>> {
    // If we do not recognize the root node, then we might as well just use DataFusion completely
    let root: EggstrainOperator = parse_execution_plan_root(&plan).map_err(|_| {
        DataFusionError::NotImplemented(
            "The root node of the input physical plan was not recognized".to_string(),
        )
    })?;

    // A queue of tuples containing an execution operator and the `broadtcast:: Sender` channel side
    // to send `RecordBatch`es into that operator
    let mut queue = VecDeque::new();

    // Create the topmost channel, where `root_rx` is where the final outputs will be sent
    let (root_tx, root_rx) = broadcast::channel::<RecordBatch>(BATCH_SIZE);

    // Run BFS on the `ExecutionPlan` and create our own execution DAG
    queue.push_back((root, root_tx));
    while let Some((node, tx)) = queue.pop_front() {
        match node.children().len() {
            0 => unimplemented!(),
            1 => setup_unary_operator(&mut queue, node, tx),
            2 => unimplemented!(),
            n => unreachable!("Nodes should not have more than 2 children, saw {}", n),
        }
    }

    Ok(root_rx)
}
