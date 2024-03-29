//! Hash aggregationse crate::BATCH_SIZE;

use crate::BATCH_SIZE;

use crate::execution::operators::{Operator, UnaryOperator};
use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::logical_expr::EmitTo;
use datafusion::physical_expr::equivalence::ProjectionMapping;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{AggregateExpr, InputOrderMode, PhysicalExpr};
// use datafusion_common::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

// use std::task::{Context, Poll};
use std::vec;

use super::*;


// use crate::aggregates::group_values::{new_group_values, GroupValues};
// use crate::aggregates::order::GroupOrderingFull;
// use crate::aggregates::{
//     evaluate_group_by, evaluate_many, evaluate_optional, group_schema, AggregateMode,
//     PhysicalGroupBy,
// };
// use crate::common::IPCWriter;
// use crate::metrics::{BaselineMetrics, RecordOutput};
// use crate::sorts::sort::{read_spill_as_stream, sort_batch};
// use crate::sorts::streaming_merge;
// use crate::stream::RecordBatchStreamAdapter;
// use crate::{aggregates, ExecutionPlan, PhysicalExpr, BATCH_SIZE};
// use crate::{RecordBatchStream, SendableRecordBatchStream};

// use arrow::array::*;
// use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
// use arrow_schema::SortOptions;
// use datafusion_common::{DataFusionError, Result};
// use datafusion_execution::disk_manager::RefCountedTempFile;
// use datafusion_execution::memory_pool::proxy::VecAllocExt;
// use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
// use datafusion_execution::runtime_env::RuntimeEnv;
// use datafusion_execution::TaskContext;
// use datafusion_expr::{EmitTo, GroupsAccumulator};
// use datafusion_physical_expr::expressions::Column;
// use datafusion_physical_expr::{AggregateExpr, GroupsAccumulatorAdapter, PhysicalSortExpr};

// use futures::ready;
// use futures::stream::{Stream, StreamExt};
// use log::debug;

/// TODO docs
pub(crate) struct Aggregate {
    // sort_expr: Option<Vec<PhysicalSortExpr>>,
    input_schema: SchemaRef, // TODO
    children: Vec<Arc<dyn ExecutionPlan>>,
    limit_size: Option<usize>,
    /// Group by expressions
    group_by: PhysicalGroupBy,
    /// Aggregate expressions
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    /// FILTER (WHERE clause) expression for each aggregate expression
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// Schema after the aggregate is applied
    output_schema: SchemaRef,
    // /// Input schema before any aggregation is applied. For partial aggregate this will be the
    // /// same as input.schema() but for the final aggregate it will be the same as the input
    // /// to the partial aggregate, i.e., partial and final aggregates have same `input_schema`.
    // /// We need the input schema of partial aggregate to be able to deserialize aggregate
    // /// expressions from protobuf for final aggregate.
    // /// The mapping used to normalize expressions like Partitioning and
    // /// PhysicalSortExpr that maps input to output
    // projection_mapping: ProjectionMapping,
    // required_input_ordering: Option<LexRequirement>,
    // /// Describes how the input is ordered relative to the group by columns
    // input_order_mode: InputOrderMode,
    // /// Describe how the output is ordered
    // output_ordering: Option<LexOrdering>,
}

/// TODO docs
impl Aggregate {
    pub(crate) fn new(aggr_plan: &AggregateExec) -> Self {
        Self {
            group_by: aggr_plan.group_by().clone(),
            aggr_expr: Vec::from_iter(aggr_plan.aggr_expr().iter().cloned()),
            filter_expr: Vec::from_iter(aggr_plan.filter_expr().iter().cloned()),
            output_schema: aggr_plan.schema().clone(),
            input_schema: aggr_plan.children()[0].schema(),
            children: aggr_plan.children(),
            limit_size: aggr_plan.limit(),
            // projection_mapping: todo!(),
            // required_input_ordering: todo!(),
            // input_order_mode: todo!(),
            // output_ordering: todo!(),
        }
    }
}

/// TODO docs
impl Operator for Aggregate {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.children.clone()
    }
}

impl Aggregate {
    fn aggregate_sync(
        self: &Arc<Aggregate>,
        merged_batch: RecordBatch,
        tx: broadcast::Sender<RecordBatch>,
    ) {
        // poll the stream
        // while its producing
        //  tx.send(stream.poll())

        // Evaluate the grouping expressions
        let group_by_values = evaluate_group_by(&self.group_by, &merged_batch).unwrap();

        // Evaluate the aggregation expressions.
        let input_values = evaluate_many(&self.aggregate_arguments, &merged_batch).unwrap();

        // Evaluate the filter expressions, if any, against the inputs
        let filter_values = evaluate_optional(&self.filter_expressions, &merged_batch).unwrap();

        for group_values in &group_by_values {
            // calculate the group indices for each input row
            let starting_num_groups = self.group_values.len();
            self.group_values
                .intern(group_values, &mut self.current_group_indices)
                .unwrap();
            let group_indices = &self.current_group_indices;

            // Update ordering information if necessary
            let total_num_groups = self.group_values.len();
            if total_num_groups > starting_num_groups {
                self.group_ordering
                    .new_groups(group_values, group_indices, total_num_groups)
                    .unwrap();
            }

            // Gather the inputs to call the actual accumulator
            let t = self
                .accumulators
                .iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            for ((acc, values), opt_filter) in t {
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());
                // Call the appropriate method on each aggregator with
                // the entire input row and the relevant group indexes
                acc.update_batch(values, group_indices, opt_filter, total_num_groups)
                    .unwrap();
            }
        }

        let schema = self.schema();
        if self.group_values.is_empty() {
            tx.send(RecordBatch::new_empty(schema));
            return;
        }

        let mut output = self.group_values.emit(EmitTo::All).unwrap();

        // Next output each aggregate value
        for acc in self.accumulators.iter_mut() {
            output.push(acc.evaluate(EmitTo::All).unwrap())
        }

        // emit reduces the memory usage. Ignore Err from update_memory_reservation. Even if it is
        // over the target memory size after emission, we can emit again rather than returning Err.
        let _ = self.update_memory_reservation();
        let output_batch = RecordBatch::try_new(schema, output).unwrap();

        let mut current = 0;
        let total_rows = output_batch.num_rows();
        while current + BATCH_SIZE < total_rows {
            let batch_to_send = output_batch.slice(current, BATCH_SIZE);
            tx.send(batch_to_send)
                .expect("Unable to send the aggregated batch");
            current += BATCH_SIZE;
        }
        let batch_to_send = output_batch.slice(current, total_rows - current);
        tx.send(batch_to_send)
            .expect("Unable to send the last aggregated batch");
    }
}

/// TODO docs
#[async_trait]
impl UnaryOperator for Aggregate {
    type In = RecordBatch;
    type Out = RecordBatch;

    fn into_unary(self) -> Arc<dyn UnaryOperator<In = Self::In, Out = Self::Out>> {
        Arc::new(self)
    }

    async fn execute(
        &self,
        mut rx: broadcast::Receiver<Self::In>,
        tx: broadcast::Sender<Self::Out>,
    ) {
        let mut batches = vec![];
        loop {
            match rx.recv().await {
                Ok(batch) => {
                    batches.push(batch);
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }

        let merged_batch = concat_batches(&self.input_schema, &batches).unwrap();
        let limit_size = self.limit_size;

        rayon::spawn(|| Aggregate::aggregate_sync(Arc::new(&self), merged_batch, tx));
    }
}
