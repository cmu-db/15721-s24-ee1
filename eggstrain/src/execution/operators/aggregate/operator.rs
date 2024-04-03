//! Hash aggregationse crate::BATCH_SIZE;

use crate::BATCH_SIZE;
use std::any::{Any, TypeId};

use crate::execution::operators::{Operator, UnaryOperator};
use arrow::array::AsArray;
use arrow::compute::concat_batches;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::logical_expr::EmitTo;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{AggregateExpr, PhysicalExpr};
use datafusion_common::tree_node::TreeNode;
use datafusion_common::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

// use std::task::{Context, Poll};
use arrow_array::Array;
use std::vec;

use super::*;

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
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        group_by: PhysicalGroupBy,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        output_schema: SchemaRef,
        merged_batch: RecordBatch,
        tx: broadcast::Sender<RecordBatch>,
    ) {
        // poll the stream
        // while its producing
        //  tx.send(stream.poll())

        // Evaluate the grouping expressions
        let group_by_values = evaluate_group_by(&group_by, &merged_batch).unwrap();

        let aggr_expr_ref = &aggr_expr;
        // let aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>> = aggr_expr_ref
        //     .iter()
        //     .map(|agg| {
        //         let mut result = agg.expressions();
        //         // Append ordering requirements to expressions' results. This
        //         // way order sensitive aggregators can satisfy requirement
        //         // themselves.
        //         println!("{:#?}", result);

        //         if let Some(ordering_req) = agg.order_bys() {
        //             result.extend(ordering_req.iter().map(|item| item.expr.clone()));
        //         }
        //         result
        //     })
        //     .collect();

        //TODO: this should not be mode final
        let aggregate_arguments =
            aggregate_expressions(aggr_expr_ref, &AggregateMode::Final, group_by.expr().len()).unwrap();

        let x = aggr_expr_ref[0].name();
        // let y = aggregate_arguments[0][0].name();
        // Evaluate the aggregation expressions.
        let batch_schema = merged_batch.schema().to_string();
        let val1 = merged_batch.columns()[0].data_type().to_string();
        let val2 = merged_batch.columns()[1].data_type().to_string();
        println!("{:#?}", merged_batch.columns());
        let input_values = evaluate_many(&aggregate_arguments, &merged_batch).unwrap();
        println!("{:#?}", input_values);

        // Evaluate the filter expressions, if any, against the inputs
        let filter_values = evaluate_optional(&filter_expr, &merged_batch).unwrap();

        //TODO: maybe this should be input schema not output schema
        let group_schema = group_schema(&Arc::clone(&output_schema), group_by.clone().expr().len());

        let mut group_values_struct = new_group_values(group_schema).unwrap();

        let mut current_group_indices = Default::default();

        let mut accumulators: Vec<_> = aggr_expr
            .clone()
            .iter()
            .map(create_group_accumulator)
            .collect::<Result<_>>()
            .unwrap();

        for group_values in &group_by_values {
            // calculate the group indices for each input row
            let starting_num_groups = group_values_struct.len();
            group_values_struct
                .intern(group_values, &mut current_group_indices)
                .unwrap();
            let group_indices = &current_group_indices;

            let total_num_groups = group_values_struct.len();

            // TODO: I don't think we need to maintain what groups are ordered since it a single stage aggregation
            // // Update ordering information if necessary
            // if total_num_groups > starting_num_groups {
            //     self.group_ordering
            //         .new_groups(group_values, group_indices, total_num_groups)
            //         .unwrap();
            // }

            // Gather the inputs to call the actual accumulator
            let t = accumulators
                .iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            for ((acc, values), opt_filter) in t {
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());
                // Call the appropriate method on each aggregator with
                // the entire input row and the relevant group indexes
                //let name = acc.type_id() == TypeId::of::<SumAccumulator>();
                let name = values[0].data_type().to_string();
                acc.update_batch(values, group_indices, opt_filter, total_num_groups)
                    .unwrap();
            }
        }

        let schema = output_schema.clone();
        if group_values_struct.is_empty() {
            tx.send(RecordBatch::new_empty(schema))
                .expect("Unable to send empty Record Batch for aggregate");
            return;
        }

        let mut output = group_values_struct.emit(EmitTo::All).unwrap();

        // Next output each aggregate value
        for acc in accumulators.iter_mut() {
            output.push(acc.evaluate(EmitTo::All).unwrap())
        }

        // emit reduces the memory usage. Ignore Err from update_memory_reservation. Even if it is
        // over the target memory size after emission, we can emit again rather than returning Err.
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

        let aggregate_expressions = self.aggr_expr.clone();
        let group_by = self.group_by.clone();
        let filter_expr = self.filter_expr.clone();
        let output_schema = self.output_schema.clone();

        //TODO: maybe this *self is a problem
        rayon::spawn(|| {
            Aggregate::aggregate_sync(
                aggregate_expressions,
                group_by,
                filter_expr,
                output_schema,
                merged_batch,
                tx,
            )
        });
    }
}
