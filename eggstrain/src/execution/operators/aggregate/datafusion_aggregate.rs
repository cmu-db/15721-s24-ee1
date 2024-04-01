use arrow::array::{downcast_primitive, ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow_schema::Schema;
use datafusion::common::Result;
use datafusion::physical_expr::GroupsAccumulatorAdapter;
use datafusion::physical_plan::AggregateExpr;
use datafusion::physical_plan::{aggregates::PhysicalGroupBy, PhysicalExpr};
use datafusion_expr::GroupsAccumulator;
use std::sync::Arc;

use super::GroupValues;

/// Evaluates expressions against a record batch.
pub(crate) fn evaluate(
    expr: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| {
            expr.evaluate(batch)
                .and_then(|v| v.into_array(batch.num_rows()))
        })
        .collect()
}

/// Evaluates expressions against a record batch.
pub(crate) fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter().map(|expr| evaluate(expr, batch)).collect()
}

pub(crate) fn evaluate_optional(
    expr: &[Option<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Option<ArrayRef>>> {
    expr.iter()
        .map(|expr| {
            expr.as_ref()
                .map(|expr| {
                    expr.evaluate(batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .transpose()
        })
        .collect()
}

/// Evaluate a group by expression against a `RecordBatch`
///
/// Arguments:
/// `group_by`: the expression to evaluate
/// `batch`: the `RecordBatch` to evaluate against
///
/// Returns: A Vec of Vecs of Array of results
/// The outer Vect appears to be for grouping sets
/// The inner Vect contains the results per expression
/// The inner-inner Array contains the results per row
pub(crate) fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    let exprs: Vec<ArrayRef> = group_by
        .expr()
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    let null_exprs: Vec<ArrayRef> = group_by
        .null_expr()
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(group_by
        .groups()
        .iter()
        .map(|group| {
            group
                .iter()
                .enumerate()
                .map(|(idx, is_null)| {
                    if *is_null {
                        null_exprs[idx].clone()
                    } else {
                        exprs[idx].clone()
                    }
                })
                .collect()
        })
        .collect())
}

pub(crate) fn group_schema(schema: &Schema, group_count: usize) -> SchemaRef {
    let group_fields = schema.fields()[0..group_count].to_vec();
    Arc::new(Schema::new(group_fields))
}

/// Create an accumulator for `agg_expr` -- a [`GroupsAccumulator`] if
/// that is supported by the aggregate, or a
/// [`GroupsAccumulatorAdapter`] if not.
pub(crate) fn create_group_accumulator(
    agg_expr: &Arc<dyn AggregateExpr>,
) -> Result<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        // Note in the log when the slow path is used
        //TODO: maybe let's not print here
        println!(
            "Creating GroupsAccumulatorAdapter for {}: {agg_expr:?}",
            agg_expr.name()
        );
        let agg_expr_captured = agg_expr.clone();
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}
