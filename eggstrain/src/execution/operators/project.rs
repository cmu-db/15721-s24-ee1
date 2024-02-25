use super::{Operator, UnaryOperator};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::{projection::ProjectionExec, ExecutionPlan};
use datafusion_common::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// TODO docs
pub(crate) struct Project {
    output_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    input_schema: SchemaRef, // TODO
    children: Vec<Arc<dyn ExecutionPlan>>,
}

/// TODO docs
impl Project {
    pub(crate) fn new(input_schema: SchemaRef, projection_plan: &ProjectionExec) -> Self {
        Self {
            output_expr: Vec::from(projection_plan.expr()),
            input_schema,
            children: projection_plan.children(),
        }
    }

    fn apply_projection(&self, rb: RecordBatch) -> Result<RecordBatch> {
        assert_eq!(rb.schema(), self.input_schema);

        let num_rows = rb.num_rows();

        let mut columns = Vec::with_capacity(self.output_expr.len());

        for (expr, name) in &self.output_expr {
            let col_val = expr.evaluate(&rb).expect("expr.evaluate() fails");
            let column = col_val.into_array(num_rows)?;
            columns.push((name, column, expr.nullable(&self.input_schema)?));
        }

        Ok(RecordBatch::try_from_iter_with_nullable(columns)?)
    }
}

/// TODO docs
impl Operator for Project {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.children.clone()
    }
}

/// TODO docs
#[async_trait]
impl UnaryOperator for Project {
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
        loop {
            match rx.recv().await {
                Ok(batch) => {
                    let projected_batch = self
                        .apply_projection(batch)
                        .expect("Unable to apply projection");

                    tx.send(projected_batch)
                        .expect("Unable to send the projected batch");
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }
    }
}
