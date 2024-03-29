use super::{Operator, UnaryOperator};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::cast::as_boolean_array;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_common::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// TODO docs
pub(crate) struct Filter {
    predicate: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

/// TODO docs
impl Filter {
    pub(crate) fn new(
        predicate: Arc<dyn PhysicalExpr>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            predicate,
            children,
        }
    }

    /// Taken from DataFusion's `FilterExec`
    /// [code](https://docs.rs/datafusion-physical-plan/36.0.0/src/datafusion_physical_plan/filter.rs.html#307)
    fn batch_filter(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.predicate
            .evaluate(&batch)
            .and_then(|v| v.into_array(batch.num_rows()))
            .and_then(|array| {
                Ok(as_boolean_array(&array)?)
                    // apply filter array to record batch
                    .and_then(|filter_array| Ok(filter_record_batch(&batch, filter_array)?))
            })
    }
}

/// TODO docs
impl Operator for Filter {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.children.clone()
    }
}

/// TODO docs
#[async_trait]
impl UnaryOperator for Filter {
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
                    let filtered_batch = self
                        .batch_filter(batch)
                        .expect("Filter::batch_filter() fails");

                    if filtered_batch.num_rows() > 0 {
                        tx.send(filtered_batch).expect("tx.send() fails");
                    }
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }
    }
}
