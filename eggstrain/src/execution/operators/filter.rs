use super::{Operator, UnaryOperator};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct Filter {
    pub children: Vec<Arc<dyn ExecutionPlan>>,
}

impl Operator for Filter {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.children.clone()
    }
}

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
        todo!()
    }
}
