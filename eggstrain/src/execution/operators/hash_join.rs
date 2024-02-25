use super::{Operator, BinaryOperator};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// TODO docs
pub struct HashJoin {
    pub _todo: bool,
    pub children: Vec<Arc<dyn ExecutionPlan>>,
}

/// TODO docs
impl HashJoin {
    pub fn new() -> Self {
        Self { _todo: false, children: vec![] }
    }
}

/// TODO docs
impl Operator for HashJoin {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.children.clone()
    }
}

/// TODO docs
#[async_trait]
impl BinaryOperator for HashJoin {
    type InLeft = RecordBatch;
    type InRight = RecordBatch;
    type Out = RecordBatch;

    fn into_binary(self) -> Arc<dyn BinaryOperator<InLeft = Self::InLeft, InRight = Self::InRight, Out = Self::Out>> {
        Arc::new(self)
    }

    async fn execute(
        &self,
        rx_left: broadcast::Receiver<Self::InLeft>,
        rx_right: broadcast::Receiver<Self::InRight>,
        tx: broadcast::Sender<Self::Out>,
    ) {
        todo!()
    }
}
