use async_trait::async_trait;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};

pub mod filter;
pub mod project;

pub trait Operator {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;
}

#[async_trait]
pub(crate) trait UnaryOperator: Operator + Send + Sync {
    type In;
    type Out;

    fn into_unary(self) -> Arc<dyn UnaryOperator<In = Self::In, Out = Self::Out>>;

    async fn execute(&self, rx: Receiver<Self::In>, tx: Sender<Self::Out>);
}

#[async_trait]
pub(crate) trait BinaryOperator: Operator + Send + Sync {
    type InLeft;
    type InRight;
    type Out;

    fn into_binary(
        self,
    ) -> Arc<dyn BinaryOperator<InLeft = Self::InLeft, InRight = Self::InRight, Out = Self::Out>>;

    async fn execute(
        &self,
        rx_left: Receiver<Self::InLeft>,
        rx_right: Receiver<Self::InRight>,
        tx: Sender<Self::Out>,
    );
}
