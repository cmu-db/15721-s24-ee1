use async_trait::async_trait;
use tokio::sync::broadcast::{Receiver, Sender};

pub mod order_by;
pub mod project;

#[async_trait]
pub(crate) trait UnaryOperator: Send {
    type In;
    type Out;

    fn into_unary(self) -> Box<dyn UnaryOperator<In = Self::In, Out = Self::Out>>;

    async fn execute(&self, rx: Receiver<Self::In>, tx: Sender<Self::Out>);
}

#[async_trait]
pub(crate) trait BinaryOperator: Send {
    type InLeft;
    type InRight;
    type Out;

    fn into_binary(
        self,
    ) -> Box<dyn BinaryOperator<InLeft = Self::InLeft, InRight = Self::InRight, Out = Self::Out>>;

    async fn execute(
        &self,
        rx_left: Receiver<Self::InLeft>,
        rx_right: Receiver<Self::InRight>,
        tx: Sender<Self::Out>,
    );
}
