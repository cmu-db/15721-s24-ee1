// use super::*;
use async_trait::async_trait;
use crate::execution::operators::*;
use tokio::sync::broadcast::{Receiver, Sender};

pub struct Forward {
    pub prime: usize,
}

impl Forward {
    pub fn new(prime: usize) -> Self {
        Self { prime }
    }

    // Simply multiples the number by what the prime is and then forwards to the sender
    async fn add_and_forward(prime: usize, rx: &mut Receiver<usize>, tx: &Sender<usize>) {
        loop {
            match rx.recv().await {
                Ok(x) => {
                    // Returns the number of receiving handles this value is getting sent to
                    let _ = tx.send(x + prime).expect("Receiver was somehow dropped");
                }
                Err(e) => match e {
                    tokio::sync::broadcast::error::RecvError::Closed => return,
                    tokio::sync::broadcast::error::RecvError::Lagged(_) => todo!(),
                },
            }
        }
    }
}

#[async_trait]
impl UnaryOperator for Forward {
    type In = usize;
    type Out = usize;

    fn into_unary(self) -> Box<dyn UnaryOperator<In = Self::In, Out = Self::Out>> {
        Box::new(self)
    }

    async fn execute(&self, mut rx: Receiver<Self::In>, tx: Sender<Self::Out>) {
        Forward::add_and_forward(self.prime, &mut rx, &tx).await
    }
}

#[async_trait]
impl BinaryOperator for Forward {
    type InLeft = usize;
    type InRight = usize;
    type Out = usize;

    fn into_binary(
        self,
    ) -> Box<dyn BinaryOperator<InLeft = Self::InLeft, InRight = Self::InRight, Out = Self::Out>>
    {
        Box::new(self)
    }

    // Have both children send to the same place, in whatever order they come in
    async fn execute(
        &self,
        mut rx_left: Receiver<Self::InLeft>,
        mut rx_right: Receiver<Self::InRight>,
        tx: Sender<Self::Out>,
    ) {
        let prime = self.prime;
        let tx1 = tx.clone();
        let tx2 = tx;

        tokio::spawn(async move {
            Forward::add_and_forward(prime, &mut rx_left, &tx1).await;
        });

        tokio::spawn(async move {
            Forward::add_and_forward(prime, &mut rx_right, &tx2).await;
        });
    }
}
