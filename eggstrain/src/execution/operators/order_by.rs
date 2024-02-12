//! This entire implementation is very wrong since it is taking in a specific type

use super::*;
use async_trait::async_trait;
use std::cmp::{Ordering, PartialOrd};
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::oneshot;

pub struct OrderBy<F, T> {
    pub comparison: F,
    _phantom: PhantomData<T>,
}

impl<F, T> OrderBy<F, T> {
    pub fn new(comparison: F) -> Self {
        Self {
            comparison,
            _phantom: PhantomData,
        }
    }
}

/// TODO figure out proper trait bounds and lifetimes
#[async_trait]
impl<F, T> UnaryOperator for OrderBy<F, T>
where
    T: PartialOrd + Clone + Debug + Send + Sync + 'static,
    F: (Fn(&T, &T) -> Ordering) + Send + Sync + Clone + 'static,
{
    type In = Vec<T>;
    type Out = Vec<T>;

    fn into_unary(self) -> Box<dyn UnaryOperator<In = Self::In, Out = Self::Out>> {
        Box::new(self)
    }

    async fn execute(&self, mut rx: Receiver<Self::In>, tx: Sender<Self::Out>) {
        let mut gather = Vec::new();

        loop {
            match rx.recv().await {
                Ok(mut batch) => {
                    println!("Received {:?}", batch);
                    gather.append(&mut batch);
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }

        let (tx_one, rx_one) = oneshot::channel();
        let comparison = self.comparison.clone();

        println!("Spawning rayon thread now!");

        rayon::spawn(move || {
            println!("Beginning sort!");
            gather.sort_by(comparison);
            tx_one.send(gather).expect("Oneshot Send failed");
        });

        match rx_one.await {
            Ok(sorted) => {
                tx.send(sorted).expect("Send failed");
                println!("Sent sorted vecs");
            }
            Err(_) => todo!(),
        };
    }
}

#[tokio::test]
async fn test_order_by() {
    use tokio::sync::broadcast;

    let comparison = |a: &i32, b: &i32| a.partial_cmp(b).unwrap();
    let operator = OrderBy::new(comparison);
    let operator = operator.into_unary();

    let (tx_in, rx_in) = broadcast::channel(1000);
    let (tx_out, mut rx_out) = broadcast::channel(1000);

    let nums = (0..10).collect::<Vec<_>>();
    let mut multi_nums = vec![nums.clone(), nums.clone(), nums.clone(), nums.clone()];

    tokio::spawn(async move {
        operator.execute(rx_in, tx_out).await;
    });

    while let Some(vec) = multi_nums.pop() {
        tx_in.send(vec).expect("Send vec failed");
    }
    std::mem::drop(tx_in); // notifies that there is nothing left to send

    let sorted_nums = rx_out.recv().await.expect("Receive error from channel");

    println!("Sorted nums: {:?}", sorted_nums);
}
