pub mod bad_dag;

use crate::execution::operators::forward_toy::Forward;
use crate::execution::operators::*;
use bad_dag::*;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::broadcast::{channel, Receiver, Sender};

// All leaf nodes will send these
fn send_numbers(tx: Sender<usize>) {
    tx.send(0).unwrap();
}

/// Right now this function simply instantiates a bunch of `Forward` nodes, which are defined in
/// `lib.rs`.
/// The `Forward` nodes each implement the `execute` call, and it simply adds a specified number
/// to the numbers it sees and then forwards it to the next channel. In the test cases
pub fn build_exec_tree(plan: Arc<BadDag>) -> Receiver<usize> {
    let mut queue = VecDeque::with_capacity(64);

    // Final output is going to be sent to root_rx
    let (root_tx, root_rx) = channel(64);
    // Children of the root will use root_tx to send to the root
    queue.push_back((plan.clone(), root_tx));

    // Do BFS on the DAG, note we don't need to check if we've visited it before since we
    // know the DAG does not contain cycles
    while let Some((node, tx)) = queue.pop_front() {
        match &node.children {
            Children::Zero => {
                send_numbers(tx);
            }
            Children::One(n) => {
                // Create the link between the child and the grandchild
                let (child_tx, child_rx) = channel(64);

                // Store away the grandchild sender for later
                queue.push_back((n.clone(), child_tx));

                tokio::spawn(async move {
                    let exec_node = Forward::new(node.value).into_unary();
                    exec_node.execute(child_rx, tx).await;
                });
            }
            Children::Two(a, b) => {
                let (left_child_tx, left_child_rx) = channel(64);
                let (right_child_tx, right_child_rx) = channel(64);

                queue.push_back((a.clone(), left_child_tx));
                queue.push_back((b.clone(), right_child_tx));

                tokio::spawn(async move {
                    let exec_node = Forward::new(node.value).into_binary();
                    exec_node.execute(left_child_rx, right_child_rx, tx).await;
                });
            }
        }
    }

    root_rx
}

pub async fn execute_plan(plan: Arc<BadDag>) {
    let mut rx = build_exec_tree(plan);
    let mut all_values = vec![];

    // Once the tree is complete, we can start polling
    loop {
        match rx.recv().await {
            Ok(x) => {
                // Returns the number of receiving handles this value is getting sent to
                println!("Received {:16b}", x);
                all_values.push(x);
            }
            Err(e) => match e {
                tokio::sync::broadcast::error::RecvError::Closed => {
                    all_values.sort();
                    println!("Finished everything!\nValues:\n{:?}", all_values);
                    break;
                }
                tokio::sync::broadcast::error::RecvError::Lagged(_) => todo!(),
            },
        }
    }
}

#[tokio::test]
async fn test_simple() {
    // Create a basic tree for data to flow up
    execute_plan(BadDag::basic_tree()).await;
}

#[tokio::test]
async fn test_simple_binary_tree() {
    // Create a basic dag for data to flow up
    execute_plan(BadDag::binary_tree()).await;
}

#[tokio::test]
async fn test_simple_dag() {
    // Create a basic dag for data to flow up
    execute_plan(BadDag::medium_dag()).await;
}
