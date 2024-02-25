use super::{BinaryOperator, Operator};
use crate::execution::record_table::RecordTable;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// TODO docs
pub struct HashJoin {
    _todo: bool,
    children: Vec<Arc<dyn ExecutionPlan>>,
    schema: SchemaRef,
    equate_on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
}

/// TODO docs
impl HashJoin {
    pub(crate) fn new(
        schema: SchemaRef,
        equate_on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    ) -> Self {
        Self {
            _todo: false,
            children: vec![],
            schema,
            equate_on,
        }
    }

    /// Given a [`RecordBatch`]`, hashes based on the input physical expressions
    ///
    /// TODO docs
    fn hash_batch(&self, batch: &RecordBatch) -> Vec<usize> {
        todo!("use self.equate_on to hash each of the tuple keys in the batch")
    }

    /// Builds the Hash Table from the [`RecordBatch`]es coming from the left child.
    ///
    /// TODO docs
    async fn build_table(&self, mut rx_left: broadcast::Receiver<RecordBatch>) -> RecordTable {
        // Take in all of the record batches from the left and create a hash table
        let mut record_table = RecordTable::new(self.schema.clone());

        loop {
            match rx_left.recv().await {
                Ok(batch) => {
                    // TODO gather N batches and use rayon to insert all at once
                    let hashes = self.hash_batch(&batch);
                    record_table.insert_batch(batch, hashes);
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }

        record_table
    }

    /// Given a single batch (coming from the right child), probes the hash table and outputs a
    /// [`RecordBatch`] for every tuple on the right that gets matched with a tuple in the hash table.
    ///
    /// Note: This is super inefficient since its possible that we could emit a bunch of
    /// [`RecordBatch`]es that have just 1 tuple in them. This is a place for easy optimization.
    async fn probe_table(
        &self,
        table: &RecordTable,
        right_batch: RecordBatch,
        tx: &broadcast::Sender<RecordBatch>,
    ) {
        let hashes = self.hash_batch(&right_batch);

        for (right_row, &hash) in hashes.iter().enumerate() {
            // Construct a RecordBatch for each tuple that might get joined with tuples in the hash table

            // For each of these hashes, check if it is in the table
            let Some(records) = table.get_records(hash) else {
                return;
            };
            assert!(!records.is_empty());

            // There are records associated with this hash value, so we need to emit things
            for &record in records {
                let (left_batch, left_row) = table.get(record).unwrap();

                todo!("Join tuples together and then send through `tx`");
            }
        }
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

    fn into_binary(
        self,
    ) -> Arc<dyn BinaryOperator<InLeft = Self::InLeft, InRight = Self::InRight, Out = Self::Out>>
    {
        Arc::new(self)
    }

    async fn execute(
        &self,
        rx_left: broadcast::Receiver<Self::InLeft>,
        mut rx_right: broadcast::Receiver<Self::InRight>,
        tx: broadcast::Sender<Self::Out>,
    ) {
        // Phase 1: Build Phase
        // TODO assign to its own tokio task
        let record_table = self.build_table(rx_left).await;

        // Phase 2: Probe Phase
        loop {
            match rx_right.recv().await {
                Ok(batch) => {
                    self.probe_table(&record_table, batch, &tx).await;
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }
    }
}
