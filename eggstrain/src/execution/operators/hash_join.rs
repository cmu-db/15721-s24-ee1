use super::{BinaryOperator, Operator};
use crate::execution::record_table::RecordTable;
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// TODO docs
pub struct HashJoin {
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    equate_on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

/// TODO docs
impl HashJoin {
    pub(crate) fn new(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        equate_on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            children,
            left_schema,
            right_schema,
            equate_on,
        }
    }

    /// Given a [`RecordBatch`]`, hashes based on the input physical expressions.
    ///
    /// TODO docs
    fn hash_batch<const LEFT: bool>(&self, batch: &RecordBatch) -> Result<Vec<u64>> {
        let rows = batch.num_rows();

        // A vector of columns, horizontally these are the join keys
        let mut column_vals = Vec::with_capacity(self.equate_on.len());

        for (left_eq, right_eq) in self.equate_on.iter() {
            let eq = if LEFT { left_eq } else { right_eq };

            // Extract a single column
            let col_val = eq.evaluate(batch)?;
            match col_val {
                ColumnarValue::Array(arr) => column_vals.push(arr),
                ColumnarValue::Scalar(s) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Join physical expression scalar condition on {:#?} not implemented",
                        s
                    )));
                }
            }
        }

        let mut hashes = Vec::with_capacity(column_vals.len());
        create_hashes(&column_vals, &Default::default(), &mut hashes)?;
        assert_eq!(hashes.len(), rows);

        Ok(hashes)
    }

    /// Builds the Hash Table from the [`RecordBatch`]es coming from the left child.
    ///
    /// TODO docs
    async fn build_table(
        &self,
        mut rx_left: broadcast::Receiver<RecordBatch>,
    ) -> Result<RecordTable> {
        // Take in all of the record batches from the left and create a hash table
        let mut record_table = RecordTable::new(self.left_schema.clone());

        loop {
            match rx_left.recv().await {
                Ok(batch) => {
                    // TODO gather N batches and use rayon to insert all at once
                    let hashes = self.hash_batch::<true>(&batch)?;
                    record_table.insert_batch(batch, hashes);
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }

        Ok(record_table)
    }

    /// Given a single batch (coming from the right child), probes the hash table and outputs a
    /// [`RecordBatch`] for every tuple on the right that gets matched with a tuple in the hash table.
    ///
    /// Note: This is super inefficient since its possible that we could emit a bunch of
    /// [`RecordBatch`]es that have just 1 tuple in them.
    ///
    /// TODO This is a place for easy optimization.
    ///
    /// TODO only implements an inner join
    async fn probe_table(
        &self,
        table: &RecordTable,
        right_batch: RecordBatch,
        tx: &broadcast::Sender<RecordBatch>,
    ) -> Result<()> {
        let hashes = self.hash_batch::<false>(&right_batch)?;

        let left_column_count = self.left_schema.fields().size();
        let right_column_count = self.right_schema.fields().size();
        let output_columns = left_column_count + right_column_count - self.equate_on.len();

        for (right_row, &hash) in hashes.iter().enumerate() {
            // Construct a RecordBatch for each tuple that might get joined with tuples in the hash table
            let mut out_columns: Vec<(String, ArrayRef)> = Vec::with_capacity(output_columns);

            // For each of these hashes, check if it is in the table
            let Some(records) = table.get_records(hash) else {
                return Ok(());
            };
            assert!(!records.is_empty());

            // There are records associated with this hash value, so we need to emit things
            for &record in records {
                let (left_batch, left_row) = table.get(record).unwrap();

                // Left tuple is in `left_batch` at `left_row` offset
                // Right tuple is in `right_batch` at `right_row` offset
            }

            let joined_batch = RecordBatch::try_from_iter(out_columns)?;

            tx.send(joined_batch)
                .expect("Unable to send the projected batch");
        }

        Ok(())
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
        let record_table = self
            .build_table(rx_left)
            .await
            .expect("Unable to build hash table");

        // Phase 2: Probe Phase
        loop {
            match rx_right.recv().await {
                Ok(batch) => {
                    self.probe_table(&record_table, batch, &tx)
                        .await
                        .expect("Unable to probe hash table");
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }
    }
}
