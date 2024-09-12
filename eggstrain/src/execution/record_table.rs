use super::record_buffer::{RecordBuffer, RecordIndex};
use arrow::row::RowConverter;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::Result;
use std::collections::HashMap; // TODO replace with a raw table

pub struct RecordTable {
    /// Maps a Hash value to a `RecordIndex` into the `RecordBuffer`
    inner: HashMap<u64, Vec<RecordIndex>>,
    pub(crate) buffer: RecordBuffer,
}

impl RecordTable {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            buffer: RecordBuffer::new(schema),
            inner: HashMap::new(),
        }
    }

    pub fn with_capacity(schema: SchemaRef, map_capacity: usize, buffer_capacity: usize) -> Self {
        Self {
            buffer: RecordBuffer::with_capacity(schema, buffer_capacity),
            inner: HashMap::with_capacity(map_capacity),
        }
    }

    pub fn converter(&self) -> &RowConverter {
        self.buffer.converter()
    }

    pub fn insert_batch(&mut self, batch: RecordBatch, hashes: Vec<u64>) -> Result<()> {
        assert_eq!(
            batch.num_rows(),
            hashes.len(),
            "There should be an equal amount of batch rows and hashed values"
        );

        // Points to the location of the base of the record batch
        let base_record_id = self.buffer.insert(batch)?;

        for (row, &hash) in hashes.iter().enumerate() {
            // Insert the record into the hashtable bucket
            self.inner
                .entry(hash)
                .or_default()
                .push(base_record_id.with_row(row as u32))
        }

        Ok(())
    }

    pub fn get_record_indices(&self, hash: u64) -> Option<&Vec<RecordIndex>> {
        self.inner.get(&hash)
    }
}
