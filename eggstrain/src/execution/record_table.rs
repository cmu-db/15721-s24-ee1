use super::record_buffer::{RecordBuffer, RecordIndex};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::collections::HashMap; // TODO replace with a raw table

pub struct RecordTable {
    /// Maps a Hash value to a `RecordIndex` into the `RecordBuffer`
    inner: HashMap<usize, Vec<RecordIndex>>,
    buffer: RecordBuffer,
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

    pub fn insert_batch(&mut self, batch: RecordBatch, hashes: Vec<usize>) {
        assert_eq!(
            batch.num_rows(),
            hashes.len(),
            "There should be an equal amount of batch rows and hashed values"
        );

        // Points to the location of the base of the record batch
        let base_record_id = self.buffer.insert(batch);

        for (row, &hash) in hashes.iter().enumerate() {
            // Given the row, we can create a record id for a specific tuple by updating the row
            // from the base_record_id
            let mut record_id = base_record_id;
            record_id.update_row(row as u32);

            // Insert the record into the hashtable bucket
            self.inner.entry(hash).or_default().push(record_id)
        }
    }

    pub fn get_records(&self, hash: usize) -> Option<&Vec<RecordIndex>> {
        self.inner.get(&hash)
    }

    pub fn get(&self, index: RecordIndex) -> Option<(&RecordBatch, u32)> {
        self.buffer.get(index)
    }
}
