//! Parquet output format

use crate::statistics::WriteStatistics;
use crate::ParquetVersion;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use log::debug;
use parquet::arrow::arrow_writer::{compute_leaves, ArrowColumnChunk};
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{ColumnPath, SchemaDescPtr};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tpchgen_arrow::RecordBatchIterator;

pub trait IntoSize {
    /// Convert the object into a size
    fn into_size(self) -> Result<usize, io::Error>;
}

/// Converts a set of RecordBatchIterators into a Parquet file
///
/// Uses num_threads to generate the data in parallel
///
/// Note the input is an iterator of [`RecordBatchIterator`]; The batches
/// produced by each iterator is encoded as its own row group.
pub async fn generate_parquet<W: Write + Send + IntoSize + 'static, I>(
    writer: W,
    iter_iter: I,
    num_threads: usize,
    parquet_compression: Compression,
    uncompressed_column_overrides: &[String],
    column_encoding_overrides: &HashMap<String, Encoding>,
    disable_dictionary_encoding_columns: &[String],
    parquet_version: ParquetVersion,
    data_pagesize_limit: usize,
) -> Result<(), io::Error>
where
    I: Iterator<Item: RecordBatchIterator> + 'static,
{
    debug!(
        "Generating Parquet with {num_threads} threads, using {parquet_compression} compression, version {parquet_version}"
    );
    // Based on example in https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowColumnWriter.html
    let mut iter_iter = iter_iter.peekable();

    // get schema from the first iterator
    let Some(first_iter) = iter_iter.peek() else {
        return Ok(()); // no data shrug
    };
    let schema = Arc::clone(first_iter.schema());

    // Compute the parquet schema
    let mut writer_properties_builder = WriterProperties::builder()
        .set_compression(parquet_compression)
        .set_writer_version(parquet_version.to_writer_version())
        .set_data_page_size_limit(data_pagesize_limit)
        .set_data_page_row_count_limit(1 * 1024 * 1024);

    for column in uncompressed_column_overrides {
        writer_properties_builder = writer_properties_builder
            .set_column_compression(ColumnPath::from(column.as_str()), Compression::UNCOMPRESSED);
    }

    // Apply column encoding overrides
    for (column, encoding) in column_encoding_overrides {
        debug!("Setting column {column} encoding to {encoding}");
        writer_properties_builder = writer_properties_builder
            .set_column_encoding(ColumnPath::from(column.as_str()), *encoding);
    }

    // Disable dictionary encoding for specified columns
    for column in disable_dictionary_encoding_columns {
        debug!("Disabling dictionary encoding for column {column}");
        writer_properties_builder = writer_properties_builder
            .set_column_dictionary_enabled(ColumnPath::from(column.as_str()), false);
    }

    let writer_properties = writer_properties_builder.build();
    let writer_properties = Arc::new(writer_properties);
    let parquet_schema = Arc::new(
        ArrowSchemaConverter::new()
            .with_coerce_types(writer_properties.coerce_types())
            .convert(&schema)
            .unwrap(),
    );

    // create a stream that computes the data for each row group
    let mut row_group_stream = futures::stream::iter(iter_iter)
        .map(async |iter| {
            let parquet_schema = Arc::clone(&parquet_schema);
            let writer_properties = Arc::clone(&writer_properties);
            let schema = Arc::clone(&schema);
            // run on a separate thread
            tokio::task::spawn(async move {
                encode_row_group(parquet_schema, writer_properties, schema, iter)
            })
            .await
            .expect("Inner task panicked")
        })
        .buffered(num_threads); // generate row groups in parallel

    let mut statistics = WriteStatistics::new("row groups");

    // A blocking task that writes the row groups to the file
    // done in a blocking task to avoid having a thread waiting on IO
    // Now, read each completed row group and write it to the file
    let root_schema = parquet_schema.root_schema_ptr();
    let writer_properties_captured = Arc::clone(&writer_properties);
    let (tx, mut rx): (
        Sender<Vec<ArrowColumnChunk>>,
        Receiver<Vec<ArrowColumnChunk>>,
    ) = tokio::sync::mpsc::channel(num_threads);
    let writer_task = tokio::task::spawn_blocking(move || {
        // Create parquet writer
        let mut writer =
            SerializedFileWriter::new(writer, root_schema, writer_properties_captured).unwrap();

        while let Some(chunks) = rx.blocking_recv() {
            // Start row group
            let mut row_group_writer = writer.next_row_group().unwrap();

            // Slap the chunks into the row group
            for chunk in chunks {
                chunk.append_to_row_group(&mut row_group_writer).unwrap();
            }
            row_group_writer.close().unwrap();
            statistics.increment_chunks(1);
        }
        let size = writer.into_inner()?.into_size()?;
        statistics.increment_bytes(size);
        Ok(()) as Result<(), io::Error>
    });

    // now, drive the input stream and send results to the writer task
    while let Some(chunks) = row_group_stream.next().await {
        // send the chunks to the writer task
        if let Err(e) = tx.send(chunks).await {
            debug!("Error sending chunks to writer: {e}");
            break; // stop early
        }
    }
    // signal the writer task that we are done
    drop(tx);

    // Wait for the writer task to finish
    writer_task.await??;

    Ok(())
}

/// Creates the data for a particular row group
///
/// Note at the moment it does not use multiple tasks/threads but it could
/// potentially encode multiple columns with different threads .
///
/// Returns an array of [`ArrowColumnChunk`]
fn encode_row_group<I>(
    parquet_schema: SchemaDescPtr,
    writer_properties: Arc<WriterProperties>,
    schema: SchemaRef,
    iter: I,
) -> Vec<ArrowColumnChunk>
where
    I: RecordBatchIterator,
{
    // Create writers for each of the leaf columns
    #[allow(deprecated)]
    let mut col_writers = parquet::arrow::arrow_writer::get_column_writers(
        &parquet_schema,
        &writer_properties,
        &schema,
    )
    .unwrap();

    // generate the data and send it to the tasks (via the sender channels)
    for batch in iter {
        let columns = batch.columns().iter();
        let col_writers = col_writers.iter_mut();
        let fields = schema.fields().iter();

        for ((col_writer, field), arr) in col_writers.zip(fields).zip(columns) {
            for leaves in compute_leaves(field.as_ref(), arr).unwrap() {
                col_writer.write(&leaves).unwrap();
            }
        }
    }
    // finish the writers and create the column chunks
    col_writers
        .into_iter()
        .map(|col_writer| col_writer.close().unwrap())
        .collect()
}
