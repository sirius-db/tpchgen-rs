use crate::conversions::{decimal128_array_from_iter, string_view_array_from_display_iter};
use crate::{
    ColumnTypeConfig, DEFAULT_BATCH_SIZE, DecimalColumnType, KeyColumnType, RecordBatchIterator,
};
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;
use tpchgen::generators::{SupplierGenerator, SupplierGeneratorIterator};

/// Generate [`Supplier`]s in [`RecordBatch`] format
///
/// [`Supplier`]: tpchgen::generators::Supplier
///
/// # Example:
/// ```
/// # use tpchgen::generators::{SupplierGenerator};
/// # use tpchgen_arrow::SupplierArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = SupplierGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = SupplierArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// let lines = formatted_batches.lines().collect::<Vec<_>>();
/// assert_eq!(lines, vec![
///   "+-----------+--------------------+-------------------------------------+-------------+-----------------+-----------+-----------------------------------------------------------------------------------------------------+", "| s_suppkey | s_name             | s_address                           | s_nationkey | s_phone         | s_acctbal | s_comment                                                                                           |", "+-----------+--------------------+-------------------------------------+-------------+-----------------+-----------+-----------------------------------------------------------------------------------------------------+", "| 1         | Supplier#000000001 |  N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ | 17          | 27-918-335-1736 | 5755.94   | each slyly above the careful                                                                        |", "| 2         | Supplier#000000002 | 89eJ5ksX3ImxJQBvxObC,               | 5           | 15-679-861-2259 | 4032.68   |  slyly bold instructions. idle dependen                                                             |", "| 3         | Supplier#000000003 | q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3   | 1           | 11-383-516-1199 | 4192.40   | blithely silent requests after the express dependencies are sl                                      |", "| 4         | Supplier#000000004 | Bk7ah4CK8SYQTepEmvMkkgMwg           | 15          | 25-843-787-7479 | 4641.08   | riously even requests above the exp                                                                 |", "| 5         | Supplier#000000005 | Gcdm2rJRzl5qlTVzc                   | 11          | 21-151-690-3663 | -283.84   | . slyly regular pinto bea                                                                           |", "| 6         | Supplier#000000006 | tQxuVm7s7CnK                        | 14          | 24-696-997-4969 | 1365.79   | final accounts. regular dolphins use against the furiously ironic decoys.                           |", "| 7         | Supplier#000000007 | s,4TicNGB4uO6PaSqNBUq               | 23          | 33-990-965-2201 | 6820.35   | s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit |", "| 8         | Supplier#000000008 | 9Sq4bBH2FQEmaFOocY45sRTxo6yuoG      | 17          | 27-498-742-3860 | 7627.85   | al pinto beans. asymptotes haggl                                                                    |", "| 9         | Supplier#000000009 | 1KhUgZegwM3ua7dsYmekYBsK            | 10          | 20-403-398-8662 | 5302.37   | s. unusual, even requests along the furiously regular pac                                           |", "| 10        | Supplier#000000010 | Saygah3gYWMp72i PY                  | 24          | 34-852-489-8585 | 3891.91   | ing waters. regular requests ar                                                                     |", "+-----------+--------------------+-------------------------------------+-------------+-----------------+-----------+-----------------------------------------------------------------------------------------------------+"
/// ]);
/// ```
pub struct SupplierArrow {
    inner: SupplierGeneratorIterator<'static>,
    scale_factor: f64,
    batch_size: usize,
    column_type_config: ColumnTypeConfig,
    /// Cached schema based on column_type_config
    schema: SchemaRef,
}

impl SupplierArrow {
    pub fn new(generator: SupplierGenerator<'static>) -> Self {
        let scale_factor = generator.scale_factor();
        let column_type_config = ColumnTypeConfig::default().for_scale_factor(scale_factor);
        let schema = make_supplier_schema(&column_type_config);
        Self {
            inner: generator.iter(),
            scale_factor,
            batch_size: DEFAULT_BATCH_SIZE,
            column_type_config,
            schema,
        }
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for SupplierArrow {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    fn with_column_type_config(mut self, config: ColumnTypeConfig) -> Self {
        let config = config.for_scale_factor(self.scale_factor);
        self.schema = make_supplier_schema(&config);
        self.column_type_config = config;
        self
    }
}

impl Iterator for SupplierArrow {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let s_suppkey: ArrayRef = match self.column_type_config.suppkey_type {
            KeyColumnType::I32 => {
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| {
                    i32::try_from(r.s_suppkey).expect("key exceeds resolved Int32 range")
                })))
            }
            KeyColumnType::I64 => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.s_suppkey as i64),
            )),
        };
        let s_name = string_view_array_from_display_iter(rows.iter().map(|r| r.s_name));
        let s_address = string_view_array_from_display_iter(rows.iter().map(|r| &r.s_address));

        // Build s_nationkey based on config
        let s_nationkey: ArrayRef = match self.column_type_config.nationkey_type {
            KeyColumnType::I32 => Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.s_nationkey as i32),
            )),
            KeyColumnType::I64 => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.s_nationkey as i64),
            )),
        };

        let s_phone = string_view_array_from_display_iter(rows.iter().map(|r| &r.s_phone));

        // Build s_acctbal based on config
        let s_acctbal: ArrayRef = match self.column_type_config.decimal_type {
            DecimalColumnType::F64 => Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.s_acctbal.as_f64()),
            )),
            DecimalColumnType::Decimal128 => {
                Arc::new(decimal128_array_from_iter(rows.iter().map(|r| r.s_acctbal)))
            }
        };

        let s_comment = string_view_array_from_display_iter(rows.iter().map(|r| &r.s_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                s_suppkey,
                Arc::new(s_name),
                Arc::new(s_address),
                s_nationkey,
                Arc::new(s_phone),
                s_acctbal,
                Arc::new(s_comment),
            ],
        )
        .unwrap();
        Some(batch)
    }
}

fn make_supplier_schema(config: &ColumnTypeConfig) -> SchemaRef {
    let suppkey_type = match config.suppkey_type {
        KeyColumnType::I32 => DataType::Int32,
        KeyColumnType::I64 => DataType::Int64,
    };
    let nationkey_type = match config.nationkey_type {
        KeyColumnType::I32 => DataType::Int32,
        KeyColumnType::I64 => DataType::Int64,
    };
    let acctbal_type = match config.decimal_type {
        DecimalColumnType::F64 => DataType::Float64,
        DecimalColumnType::Decimal128 => DataType::Decimal128(15, 2),
    };

    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", suppkey_type, false),
        Field::new("s_name", DataType::Utf8View, false),
        Field::new("s_address", DataType::Utf8View, false),
        Field::new("s_nationkey", nationkey_type, false),
        Field::new("s_phone", DataType::Utf8View, false),
        Field::new("s_acctbal", acctbal_type, false),
        Field::new("s_comment", DataType::Utf8View, false),
    ]))
}
