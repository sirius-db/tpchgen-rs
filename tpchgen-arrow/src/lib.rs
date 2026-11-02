//! Generate TPCH data as Arrow RecordBatches
//!
//! This crate provides generators for TPCH tables that directly produces
//! Arrow [`RecordBatch`]es. This is significantly faster than generating TBL or CSV
//! files and then parsing them into Arrow.
//!
//! # Example
//! ```
//! # use tpchgen::generators::LineItemGenerator;
//! # use tpchgen_arrow::LineItemArrow;
//! # use arrow::util::pretty::pretty_format_batches;
//! // Create a SF=1 generator for the LineItem table
//! let generator = LineItemGenerator::new(1.0, 1, 1);
//! let mut arrow_generator = LineItemArrow::new(generator)
//!   .with_batch_size(10);
//! // The generator is a Rust iterator, producing RecordBatch
//! let batch = arrow_generator.next().unwrap();
//! // compare the output by pretty printing it
//! let formatted_batches = pretty_format_batches(&[batch]).unwrap().to_string();
//! assert_eq!(formatted_batches.lines().collect::<Vec<_>>(), vec![
//!   "+------------+-----------+-----------+--------------+------------+-----------------+------------+-------+--------------+--------------+------------+--------------+---------------+-------------------+------------+-------------------------------------+",
//!   "| l_orderkey | l_partkey | l_suppkey | l_linenumber | l_quantity | l_extendedprice | l_discount | l_tax | l_returnflag | l_linestatus | l_shipdate | l_commitdate | l_receiptdate | l_shipinstruct    | l_shipmode | l_comment                           |",
//!   "+------------+-----------+-----------+--------------+------------+-----------------+------------+-------+--------------+--------------+------------+--------------+---------------+-------------------+------------+-------------------------------------+",
//!   "| 1          | 155190    | 7706      | 1            | 17.00      | 21168.23        | 0.04       | 0.02  | N            | O            | 1996-03-13 | 1996-02-12   | 1996-03-22    | DELIVER IN PERSON | TRUCK      | egular courts above the             |",
//!   "| 1          | 67310     | 7311      | 2            | 36.00      | 45983.16        | 0.09       | 0.06  | N            | O            | 1996-04-12 | 1996-02-28   | 1996-04-20    | TAKE BACK RETURN  | MAIL       | ly final dependencies: slyly bold   |",
//!   "| 1          | 63700     | 3701      | 3            | 8.00       | 13309.60        | 0.10       | 0.02  | N            | O            | 1996-01-29 | 1996-03-05   | 1996-01-31    | TAKE BACK RETURN  | REG AIR    | riously. regular, express dep       |",
//!   "| 1          | 2132      | 4633      | 4            | 28.00      | 28955.64        | 0.09       | 0.06  | N            | O            | 1996-04-21 | 1996-03-30   | 1996-05-16    | NONE              | AIR        | lites. fluffily even de             |",
//!   "| 1          | 24027     | 1534      | 5            | 24.00      | 22824.48        | 0.10       | 0.04  | N            | O            | 1996-03-30 | 1996-03-14   | 1996-04-01    | NONE              | FOB        |  pending foxes. slyly re            |",
//!   "| 1          | 15635     | 638       | 6            | 32.00      | 49620.16        | 0.07       | 0.02  | N            | O            | 1996-01-30 | 1996-02-07   | 1996-02-03    | DELIVER IN PERSON | MAIL       | arefully slyly ex                   |",
//!   "| 2          | 106170    | 1191      | 1            | 38.00      | 44694.46        | 0.00       | 0.05  | N            | O            | 1997-01-28 | 1997-01-14   | 1997-02-02    | TAKE BACK RETURN  | RAIL       | ven requests. deposits breach a     |",
//!   "| 3          | 4297      | 1798      | 1            | 45.00      | 54058.05        | 0.06       | 0.00  | R            | F            | 1994-02-02 | 1994-01-04   | 1994-02-23    | NONE              | AIR        | ongside of the furiously brave acco |",
//!   "| 3          | 19036     | 6540      | 2            | 49.00      | 46796.47        | 0.10       | 0.00  | R            | F            | 1993-11-09 | 1993-12-20   | 1993-11-24    | TAKE BACK RETURN  | RAIL       |  unusual accounts. eve              |",
//!   "| 3          | 128449    | 3474      | 3            | 27.00      | 39890.88        | 0.06       | 0.07  | A            | F            | 1994-01-16 | 1993-11-22   | 1994-01-23    | DELIVER IN PERSON | SHIP       | nal foxes wake.                     |",
//!   "+------------+-----------+-----------+--------------+------------+-----------------+------------+-------+--------------+--------------+------------+--------------+---------------+-------------------+------------+-------------------------------------+"
//! ]);
//! ```

use std::fmt;
use std::str::FromStr;

pub mod conversions;
mod customer;
mod lineitem;
mod nation;
mod order;
mod part;
mod partsupp;
mod region;
mod supplier;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
pub use customer::CustomerArrow;
pub use lineitem::LineItemArrow;
pub use nation::NationArrow;
pub use order::OrderArrow;
pub use part::PartArrow;
pub use partsupp::PartSuppArrow;
pub use region::RegionArrow;
pub use supplier::SupplierArrow;

/// The default number of rows in each Batch
pub const DEFAULT_BATCH_SIZE: usize = 8_000;

/// Type to use for decimal/monetary columns.
///
/// Controls the Arrow type for columns like c_acctbal, l_quantity, l_extendedprice,
/// l_discount, l_tax, o_totalprice, p_retailprice, ps_supplycost, s_acctbal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DecimalColumnType {
    #[default]
    Decimal128,
    F64,
}

impl fmt::Display for DecimalColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecimalColumnType::Decimal128 => write!(f, "decimal128"),
            DecimalColumnType::F64 => write!(f, "f64"),
        }
    }
}

impl FromStr for DecimalColumnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "decimal128" => Ok(DecimalColumnType::Decimal128),
            "f64" => Ok(DecimalColumnType::F64),
            _ => Err(format!(
                "Invalid decimal column type: '{}'. Valid values are: decimal128, f64",
                s
            )),
        }
    }
}

/// Type to use for date columns.
///
/// Controls the Arrow type for columns like l_shipdate, l_commitdate, l_receiptdate, o_orderdate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DateColumnType {
    /// Use Date32 for date columns (default)
    #[default]
    Date32,
    /// Use Timestamp(Millisecond, None) for date columns
    TimestampMs,
}

impl fmt::Display for DateColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateColumnType::Date32 => write!(f, "date32"),
            DateColumnType::TimestampMs => write!(f, "timestamp_ms"),
        }
    }
}

impl FromStr for DateColumnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "date32" => Ok(DateColumnType::Date32),
            "timestamp_ms" => Ok(DateColumnType::TimestampMs),
            _ => Err(format!(
                "Invalid date column type: '{}'. Valid values are: date32, timestamp_ms",
                s
            )),
        }
    }
}

/// Type to use for key columns.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyColumnType {
    /// Prefer Int32, automatically widening to Int64 if the key range requires it (default)
    #[default]
    I32,
    /// Use Int64 for key columns
    I64,
}

impl fmt::Display for KeyColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyColumnType::I64 => write!(f, "i64"),
            KeyColumnType::I32 => write!(f, "i32"),
        }
    }
}

impl FromStr for KeyColumnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "i64" => Ok(KeyColumnType::I64),
            "i32" => Ok(KeyColumnType::I32),
            _ => Err(format!(
                "Invalid key column type: '{}'. Valid values are: i64, i32",
                s
            )),
        }
    }
}

/// Configuration for column types in generated Arrow data.
///
/// This allows customizing the Arrow types used for specific column categories:
/// - Decimal columns (monetary values like prices, account balances)
/// - Date columns (order dates, ship dates, etc.)
/// - Key columns (primary and foreign keys)
///
/// By default key columns use Int32 when their full key range fits, and Int64
/// otherwise. The choice uses the dataset's scale factor, so primary and foreign
/// keys have matching types across tables, batches, and generator partitions.
/// Explicit I32 requests also widen when necessary; I64 requests stay Int64.
///
/// # Example
/// ```
/// use tpchgen_arrow::{ColumnTypeConfig, DateColumnType, DecimalColumnType, KeyColumnType};
///
/// let config = ColumnTypeConfig {
///     decimal_type: DecimalColumnType::F64,
///     date_type: DateColumnType::TimestampMs,
///     nationkey_type: KeyColumnType::I32,
///     regionkey_type: KeyColumnType::I32,
///     custkey_type: KeyColumnType::I32,
///     partkey_type: KeyColumnType::I32,
///     suppkey_type: KeyColumnType::I32,
///     orderkey_type: KeyColumnType::I32,
/// };
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ColumnTypeConfig {
    /// Type for decimal/monetary columns
    pub decimal_type: DecimalColumnType,
    /// Type for date columns (l_shipdate, l_commitdate, l_receiptdate, o_orderdate)
    pub date_type: DateColumnType,
    /// Type for nationkey columns (c_nationkey, n_nationkey, s_nationkey)
    pub nationkey_type: KeyColumnType,
    /// Type for regionkey columns (n_regionkey, r_regionkey)
    pub regionkey_type: KeyColumnType,
    /// Type for custkey columns (c_custkey, o_custkey)
    pub custkey_type: KeyColumnType,
    /// Type for partkey columns (p_partkey, ps_partkey, l_partkey)
    pub partkey_type: KeyColumnType,
    /// Type for suppkey columns (s_suppkey, ps_suppkey, l_suppkey)
    pub suppkey_type: KeyColumnType,
    /// Type for orderkey columns (o_orderkey, l_orderkey)
    pub orderkey_type: KeyColumnType,
}

impl ColumnTypeConfig {
    /// Resolve scalable keys against the entire dataset, not a single partition.
    fn for_scale_factor(mut self, scale_factor: f64) -> Self {
        use tpchgen::generators::{
            CustomerGenerator, OrderGenerator, PartGenerator, SupplierGenerator,
        };

        if CustomerGenerator::calculate_row_count(scale_factor, 1, 1) > i32::MAX as i64 {
            self.custkey_type = KeyColumnType::I64;
        }
        if PartGenerator::calculate_row_count(scale_factor, 1, 1) > i32::MAX as i64 {
            self.partkey_type = KeyColumnType::I64;
        }
        if SupplierGenerator::calculate_row_count(scale_factor, 1, 1) > i32::MAX as i64 {
            self.suppkey_type = KeyColumnType::I64;
        }
        // Order keys contain gaps, so their maximum can exceed the row count.
        let order_count = OrderGenerator::calculate_row_count(scale_factor, 1, 1);
        if order_count > i32::MAX as i64
            || OrderGenerator::make_order_key(order_count) > i32::MAX as i64
        {
            self.orderkey_type = KeyColumnType::I64;
        }
        self
    }
}

/// Iterator of Arrow [`RecordBatch`] that also knows its schema
pub trait RecordBatchIterator: Iterator<Item = RecordBatch> + Send {
    fn schema(&self) -> &SchemaRef;

    /// Set the batch size for this iterator
    fn with_batch_size(self, batch_size: usize) -> Self
    where
        Self: Sized;

    /// Set column type configuration to customize column types.
    /// I32 key requests automatically widen to Int64 when required by the scale factor.
    ///
    /// This allows configuring types for categories of columns:
    /// - decimal_type: Controls monetary columns (c_acctbal, l_quantity, etc.)
    /// - date_type: Controls date columns (l_shipdate, l_commitdate, l_receiptdate, o_orderdate)
    /// - nationkey_type: Controls nationkey columns (c_nationkey, n_nationkey, s_nationkey)
    /// - regionkey_type: Controls regionkey columns (n_regionkey, r_regionkey)
    /// - custkey_type: Controls custkey columns (c_custkey, o_custkey)
    /// - partkey_type: Controls partkey columns (p_partkey, ps_partkey, l_partkey)
    /// - suppkey_type: Controls suppkey columns (s_suppkey, ps_suppkey, l_suppkey)
    /// - orderkey_type: Controls orderkey columns (o_orderkey, l_orderkey)
    fn with_column_type_config(self, config: ColumnTypeConfig) -> Self
    where
        Self: Sized;
}
