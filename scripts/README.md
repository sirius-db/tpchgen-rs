---
title: "TPC-H Data Generation Scripts"
description: "Scripts for generating TPC-H datasets with specific partition configurations"
last_updated: "2026-01-20"
related_paths:
  - "../README.md"
tags: ["documentation", "auto-generated", "context", "tpch", "data-generation"]
---

# TPC-H Data Generation Script

The repository includes a `generate_tpch.sh` script that automates the generation of TPC-H datasets with specific partition configurations for each table. The script supports arbitrary scale factors, each with its own optimized partition configuration, and can generate data in parallel using multiple CPU threads.

## Usage


With docker:

```
docker run -it --rm \
   --user $(id -u):$(id -g) \
   -v /output-directory-on-host:/data \
   ghcr.io/tomaugspurger/tpchgen-rs:tom-sync-upstream-clean \
   -s 10 -o /data/scale-10
```

## Differences From Upstream

By default, the script will produce output that differs from a default `tpchgen-cli` in a few ways:

1. Column compression is optimized by not compressing incompressable columns. This affects the columns `c_mktsegment,c_nationkey,l_commitdate,l_discount,l_quantity,l_receiptdate,l_shipdate,l_shipinstruct,l_shipmode,l_tax,n_nationkey,n_regionkey,o_orderdate,o_orderpriority,o_shippriority,p_brand,p_container,p_mfgr,p_size,p_type,r_regionkey,s_nationkey`.
2. Column encoding is optimized. In particular, we use `DELTA_LENGTH_BYTE_ARRAY` for string columns, rather than `plain`.
3. We use parquet format version 2 instead of version 1.
4. We use Int32 identifiers where possible. Order, customer, part, and supplier keys automatically widen to Int64 when their full key range exceeds Int32; matching primary and foreign keys use the same width across tables and partitions. Order keys account for their deliberate gaps; nation and region keys default to Int32.
5. Row Group and Partition sizes are optimized for GPUs. We target approximately 1,000,000 rows per row group and 100,000,000 rows per file.

Both the wrapper and CLI use corrected 64-bit random-stream advancement at SF
30,000 and above. This makes values independent of partition layout but changes
legacy output. See the [generation compatibility note](../README.md#generating-data)
before reusing an existing dataset.

## Output Metadata

`scripts/generate_tpch.sh` generates a `metadata.json` at the root of the directory containing information about the generated tables.

## Partition Configuration

The number of partitions per table is determined by the formula

```
max(1, ceil(SF * multiplier / 100_000_000))
```

where

* SF is the user-provided scale-factor
* multiplier is the per-table multiple from the table below
* 100,000,000 refers to the (approximate) target number of rows per file

| Table      | Multiplier |
| ---------- | ---------- |
| part       | 200000     |
| partsupp   | 800000     |
| supplier   | 10000      |
| customer   | 150000     |
| lineitem   | 6000000    |
| orders     | 1500000    |

## Parallel Execution

The script automatically detects the number of available CPU threads on your system and uses them for parallel data generation. You can override this with the `-j` or `--jobs` option to specify a custom number of parallel jobs.

The script will use one of the following methods for parallel execution, in order of preference:
1. GNU Parallel (if available)
2. xargs with parallel execution support
3. A simple fallback using background jobs

## Usage

```shell
./generate_tpch.sh [OPTIONS]

Options:
  -s, --scale SCALE              Scale factor (any positive integer; default: 1000)
  -f, --format FORMAT            Output format: parquet or tbl (default: parquet)
  -o, --output DIR               Base output directory (default: tpch-data)
  -j, --jobs THREADS             Number of parallel jobs (default: number of CPU threads)
      --parquet-row-group-bytes N  Override parquet row group size in bytes for all tables
      --use-upstream-compression Use upstream default compression (compress all columns)
      --use-float-type           Use f64 for decimal columns (instead of decimal128)
      --use-timestamp-type       Use timestamp_ms for date columns (instead of date32)
      --use-large-ids            Use i64 for nationkey/regionkey columns (instead of i32)
  -h, --help                     Show this help message
```

### Column Type Options

By default, the script uses the following column types:

| Column Category | Default Type | Flag to Override | Override Type |
| --------------- | ------------ | ---------------- | ------------- |
| Decimal/monetary columns | `decimal128` | `--use-float-type` | `f64` |
| Date columns | `date32` | `--use-timestamp-type` | `timestamp_ms` |
| Nationkey/Regionkey columns | `i32` | `--use-large-ids` | `i64` |

Note: The default `i32` for nationkey/regionkey differs from the upstream default of `i64`. Use `--use-large-ids` to match upstream behavior.

### Compression Options

By default, certain columns are written uncompressed to optimize for specific workloads. Use `--use-upstream-compression` to compress all columns with the default Snappy compression.

### Parquet Row Group Size

The script uses per-table default row group sizes optimized to produce approximately 100 million rows per row group:

| Table | Default Row Group Bytes |
| ----- | ----------------------- |
| customer | 165,000,000 |
| lineitem | 68,700,000 |
| nation | 5,000 |
| orders | 99,000,000 |
| part | 69,000,000 |
| partsupp | 147,000,000 |
| region | 5,000 |
| supplier | 154,000,000 |

Use `--parquet-row-group-bytes N` to override these defaults with a single value for all tables.

## Examples

```shell
# Generate TPC-H data with scale factor 1000 in Parquet format using all CPU threads
./generate_tpch.sh -s 1000 -f parquet -o tpch-sf1000

# Generate TPC-H data with scale factor 3000 using 16 parallel jobs
./generate_tpch.sh -s 3000 -f parquet -o tpch-sf3000 -j 16

# Generate TPC-H data with scale factor 1000 using only 4 parallel jobs and custom row group size
./generate_tpch.sh -s 1000 -f parquet -o tpch-sf1000 -j 4 --parquet-row-group-bytes 1048576

# Generate TPC-H data with f64 decimals and timestamp dates
./generate_tpch.sh -s 100 --use-float-type --use-timestamp-type

# Generate TPC-H data with upstream-compatible i64 nationkey/regionkey columns
./generate_tpch.sh -s 100 --use-large-ids

# Generate TPC-H data with upstream-compatible compression (all columns compressed)
./generate_tpch.sh -s 100 --use-upstream-compression
```

The script will create a directory structure where each table has its own subdirectory containing the specified number of partitions based on the chosen scale factor. For example:

```
tpch-data/
├── customer/
│   ├── part.0.parquet
│   ├── part.1.parquet
│   └── ... (2 parts for SF1000, 5 parts for SF3000)
├── lineitem/
│   ├── part.0.parquet
│   ├── part.1.parquet
│   └── ... (60 parts for SF1000, 180 parts for SF3000)
├── nation/
│   ├── part.0.parquet
│   └── ... (1 part for SF1000, 1 part for SF3000)
└── ... (other tables)
```

The partition files are zero-indexed (starting from 0) and use the format `part.{N}.{format}` where N is the partition number and format is either 'parquet' or 'tbl' depending on the chosen output format.

## Inspecting Generated Parquet Files

The repository includes an `inspect_tpch_parquet.py` script that helps you examine the metadata and schema of the generated Parquet files. This Python script provides detailed information about the structure and content of the TPC-H table partitions.

### Requirements

- Python 3
- PyArrow library (`pip install pyarrow`)

### Usage

```shell
python inspect_tpch_parquet.py <tpch_data_directory>
```

For example:
```shell
python inspect_tpch_parquet.py tpch-data
```

## Metadata Schema

The `metadata.json` file generated by `generate_tpch.sh` contains detailed information about the generated TPC-H dataset. The schema is documented below.

### Metadata

The root object of the metadata file.

| Name | Type | Description |
| ---- | ---- | ----------- |
| options | object | The command-line options used to generate the dataset (key-value pairs). |
| tables | object | A mapping of table names to TableInfo objects. |

### TableInfo

Information about a single TPC-H table.

| Name | Type | Description |
| ---- | ---- | ----------- |
| partition_count | integer | The number of partitions (files) in the table. |
| row_group_count | integer | The total number of row groups in the table. |
| row_count | integer | The total number of rows in the table. |
| total_bytes | integer | The total size of the table in bytes. |
| parquet_format_version | integer | The version of the parquet format used (1 or 2). |
| created_by | string | The software that created the parquet file. |
| table_schema | array\<SchemaField\> | The schema of the table as an array of field definitions. |
| avg_rows_per_partition | integer | The average number of rows per partition. |
| min_rows_per_partition | integer | The fewest rows in any partition. |
| max_rows_per_partition | integer | The most rows in any partition. |
| avg_bytes_per_partition | integer | The average size of a partition in bytes. |
| min_bytes_per_partition | integer | The smallest partition in bytes. |
| max_bytes_per_partition | integer | The largest partition in bytes. |
| avg_rows_per_row_group | integer | The average number of rows per row group. |
| min_rows_per_row_group | integer | The fewest rows in any row group. |
| max_rows_per_row_group | integer | The most rows in any row group. |
| avg_bytes_per_row_group | integer | The average size of a row group in bytes. |
| min_bytes_per_row_group | integer | The smallest row group in bytes. |
| max_bytes_per_row_group | integer | The largest row group in bytes. |
| columns | object | A mapping of column names to ColumnInfo objects. |

### SchemaField

Describes a single field in the table schema.

| Name | Type | Description |
| ---- | ---- | ----------- |
| name | string | The name of the column. |
| type | string | The Arrow data type of the column (e.g., "int64", "string", "decimal128(15, 2)"). |
| nullable | boolean | Whether the column allows null values. |

### ColumnInfo

Metadata for a single column in a parquet table, aggregated across all row groups and partitions.

| Name | Type | Description |
| ---- | ---- | ----------- |
| name | string | The name of the column. |
| physical_type | string | The physical parquet type of the column (e.g., "INT64", "BYTE_ARRAY"). |
| is_stats_set | boolean | Whether statistics are available for the column. |
| stats | ColumnStats \| null | Aggregated statistics for the column, or null if not available. |
| encodings | array\<string\> | The encodings used for the column (e.g., "PLAIN", "RLE_DICTIONARY", "DELTA_LENGTH_BYTE_ARRAY"). |
| compression | string | The compression codec used for the column (e.g., "SNAPPY", "UNCOMPRESSED"). |
| total_compressed_size | integer | The total size of the compressed column data in bytes across all row groups. |
| total_uncompressed_size | integer | The total size of the uncompressed column data in bytes across all row groups. |

### ColumnStats

Aggregated statistics for a column across all row groups and partitions.

| Name | Type | Description |
| ---- | ---- | ----------- |
| min | any | The minimum value of the column across all row groups. Type depends on the column type. |
| max | any | The maximum value of the column across all row groups. Type depends on the column type. |
| null_count | integer | The total number of null values in the column across all row groups. |
| num_values | integer | The total number of values in the column across all row groups. |
