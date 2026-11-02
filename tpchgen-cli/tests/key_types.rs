//! Verify adaptive key widths survive the full CLI-to-Parquet path.

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::DataType;
use assert_cmd::Command;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn parquet_preserves_primary_keys_above_int32() {
    for (table, base, option) in [
        ("customer", 150_000.0, "--custkey-type"),
        ("part", 200_000.0, "--partkey-type"),
        ("supplier", 10_000.0, "--suppkey-type"),
    ] {
        let output = tempdir().unwrap();
        let maximum = i32::MAX as i64 + 1;
        let scale = (maximum as f64 + 0.25) / base;
        let partition_count = i32::MAX.to_string();
        Command::cargo_bin("tpchgen-cli")
            .unwrap()
            .args(["--format", "parquet", "--tables", table])
            .args(["--scale-factor", &scale.to_string()])
            .args(["--parts", &partition_count, "--part", &partition_count])
            .args([option, "i32"])
            .arg("--output-dir")
            .arg(output.path())
            .assert()
            .success();

        let file = File::open(
            output
                .path()
                .join(format!("{table}/{table}.{partition_count}.parquet")),
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(reader.schema().field(0).data_type(), &DataType::Int64);
        let keys: Vec<i64> = reader
            .build()
            .unwrap()
            .flat_map(|batch| {
                batch
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(keys, vec![i32::MAX as i64, maximum]);
    }
}

#[test]
fn parquet_order_keys_follow_sparse_range_and_explicit_override() {
    for (order_count, requested, expected, maximum) in [
        (536_870_911, "i32", DataType::Int32, 2_147_483_623),
        (536_870_912, "i32", DataType::Int64, 2_147_483_648),
        (1_500_000, "i64", DataType::Int64, 6_000_000),
    ] {
        let output = tempdir().unwrap();
        let scale = (order_count as f64 + 0.25) / 1_500_000.0;
        let partitions = order_count.to_string();
        Command::cargo_bin("tpchgen-cli")
            .unwrap()
            .args(["--format", "parquet", "--tables", "orders,lineitem"])
            .args(["--scale-factor", &scale.to_string()])
            .args(["--parts", &partitions, "--part", &partitions])
            .args(["--orderkey-type", requested])
            .arg("--output-dir")
            .arg(output.path())
            .assert()
            .success();
        for table in ["orders", "lineitem"] {
            let file = File::open(
                output
                    .path()
                    .join(format!("{table}/{table}.{partitions}.parquet")),
            )
            .unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            assert_eq!(reader.schema().field(0).data_type(), &expected);
            let mut rows = 0;
            for batch in reader.build().unwrap() {
                let batch = batch.unwrap();
                let keys: Vec<i64> = match expected {
                    DataType::Int32 => batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .map(|&key| key as i64)
                        .collect(),
                    DataType::Int64 => batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec(),
                    _ => unreachable!(),
                };
                assert!(keys.iter().all(|&key| key == maximum));
                rows += batch.num_rows();
            }
            assert!((1..=7).contains(&rows));
        }
    }
}
