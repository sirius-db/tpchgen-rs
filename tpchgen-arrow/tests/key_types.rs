//! Check key widths using tiny partitions of large datasets, without generating billions of rows.

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::DataType;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, OrderGenerator, PartGenerator, PartSuppGenerator,
    SupplierGenerator,
};
use tpchgen_arrow::{
    ColumnTypeConfig, CustomerArrow, KeyColumnType, LineItemArrow, OrderArrow, PartArrow,
    PartSuppArrow, RecordBatchIterator, SupplierArrow,
};

#[test]
fn matching_key_types_across_tables_and_partitions() {
    // Each key family widens independently at the first integer scale that exceeds Int32.
    for (scale, order_type, cust, part, supp) in [
        (
            1.0,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
        ),
        (
            357.0,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
        ),
        (
            358.0,
            DataType::Int64,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
        ),
        (
            10737.0,
            DataType::Int64,
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
        ),
        (
            10738.0,
            DataType::Int64,
            DataType::Int32,
            DataType::Int64,
            DataType::Int32,
        ),
        (
            14316.0,
            DataType::Int64,
            DataType::Int32,
            DataType::Int64,
            DataType::Int32,
        ),
        (
            14317.0,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int32,
        ),
        (
            214748.0,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int32,
        ),
        (
            214749.0,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
        ),
    ] {
        // An early partition must use the same widths as the end of the dataset.
        for partition in [1, 1000] {
            let customer = CustomerArrow::new(CustomerGenerator::new(scale, partition, 1000));
            let order = OrderArrow::new(OrderGenerator::new(scale, partition, 1000));
            let part_gen = PartArrow::new(PartGenerator::new(scale, partition, 1000));
            let supplier = SupplierArrow::new(SupplierGenerator::new(scale, partition, 1000));
            let partsupp = PartSuppArrow::new(PartSuppGenerator::new(scale, partition, 1000));
            let lineitem = LineItemArrow::new(LineItemGenerator::new(scale, partition, 1000));
            for (generator, keys) in [
                (
                    &customer as &dyn RecordBatchIterator,
                    vec![("c_custkey", &cust)],
                ),
                (
                    &order,
                    vec![("o_custkey", &cust), ("o_orderkey", &order_type)],
                ),
                (&part_gen, vec![("p_partkey", &part)]),
                (&supplier, vec![("s_suppkey", &supp)]),
                (
                    &partsupp,
                    vec![("ps_partkey", &part), ("ps_suppkey", &supp)],
                ),
                (
                    &lineitem,
                    vec![
                        ("l_partkey", &part),
                        ("l_suppkey", &supp),
                        ("l_orderkey", &order_type),
                    ],
                ),
            ] {
                for (name, expected) in keys {
                    assert_eq!(
                        generator
                            .schema()
                            .field_with_name(name)
                            .unwrap()
                            .data_type(),
                        expected,
                        "{name} at SF={scale}, partition={partition}"
                    );
                }
            }
        }
    }
}

macro_rules! primary_key_boundary {
    ($name:ident, $generator:ident, $arrow:ident, $base:expr) => {
        #[test]
        fn $name() {
            // Fractional scales exercise the exact boundary, including the final partial row.
            for maximum in [i32::MAX as i64 - 1, i32::MAX as i64, i32::MAX as i64 + 1] {
                let scale = (maximum as f64 + 0.25) / $base;
                assert_eq!($generator::calculate_row_count(scale, 1, 1), maximum);
                let partition_count = maximum.min(i32::MAX as i64) as i32;
                // At most two rows in the last partition; keys include the dataset maximum.
                let generator =
                    $arrow::new($generator::new(scale, partition_count, partition_count))
                        .with_column_type_config(ColumnTypeConfig::default())
                        .with_batch_size(1);
                let expected = if maximum > i32::MAX as i64 {
                    DataType::Int64
                } else {
                    DataType::Int32
                };
                assert_eq!(generator.schema().field(0).data_type(), &expected);
                let schema = generator.schema().clone();
                let mut last_key = 0;
                for batch in generator {
                    assert_eq!(batch.schema(), schema);
                    last_key = match expected {
                        DataType::Int32 => batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(0) as i64,
                        DataType::Int64 => batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        _ => unreachable!(),
                    };
                }
                assert_eq!(last_key, maximum);
            }
        }
    };
}

primary_key_boundary!(
    customer_boundary,
    CustomerGenerator,
    CustomerArrow,
    150_000.0
);
primary_key_boundary!(part_boundary, PartGenerator, PartArrow, 200_000.0);
primary_key_boundary!(
    supplier_boundary,
    SupplierGenerator,
    SupplierArrow,
    10_000.0
);

#[test]
fn order_keys_widen_at_the_sparse_key_boundary() {
    // Consecutive order indices jump across Int32's maximum because of the key gaps.
    for (order_count, maximum, expected) in [
        (536_870_910, 2_147_483_622, DataType::Int32),
        (536_870_911, 2_147_483_623, DataType::Int32),
        (536_870_912, 2_147_483_648, DataType::Int64),
    ] {
        let scale = (order_count as f64 + 0.25) / 1_500_000.0;
        assert_eq!(
            OrderGenerator::calculate_row_count(scale, 1, 1),
            order_count as i64
        );
        for partition in [1, order_count] {
            let generators: Vec<Box<dyn RecordBatchIterator>> = vec![
                Box::new(
                    OrderArrow::new(OrderGenerator::new(scale, partition, order_count))
                        .with_column_type_config(ColumnTypeConfig::default())
                        .with_batch_size(1),
                ),
                Box::new(
                    LineItemArrow::new(LineItemGenerator::new(scale, partition, order_count))
                        .with_column_type_config(ColumnTypeConfig::default())
                        .with_batch_size(1),
                ),
            ];
            for generator in generators {
                let schema = generator.schema().clone();
                assert_eq!(schema.field(0).data_type(), &expected);
                let expected_key = if partition == 1 { 1 } else { maximum };
                let mut row_count = 0;
                for batch in generator {
                    assert_eq!(batch.schema(), schema);
                    let key = match expected {
                        DataType::Int32 => batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(0) as i64,
                        DataType::Int64 => batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        _ => unreachable!(),
                    };
                    assert_eq!(key, expected_key);
                    row_count += batch.num_rows();
                }
                assert!((1..=7).contains(&row_count));
            }
        }
    }
}

#[test]
fn explicit_i64_stays_i64_and_i32_cannot_overflow() {
    for requested in [KeyColumnType::I32, KeyColumnType::I64] {
        for scale in [1.0, 214749.0] {
            let config = ColumnTypeConfig {
                orderkey_type: requested,
                custkey_type: requested,
                partkey_type: requested,
                suppkey_type: requested,
                ..Default::default()
            };
            let expected = if requested == KeyColumnType::I64 || scale > 1.0 {
                DataType::Int64
            } else {
                DataType::Int32
            };
            let generators: Vec<Box<dyn RecordBatchIterator>> = vec![
                Box::new(
                    CustomerArrow::new(CustomerGenerator::new(scale, 1, 1))
                        .with_column_type_config(config),
                ),
                Box::new(
                    OrderArrow::new(OrderGenerator::new(scale, 1, 1))
                        .with_column_type_config(config),
                ),
                Box::new(
                    PartArrow::new(PartGenerator::new(scale, 1, 1)).with_column_type_config(config),
                ),
                Box::new(
                    SupplierArrow::new(SupplierGenerator::new(scale, 1, 1))
                        .with_column_type_config(config),
                ),
                Box::new(
                    PartSuppArrow::new(PartSuppGenerator::new(scale, 1, 1))
                        .with_column_type_config(config),
                ),
                Box::new(
                    LineItemArrow::new(LineItemGenerator::new(scale, 1, 1))
                        .with_column_type_config(config),
                ),
            ];
            for mut generator in generators {
                let schema = generator.schema().clone();
                // Generate a batch too, so schema and array widths must agree.
                let batch = generator.next().unwrap();
                assert_eq!(batch.schema(), schema);
                for field in schema.fields() {
                    if ["orderkey", "custkey", "partkey", "suppkey"]
                        .iter()
                        .any(|suffix| field.name().ends_with(suffix))
                    {
                        assert_eq!(
                            field.data_type(),
                            &expected,
                            "{} at SF={scale}",
                            field.name()
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn foreign_keys_remain_positive_and_preserve_values_above_int32() {
    // Both random key ranges exceed Int32 before the old SF=30000 RNG switch.
    let scale = 20000.0;
    let max_customer = CustomerGenerator::calculate_row_count(scale, 1, 1);
    let max_part = PartGenerator::calculate_row_count(scale, 1, 1);
    let max_supplier = SupplierGenerator::calculate_row_count(scale, 1, 1);
    let mut orders = OrderArrow::new(OrderGenerator::new(scale, 1, 1)).with_batch_size(64);
    let batch = orders.next().unwrap();
    let customers = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(
        customers
            .values()
            .iter()
            .all(|&key| key >= 1 && key <= max_customer)
    );
    assert!(customers.values().iter().any(|&key| key > i32::MAX as i64));

    let mut lines = LineItemArrow::new(LineItemGenerator::new(scale, 1, 1)).with_batch_size(64);
    let batch = lines.next().unwrap();
    let parts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let suppliers = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(
        parts
            .values()
            .iter()
            .all(|&key| key >= 1 && key <= max_part)
    );
    assert!(parts.values().iter().any(|&key| key > i32::MAX as i64));
    assert!(
        suppliers
            .values()
            .iter()
            .all(|&key| key >= 1 && key as i64 <= max_supplier)
    );
}

#[test]
fn partsupp_preserves_part_and_supplier_keys_above_int32() {
    let generator = PartSuppGenerator::new(300_000.0, 100_000_000, 2_000_000_000);
    let expected: Vec<_> = generator.clone().iter().take(16).collect();
    let batch = PartSuppArrow::new(generator)
        .with_batch_size(16)
        .next()
        .unwrap();
    let parts = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let suppliers = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(expected.iter().any(|row| row.ps_partkey > i32::MAX as i64));
    assert!(expected.iter().any(|row| row.ps_suppkey > i32::MAX as i64));
    for (index, row) in expected.iter().enumerate() {
        assert_eq!(parts.value(index), row.ps_partkey);
        assert_eq!(suppliers.value(index), row.ps_suppkey);
    }

    let generator = LineItemGenerator::new(300_000.0, 1, 1);
    let expected: Vec<_> = generator.clone().iter().take(64).collect();
    let batch = LineItemArrow::new(generator)
        .with_batch_size(64)
        .next()
        .unwrap();
    let suppliers = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(expected.iter().any(|row| row.l_suppkey > i32::MAX as i64));
    for (index, row) in expected.iter().enumerate() {
        assert_eq!(suppliers.value(index), row.l_suppkey);
    }
}
