//! Large-key generation must produce the same rows for every partition plan.

use std::collections::BTreeMap;
use tpchgen::generators::{LineItemGenerator, OrderGenerator};

#[test]
fn large_scale_partitions_match_sequential_orders_and_lines() {
    // Exercise both sides of the seed-stream switch and very large key domains.
    for scale in [
        1.0, 10_738.0, 14_317.0, 20_000.0, 29_999.0, 30_000.0, 30_001.0, 100_000.0, 300_000.0,
    ] {
        let total_orders = OrderGenerator::calculate_row_count(scale, 1, 1);
        let mut plans = vec![
            total_orders.min(2_000_000_000) as i32,
            total_orders.min(1_000_000_003) as i32,
        ];
        plans.dedup();
        for partitions in plans {
            // Each partition contains at most 450 orders in this matrix.
            let prefix_orders = (total_orders / partitions as i64) * 3;
            let last_order_key = OrderGenerator::make_order_key(prefix_orders);
            let expected_orders = OrderGenerator::new(scale, 1, 1)
                .iter()
                .take(prefix_orders as usize)
                .map(|row| row.to_string());
            let actual_orders = (1..=3)
                .flat_map(|part| OrderGenerator::new(scale, part, partitions).iter())
                .map(|row| row.to_string());
            assert_same_rows(expected_orders, actual_orders, "orders", scale, partitions);

            let expected_lines = LineItemGenerator::new(scale, 1, 1)
                .iter()
                .take_while(|row| row.l_orderkey <= last_order_key)
                .map(|row| row.to_string());
            let actual_lines = (1..=3)
                .flat_map(|part| LineItemGenerator::new(scale, part, partitions).iter())
                .map(|row| row.to_string());
            assert_same_rows(expected_lines, actual_lines, "lineitem", scale, partitions);
        }
    }
}

#[test]
fn large_scale_order_totals_match_lineitems_from_another_plan() {
    for scale in [30_000.0, 100_000.0, 300_000.0] {
        let orders: Vec<_> = OrderGenerator::new(scale, 2, 2_000_000_000)
            .iter()
            .take(3)
            .collect();
        assert_eq!(orders.len(), 3);
        let first_key = orders.first().unwrap().o_orderkey;
        let last_key = orders.last().unwrap().o_orderkey;
        let mut totals = BTreeMap::<i64, i64>::new();
        for line in LineItemGenerator::new(scale, 1, 1)
            .iter()
            .skip_while(|row| row.l_orderkey < first_key)
            .take_while(|row| row.l_orderkey <= last_key)
        {
            // TPC-H totals discount each line, round to cents, then add tax.
            let discounted = line.l_extendedprice.0 * (100 - line.l_discount.0) / 100;
            let taxed = discounted * (100 + line.l_tax.0) / 100;
            *totals.entry(line.l_orderkey).or_default() += taxed;
        }
        assert_eq!(totals.len(), orders.len());
        for order in orders {
            assert_eq!(
                totals[&order.o_orderkey], order.o_totalprice.0,
                "order {} at SF={scale}",
                order.o_orderkey
            );
        }
    }
}

fn assert_same_rows(
    mut expected: impl Iterator<Item = String>,
    mut actual: impl Iterator<Item = String>,
    table: &str,
    scale: f64,
    partitions: i32,
) {
    let mut row_index = 0;
    loop {
        match (expected.next(), actual.next()) {
            (None, None) => break,
            (Some(expected), Some(actual)) => assert_eq!(
                actual, expected,
                "{table} at SF={scale}, partitions={partitions}, row={row_index}"
            ),
            _ => panic!("{table} row count differs at SF={scale}, partitions={partitions}"),
        }
        row_index += 1;
    }
    assert!(row_index > 0);
}
