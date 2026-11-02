# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About This Repository

This is a fork of [clflushopt/tpchgen-rs](https://github.com/clflushopt/tpchgen-rs). See [scripts/README.md](scripts/README.md) for the differences introduced in this fork (optimized compression, DELTA_LENGTH_BYTE_ARRAY encoding, Parquet v2, Int32 keys, GPU-optimized row groups).

Blazing-fast TPC-H (and TPC-DS) benchmark data generator in pure Rust.

## Common Commands

```bash
# Build all crates
cargo build

# Build release (for performance testing)
cargo build --release

# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p tpchgen --tests
cargo test -p tpchgen-arrow
cargo test -p tpchgen-cli
cargo test -p tpcdsgen

# Run doc tests
cargo test -p tpchgen --doc

# Run a single test by name
cargo test -p tpchgen test_lineitem_sf_0_001

# Lint and format
cargo fmt --all -- --check
cargo clippy -- -D warnings

# Check for spelling errors (required before PRs)
typos

# Build docs
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace

# Run the CLI
cargo run --bin tpchgen-cli -- -s 1 --output-dir /tmp/tpch
cargo run --release --bin tpchgen-cli -- -s 10 --format parquet --parts 4 --output-dir /tmp/tpch
```

## Rust Toolchain

Pinned to Rust `1.89.0` (see `rust-toolchain.toml`). Includes `rustfmt` and `clippy` components.

## Workspace Structure

This is a Cargo workspace with four crates:

### `tpchgen` (core library)
- **Zero external dependencies by design** — easy to embed anywhere.
- `src/generators.rs` — all 8 table generators (`NationGenerator`, `RegionGenerator`, `PartGenerator`, `SupplierGenerator`, `PartSuppGenerator`, `CustomerGenerator`, `OrderGenerator`, `LineItemGenerator`). Each implements `IntoIterator` and exposes an `.iter()` method yielding row structs.
- `src/random.rs` — deterministic PRNG and random value generators.
- `src/distribution.rs` — static distributions loaded from embedded `dists.dss` data file.
- `src/text.rs` — Zipf-distributed text pool for comment columns.
- `src/decimal.rs`, `src/dates.rs` — integer-based arithmetic for TPC-H decimals and dates (no floats).
- `src/csv.rs` — CSV format wrappers for each row type.
- Row structs implement `Display` to produce the canonical TBL pipe-delimited format.
- Integration tests in `tests/integration_tests.rs` compare against reference `.tbl.gz` fixtures in `data/sf-*/`.

### `tpchgen-arrow`
- Depends only on `arrow` crate (no parquet).
- Each table has a `*Arrow` struct (e.g., `LineItemArrow`) that wraps the corresponding generator and implements `Iterator<Item = RecordBatch>` via the `RecordBatchIterator` trait.
- `src/lib.rs` defines `ColumnTypeConfig` for controlling Arrow column types: `DecimalColumnType` (Decimal128 vs f64), `DateColumnType` (Date32 vs TimestampMs), `KeyColumnType` (i32 vs i64).
- Default batch size: 8,000 rows (`DEFAULT_BATCH_SIZE`).

### `tpchgen-cli`
- CLI binary in `bin/main.rs` using `clap`; library in `src/lib.rs`.
- Key types: `TpchGenerator` (builder pattern), `GeneratorConfig`, `Table`, `OutputFormat`, `ParquetVersion`.
- Async generation (`tokio`) with parallel table/partition execution via `src/runner.rs`.
- Output planning in `src/output_plan.rs` — determines what files to generate.
- Parquet writing in `src/parquet.rs`; TBL in `src/tbl.rs`; CSV in `src/csv.rs`.
- Supports per-column compression overrides (`--uncompressed-columns`) and encoding overrides (`--column-encoding`).
- `scripts/generate_tpch.py` and `scripts/generate_tpch.sh` use this CLI with GPU-optimized defaults.

### `tpcdsgen`
- Separate TPC-DS generator (independent from tpchgen). Standalone binary with its own data files under `tpcdsgen/data/`.

## Architecture Patterns

**Generator pattern**: All table generators take `(scale_factor: f64, part: i32, num_parts: i32)`. The `part`/`num_parts` arguments divide work for parallel generation — each part independently generates its slice of rows, enabling embarrassingly parallel output.

**Performance philosophy**: No heap allocations during row generation; integer arithmetic instead of floating-point; multiple cores with tuned buffer sizes. The core `tpchgen` crate avoids all dependencies to keep it embeddable.

**Correctness**: Below SF 30,000, TBL output must match the original C `dbgen` tool. At SF 30,000 and above, the 64-bit random stream must produce the same rows regardless of partition layout; its corrected row padding and skip-ahead deliberately differ from legacy `dbgen` behavior. Keep tests for both small-scale reference fixtures and large-scale partition consistency. See `TESTING.md` for existing reference checksums.

## Conformance Testing

End-to-end conformance tests are in `tests/conformance.sh` (requires building `dbgen`). The CI runs these on every PR.

## Fork-Specific Differences

This fork diverges from upstream in the generated Parquet schema:
- String columns use `DELTA_LENGTH_BYTE_ARRAY` encoding (not plain)
- Certain incompressible columns are written uncompressed
- Parquet format version 2 (not v1)
- `nationkey`/`regionkey` use Int32 (not Int64)
- Row groups and partitions are sized for GPU workloads (~1M rows/row group, ~100M rows/file)

Use `--use-upstream-compression`, `--use-large-ids`, etc. flags to match upstream behavior.
