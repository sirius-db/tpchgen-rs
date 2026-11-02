# tpchgen-rs

---

*Note, this is a fork of https://github.com/clflushopt/tpchgen-rs. See [scripts/README.md](scripts/README.md) for a list of differences.*

---

[![Apache licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/clflushopt/tpchgen-rs/blob/main/LICENSE
[actions-badge]: https://github.com/clflushopt/tpchgen-rs/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/clflushopt/tpchgen-rs/actions?query=branch%3Amain

Blazing fast [TPCH] benchmark data generator, in pure Rust with zero dependencies.

[TPCH]: https://www.tpc.org/tpch/

## Features

1. Blazing Speed 🚀
2. Obsessively Tested 📋
3. Fully parallel, streaming, constant memory usage 🧠

## Try it now

The easiest way to use this software is via the [`tpchgen-cli`] tool.

## Generating Data

### 1. Set up the environment

Install [pixi], then run `pixi install` to set up Rust 1.89.0, Python, and `pyarrow` in one step:

```shell
curl -fsSL https://pixi.sh/install.sh | bash
pixi install
pixi shell
```

[pixi]: https://pixi.sh

### 2. Build `tpchgen-cli`

```shell
RUSTFLAGS='-C target-cpu=native' cargo build --release -p tpchgen-cli
```

Add the binary to your `PATH`:

```shell
export PATH="$PWD/target/release:$PATH"
```

### 3. Generate data

Use `scripts/generate_tpch.py` to generate a full dataset with GPU-optimized partition sizes,
encodings, and compression settings:

```shell
python scripts/generate_tpch.py -s <SCALE_FACTOR> -f parquet -j <PARALLELISM> -o <OUTPUT_DIR>
```

For example, to generate scale factor 100 using 16 parallel jobs:

```shell
python scripts/generate_tpch.py -s 100 -f parquet -j 16 -o tpch-sf100
```

Key options:

| Option | Description | Default |
| ------ | ----------- | ------- |
| `-s` | Scale factor (integer) | `1000` |
| `-f` | Output format: `parquet` or `tbl` | `parquet` |
| `-o` | Output directory | `tpch-data` |
| `-j` | Number of parallel jobs | number of CPU threads |
| `--parquet-row-group-bytes N` | Override row group size in bytes for all tables | per-table defaults |
| `--use-upstream-compression` | Compress all columns (default skips incompressible ones) | off |
| `--use-float-type` | Use `f64` for decimal columns instead of `decimal128` | off |
| `--use-timestamp-type` | Use `timestamp_ms` for date columns instead of `date32` | off |
| `--use-large-ids` | Use `i64` for `nationkey`/`regionkey` instead of `i32` | off |

Parquet join keys use `i32` while their full key range fits in 2,147,483,647,
and automatically widen to `i64` otherwise. The choice applies to every primary
and foreign key in the same family across all tables and partitions:

| Key family | Maximum key | First integer scale factor using `i64` |
| ---------- | ----------- | ------------------------------------- |
| `orderkey` | 32 × floor(N / 8) + (N mod 8), N = floor(1,500,000 × SF) | 358 |
| `partkey` | floor(200,000 × SF) | 10,738 |
| `custkey` | floor(150,000 × SF) | 14,317 |
| `suppkey` | floor(10,000 × SF) | 214,749 |

Order keys contain deliberate gaps; their maximum is larger than the order row
count. `nationkey` and `regionkey` remain `i32` by default.
The `tpchgen-cli` binary accepts `--orderkey-type`, `--custkey-type`,
`--partkey-type`, and `--suppkey-type` with `i64` to request wider keys at
smaller scales. These options are not exposed by `scripts/generate_tpch.py`.
Their default `i32` setting widens automatically when needed. This behavior
also applies to the Arrow API.

At SF 30,000 and above, generating the same rows with different partition plans
produces identical values. Random draws, unused draws at the end of each row,
and partition skips all advance the same 64-bit sequence. This corrects legacy
behavior and can change output compared with older versions and reference
`dbgen` at these scales. Regenerate all tables together rather than mixing old
and corrected data. The random sequence below SF 30,000 is preserved.

The script writes each table into its own subdirectory as zero-indexed partition files:

```
tpch-sf100/
├── customer/
│   └── part.0.parquet
├── lineitem/
│   ├── part.0.parquet
│   ├── part.1.parquet
│   └── part.2.parquet   (6 files at SF=100)
├── orders/
│   ├── part.0.parquet
│   └── part.1.parquet   (2 files at SF=100)
└── ...
```

A `metadata.json` file is written to the output directory summarising the schema, row counts,
file sizes, and encodings of every generated table. See [scripts/README.md](scripts/README.md)
for full details on the partition sizing formula and all available options.

## Performance

[`tpchgen-cli`] is more than 10x faster than the next fastest TPCH generator we
know of. On a 2023 Mac M3 Max laptop, it easily generates data faster than can
be written to SSD. See [BENCHMARKS.md](./benchmarks/BENCHMARKS.md) for more
details on performance and benchmarking.

[`tpchgen-cli`]: ./tpchgen-cli/README.md

Times to create TPCH tables in Parquet format using `tpchgen-cli` and `duckdb` for various scale factors.

| Scale Factor | `tpchgen-cli` | DuckDB     | DuckDB (proprietary) |
| ------------ | ------------- | ---------- | -------------------- |
| 1            | `0:02.24`     | `0:12.29`  | `0:10.68`            |
| 10           | `0:09.97`     | `1:46.80`  | `1:41.14`            |
| 100          | `1:14.22`     | `17:48.27` | `16:40.88`           |
| 1000         | `10:26.26`    | N/A (OOM)  | N/A (OOM)            |

- DuckDB (proprietary) is the time required to create TPCH data using the
  proprietary DuckDB format
- Creating Scale Factor 1000 using DuckDB [required 647 GB of memory](https://duckdb.org/docs/stable/extensions/tpch.html#resource-usage-of-the-data-generator),
  which is why it is not included in the table above.

![Parquet Generation Performance](parquet-performance.png)


## Answers

The core `tpchgen` crate provides answers for queries 1 to 22 and for a scale factor
of 1. The answers exposed were derived from the [TPC-H Tools](https://www.tpc.org/)
official distribution.

## Testing

This crate has extensive tests to ensure correctness and produces exactly the
same, byte-for-byte output as the original [`dbgen`] implementation. We compare
the output of this crate with [`dbgen`] as part of every checkin. See
[TESTING.md](TESTING.md) for more details on testing methodology

## Crates

- [`tpchgen`](tpchgen): the core data generator logic for TPC-H. It has no
  dependencies and is easy to embed in other Rust project.

- [`tpchgen-arrow`](tpchgen-arrow) generates TPC-H data in [Apache Arrow]
  format. It depends on the arrow-rs library

- [`tpchgen-cli`](tpchgen-cli) is a [`dbgen`] compatible CLI tool that generates
  benchmark dataset using multiple processes.

[Apache Arrow]: https://arrow.apache.org/
[`dbgen`]: https://github.com/electrum/tpch-dbgen

## Contributing

Pull requests are welcome. For major changes, please open an issue first for
discussion. See our [contributors guide](CONTRIBUTING.md) for more details.

## Architecture

Please see [architecture guide](ARCHITECTURE.md) for details on how the code
is structured.

## License

The project is licensed under the [APACHE 2.0](LICENSE) license.

## References

- The TPC-H Specification, see the specification [page](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
- The Original `dbgen` Implementation you must submit an official request to access the software `dbgen` at their official [website](https://www.tpc.org/tpch/)
