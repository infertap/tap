// SPDX-License-Identifier: Apache-2.0
//! Measure content-identity derivation throughput for 16-token blocks.
//! Run with `cargo run --release --example content_chain_cost`.
//! This isolates the hash calculation; it does not measure the complete event pipeline.

use std::time::Instant;

use infertap::source::wire::content::{chain, BlockInput};

fn main() {
    let tokens: Vec<u32> = (0..16).collect(); // one block at the observed block_size
    let n = 2_000_000u32;
    let t0 = Instant::now();
    let mut p = 0u128;
    for _ in 0..n {
        p = chain(
            p,
            &BlockInput {
                tokens: &tokens,
                extra_keys: None,
            },
        );
    }
    let per_sec = f64::from(n) / t0.elapsed().as_secs_f64();
    println!("content chain: {per_sec:.0} blocks/sec over {n} blocks (checksum {p:x})");
}
