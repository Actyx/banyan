#!/bin/bash
cargo flamegraph --bin banyan-cli -- bench --count 10000000
