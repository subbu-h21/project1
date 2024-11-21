#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Install Rust in a custom directory
echo "Installing Rust..."
export CARGO_HOME="$HOME/.cargo"
export RUSTUP_HOME="$HOME/.rustup"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Add cargo binaries to PATH
export PATH="$CARGO_HOME/bin:$PATH"

# Verify Rust installation
rustc --version
cargo --version
