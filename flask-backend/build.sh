#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Install Rust
echo "Installing Rust..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Add cargo binaries to PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Verify Rust installation
rustc --version
cargo --version
