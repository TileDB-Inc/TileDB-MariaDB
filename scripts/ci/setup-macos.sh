#!/bin/bash
set -eux

export HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1

# Install dependencies on macOS
brew install \
  bison@2.7 \
  boost \
  cmake \
  gnutls \
  jemalloc \
  m4 \
  openssl@1.1 \
  pkg-config