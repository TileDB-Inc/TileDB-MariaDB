#!/bin/bash
set -eux

# Install dependencies on Ubuntu

apt-get update

g++ --version
apt-get install --yes \
  bison \
  gdb \
  libncurses5-dev
