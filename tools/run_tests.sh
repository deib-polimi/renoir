#!/usr/bin/env bash

# This is a wrapper that sets some environment variables triggering more in depth tests

export RSTREAM_TEST_LOCAL_CORES=1,2,4,8
export RSTREAM_TEST_REMOTE_HOSTS=1,2,4
export RSTREAM_TEST_REMOTE_CORES=1,2,4,8
export RSTREAM_TEST_TIMEOUT=10

cargo test "$@"