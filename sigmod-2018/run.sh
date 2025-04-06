#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
${DIR}/build/release/driver
#perf stat -e 'cpu/mem-loads,ld_blocks.store_forward/,mem_inst_retired.lock_loads/' ${DIR}/build/release/driver
