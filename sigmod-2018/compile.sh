#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
mkdir -p build/release
cd build/release
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE=""  -DFORCE_TESTS=OFF ../..
make -j8 VERBOSE=1
