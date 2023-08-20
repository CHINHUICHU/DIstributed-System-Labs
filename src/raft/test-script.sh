#!/bin/bash

rm out*

for i in {1..5}; do
    output="out-${i}"
    go test -run 2D > "$output"
    if grep -q "FAIL" "$output"; then
        echo "Failed tests in $output:"
        grep "FAIL" "$output"
        echo "--------------------------------------"
    fi
done