#!/bin/bash

set -e
#set -x
testToRun=$1

echo "*****************Test to run cpTriangleIntPairs and/or largeJson SF tests. Usage: ./runTests.sh cpTriangleIntPairs|largeJson|all*************************"

if [ "$testToRun" != "all" ] && [ "$testToRun" != "cpTriangleIntPairs" ] && [ "$testToRun" != "largeJson" ]; then
echo
echo "**********************Invalid test name provided.Usage: ./runTests.sh cpTriangleIntPairs|largeJson|all. Exiting...."
echo
exit 1
fi

echo
echo "Running tests for cpTriangleIntPairs"
echo

runtime=`date  +"%m_%d_%Y%H%M"`

if [ "$testToRun" == "all" ] || [ "$testToRun" == "cpTriangleIntPairs" ]; then
    # cpTriangleIntPairs tests
    outname="$PWD/output/cpTriangleIntPairs$runtime.out"
    echo
    echo "Running tests for cpTriangleIntPairs, output to $outname"
    echo
    snowsql -f run_cpTriangleIntPairsTest.sql -o output_file=$outname -o friendly=false -o header=false

    echo
    echo "Test complete! Test output can be found in $outname"
    echo
fi
    if [ "$testToRun" == "all" ] || [ "$testToRun" == "largeJson" ]; then
    # largeJson tests
    outname="$PWD/output/largeJson$runtime.out"
    echo
    echo "Running tests for largeJson, output to $outname"
    echo
    snowsql -f run_largeJsonTest.sql -o output_file=$outname -o friendly=false -o header=false

    echo
    echo "Test complete! Test output can be found in $outname"
    echo
 fi


