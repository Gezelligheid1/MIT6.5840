#! /bin/bash

# 2A
##################################################

LOG_FILE="debug.log"
for i in {0..10}; do
    flag=true
    go test -run 2A -race >debug.log
    if grep "FAIL: TestInitialElection2A" <"$LOG_FILE"; then
        echo "FAIL"
        flag=false
        break
    fi
done

if [ $flag == true ]; then
    echo "PASSED"
fi

##################################################
