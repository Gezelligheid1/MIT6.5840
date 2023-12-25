#! /bin/bash

# 2A
##################################################

N=200
M=10

OUTPUT_FILE="output.log"

>$OUTPUT_FILE

parallel_test() {
    answer=$(go test -run 2A -race)
    exist=$(echo "$answer" | grep "FAIL")
    if [[ $exist != "" ]]; then
        echo "$answer" >$1
        echo "FAIL in $1" >>$OUTPUT_FILE
    fi
}

for ((i = 0; i < $M; i++)); do
    if [[ -e "debug/$i.log" ]]; then
        rm "debug/$i.log"
    fi
done

for ((i = 0; i < $N; i++)); do
    for ((j = 0; j < $M; j++)); do
        LOG_FILE_NOW="debug/$j.log"
        parallel_test $LOG_FILE_NOW &
    done
    wait
done

wait

if [[ -s $OUTPUT_FILE ]]; then
    echo "FAIL" >>$OUTPUT_FILE
else
    echo "$(($N * $M)) times PASSED" >>$OUTPUT_FILE
fi

##################################################
