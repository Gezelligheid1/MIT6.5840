#! /bin/bash

log=$(date -d now +%Y%m%d)
echo ${log}*

rm -rf ${log}*

VERBOSE=1 ./dstest.py 2A -n 10000 -p 100 -r > answer.log
