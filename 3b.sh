#!/bin/bash

for i in {1..100}; do
    LOG_LEVEL=debug make project3bfinal >& log.txt || break
    echo $i finished
    done
