#!/bin/bash

function traverse() {
    for file in $(ls "$1")
    do
        if [[ ! -d ${1}/${file} ]]; then
            if [[ (${file} == *.py) && (${file} != __init__.py) ]]; then
                echo "============================================================="
                echo "Running unittest of file ${1}/${file}"
                python -m unittest ${1}/${file}
            fi
        else
            traverse "${1}/${file}"
        fi
    done
}

traverse "pslx/test"
