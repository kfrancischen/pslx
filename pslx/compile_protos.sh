#!/bin/bash

for file in schema/*; do
  if [[ $file == *.proto ]]
  then
    echo "Compiling: $file"
    protoc  --proto_path="schema" --python_out=schema/ "$file"
  fi

done
