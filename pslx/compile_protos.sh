#!/bin/bash

for file in schema/*; do
  if [[ $file == *.proto ]]
  then
    echo "Compiling: $file"
    protoc  --proto_path="schema" --python_out=schema/ "$file"
  fi

done
sed -i -e 's/import enums_pb2 as enums__pb2/import pslx.schema.enums_pb2 as enums__pb2/g' schema/snapshots_pb2.py
