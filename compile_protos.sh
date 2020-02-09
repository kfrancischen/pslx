#!/bin/bash

for file in pslx/schema/*; do
  if [[ $file == *.proto ]]
  then
    echo "Compiling: $file"
    protoc  --proto_path="pslx/schema" --python_out=pslx/schema/ "$file"
  fi

done
sed -i -e 's/import enums_pb2 as enums__pb2/import pslx.schema.enums_pb2 as enums__pb2/g' pslx/schema/snapshots_pb2.py
