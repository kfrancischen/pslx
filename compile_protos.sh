#!/bin/bash

for file in pslx/schema/*; do
  if [[ $file == *.proto && $file != pslx/schema/*rpc.proto ]]
  then
    echo "Compiling regular proto: $file"
    protoc  --proto_path="pslx/schema" --python_out=pslx/schema/ "$file"
  elif [[ $file == pslx/schema/*rpc.proto ]]
  then
    echo "Compiling rpc proto: $file"
    python -m grpc_tools.protoc -Ipslx/schema/ --python_out=pslx/schema/ --grpc_python_out=pslx/schema/ "$file"
  fi

done
sed -i -e 's/import enums_pb2 as enums__pb2/import pslx.schema.enums_pb2 as enums__pb2/g' pslx/schema/snapshots_pb2.py
sed -i -e 's/import enums_pb2 as enums__pb2/import pslx.schema.enums_pb2 as enums__pb2/g' pslx/schema/rpc_pb2.py
sed -i -e 's/import enums_pb2 as enums__pb2/import pslx.schema.enums_pb2 as enums__pb2/g' pslx/schema/storage_pb2.py
sed -i -e 's/import common_pb2 as common__pb2/import pslx.schema.common_pb2 as common__pb2/g' pslx/schema/rpc_pb2.py
sed -i -e 's/import rpc_pb2 as rpc__pb2/import pslx.schema.rpc_pb2 as rpc__pb2/g' pslx/schema/rpc_pb2_grpc.py
