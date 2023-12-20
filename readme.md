## introduction

this project extracts the module["pyplasma"] from origin pyarrow project(version before 11.0.0), you can use it as an independent module you can use it in c++ and python 

## compile

    1. cd pyplasma
    2. make build && cd build
    3. cmake ..
    4. make -j8

## example plasma for pandas

````python
import plasma
import pandas as pd
import numpy as np

# create plasma client,assuming current plasma store name is /tmp/plasma
client = plasma.PlasmaClient("/tmp/plasma")
# get plasma store server's capacity
print("plasma store server:", client.store_capacity())
# create a plasma object id
# object length is 20 bytes ,do not larger than 20 
object_id = plasma.PlasmaClient.get_object_id(20 * b"b")
# create a pandas dataframe
rows = 5
frame = pd.DataFrame(np.random.rand(rows, rows))
# if current object id is already exist,need remove object_id from server first
client.remove([object_id])
# store dataframe to plasma store server
buffer = client.write_df(object_id, frame)
# get from plasma server 
frame = client.read_df(object_id)
print(frame)
````

## example

````python
import plasma
import pandas as pd
import pyarrow as pa
import numpy as np

# create plasma client,assuming current plasma store name is /tmp/plasma
client = plasma.PlasmaClient("/tmp/plasma")
# get plasma store server's capacity
print("plasma store server:", client.store_capacity())
# create a plasma object id
# object length is 20 bytes ,do not larger than 20 
object_id = plasma.PlasmaClient.get_object_id(20 * b"b")
# create a pandas dataframe
rows = 5
frame = pd.DataFrame(np.random.rand(rows, rows))


# create a buffer from plasma store server
# write to store buffer
def calculate_ipc_size(table_frame: pa.Table) -> int:
    sink = pa.MockOutputStream()
    with pa.ipc.new_stream(sink, table_frame.schema) as writer:
        writer.write_table(table_frame)
    return sink.size()


table = pa.Table.from_pandas(frame)
buffer_size = calculate_ipc_size(table)
# if current object id is already exist,need remove object_id from server first
client.remove([object_id])
# create a buffer and it's size is buffer_size
buffer = client.create(object_id, buffer_size)
# using pyarrow ipc to write to buffer or other ways 
stream = pa.FixedSizeBufferWriter(buffer)
with pa.RecordBatchStreamWriter(stream, table.schema) as writer:
    writer.write_table(table)
# write finished,submit to plasma store server 
client.seal(object_id)
# get from plasma server 
read_buffer = client.get_buffer(object_id)
reader = pa.BufferReader(read_buffer)
table = pa.ipc.open_stream(reader).read_all()
print(table)
````

## start server

    command: ./plasma-store-server -m [memory_size] -s [store_name]
    example: ./plasma-store-server -m 1000000000 -s /tmp/plasma

## warning

    when compile if there exist some error like abi:cxx11,
    you can add an compile option:cmake .. -DGLIBCXX_USE_CXX_API=0 
    or you can add in CMakeLists.txt: 
        add definitions(-D _GLIBCXX_USE_CXX11_API=0)




