import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json

from os.path import realpath, join, exists, dirname
from pprint import pprint

# locate the data driectory
selfpath = dirname(realpath(__file__))
print selfpath

datapath = join(selfpath, "data")
print datapath

avscpath = join(datapath, "Base.avsc")
print avscpath

avropath = join(datapath, "TestJSON.avro")
print avropath

jsonpath= join(datapath, "TestJSON.json")
print jsonpath
    
if exists(avscpath):    
    schema = avro.schema.parse(open(avscpath, 'r').read())

    writer = DataFileWriter(open(avropath, "wb"), DatumWriter(), schema)
    
    if exists(jsonpath):
        data = json.loads(open(jsonpath, 'r').read())
        pprint(data)
        
        writer.append(data)    
    
    writer.close()
    
    reader = DataFileReader(open(avropath, "rb"), DatumReader())
    for record in reader:
        pprint(record)
    
    reader.close()
