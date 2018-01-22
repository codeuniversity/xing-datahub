from json import dumps
from hdfs import InsecureClient

cl = InsecureClient('http://quickstart.cloudera:50070', user='cloudera')
print(cl.list("/tmp"))

cl.write('/tmp/sample.txt', data=dumps('this is a samplexx'), overwrite=True, encoding='utf-8')
print(cl.list("/tmp"))

with cl.read("/tmp/sample.txt") as reader:
    content = reader.read()
    print(content)
