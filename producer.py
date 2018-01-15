from confluent_kafka import Producer
import json
import sys
# the mock-0.3.1 dir contains testcase.py, testutils.py & mock.py
sys.path.append('build')
import user_pb2

user_data = {
    "id": 1,
    "first_name": "Nick",
    "last_name": "Recobra",
    "haves": ["data engineering", "scala"],
    "wants": ["machine learning", "clojure"],
    "gender": ["male"],
    "languages": ["ru", "en"],
    "business_address": {
        "zipcode": "22607",
        "country": "Germany",
        "city": "Hamburg",
        "street": "Dammtorstrasse 30"
    },
    "primary_company": {
        "title": "Data Engineer",
        "industry": "IT",
        "name": "XING SE"
    }
}
msg = json.dumps(user_data)
user_dict = json.loads(msg)

user = user_pb2.User()
user.id = user_dict['id']
user.first_name = user_dict['first_name']
user.last_name = user_dict['last_name']

s = user.SerializeToString()

p = Producer({'bootstrap.servers': 'localhost:9092'})
p.produce('users', s)
p.flush()
