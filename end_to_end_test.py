import unittest
import json
import requests
import time
import os
TOKEN = os.getenv('TOKEN', '')
class EndpointTestCase(unittest.TestCase):
  def test(self):
    users = [{ 'id':'42', 'country': 'de' }, { 'id':'42', 'country': 'de' }]
    for user in users:
      resp = requests.post('http://localhost:3000/users', json=user, headers={'access-token': TOKEN})
      self.assertEqual(resp.status_code, 200)
    time.sleep(90)
    resp = requests.get('http://localhost:3003/users', headers={'access-token': TOKEN})
    returned_users = resp.json()
    print(returned_users)
    print(type(returned_users))
    self.assertEqual(len(returned_users), 2)
    self.assertDictContainsSubset(users[0], returned_users[0])
    self.assertDictContainsSubset(users[1], returned_users[1])

time.sleep(10)
unittest.main()
