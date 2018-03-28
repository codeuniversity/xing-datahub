import unittest
import json
import requests
import time
import os
TOKEN = os.getenv('token', '')
class EndpointTestCase(unittest.TestCase):
  def test(self):
    users = [{ 'id':42, 'country': 'de' }, { 'id':42, 'country': 'de' }]
    requests.post('localhost:3000/users', json=users, headers={'access-token': TOKEN})

    time.sleep(30)
    resp = requests.get('http://localhost:3003/users', headers={'access-token': TOKEN})
    returned_users = resp.json()
    self.assertEqual(len(returned_users), 2)
    self.assertEqual(returned_users, users)
unittest.main()
