from google.protobuf.json_format import MessageToJson
import hdfs_helpers
import hive_handler
import os

class ExportHandler(object):
  def __init__(self, batch_size = 1000):
    self.max_batch_size = batch_size

    if not os.path.exists('tmp'):
      os.makedirs('tmp')

    self.file_name = 'first_users'
    self.file_location = "tmp/{}.csv".format(self.file_name)
    self.file = open(self.file_location, 'w')
    self.counter = 0
    self.current_batch_size = 0

  def add_user(self, user):
    print('adding nr ', self.counter)
    print(user_to_csv_line(user), file=self.file)
    self.counter += 1
    self.current_batch_size += 1
    if self.current_batch_size >= self.max_batch_size:
      self.commit()

  def commit(self):
    if self.current_batch_size > 0:
      self.file.close()
      hdfs_helpers.put_in_hdfs("users/{}b/data".format(self.file_name), self.file_location)
      hive_handler.create_users_csv_table(name = self.file_name)
      hive_handler.insert_users_from_table(self.file_name)
      hive_handler.drop_table(self.file_name)
      # TODO: remove csv file from hdfs too
      os.remove(self.file_location)
      self._reset()

  def _reset(self):
    self.file_name = "users_from_{}".format(self.counter)
    self.file_location = "tmp/{}.csv".format(self.file_name)
    self.file = open(self.file_location, 'w')
    self.current_batch_size = 0


def user_to_csv_line(user):
  list = [
    user.id,
    user.first_name,
    user.last_name,
    user.gender,
    user.wants,
    user.haves,
    user.languages,
    MessageToJson(user.business_address).replace("\n",''),
    MessageToJson(user.primary_company).replace("\n",'')
  ]
  return ';'.join(map(str, list))

