from google.protobuf.json_format import MessageToJson
import hdfs_helpers
import hive_handler
import os
import hive_handler

def csv_address_helper(address):
  return "'{}', '{}', '{}', '{}'".format(address.country, address.zipcode, address.city, address.street)

def csv_company_helper(company):
  return "{}, {}".format(company.title, company.name)

def csv_array_helper(array):
  return "|".join(map(str, array))

def user_to_csv_line(user):
  list = [
    user.id,
    user.first_name,
    user.last_name,
    user.gender,
    csv_array_helper(user.wants),
    csv_array_helper(user.haves),
    csv_array_helper(user.languages),
    user.business_address.country,
    user.business_address.zipcode,
    user.business_address.city,
    user.business_address.street,
    user.primary_company.title,
    user.primary_company.name
  ]
  return ';'.join(map(str, list))

def connection_to_csv_line(connection):
  list = [ connection.a, connection.b ]
  return ';'.join(map(str, list))

class ExportHandler(object):
  def __init__(self, batch_size = 1000, name = 'users', converter = user_to_csv_line, schema_string = hive_handler.user_schema_string):
    self.max_batch_size = batch_size
    self.name = name
    self.converter = converter
    self.schema_string = schema_string
    if not os.path.exists('tmp'):
      os.makedirs('tmp')

    self.file_name = "first_{}".format(self.name)
    self.file_location = "tmp/{}.csv".format(self.file_name)
    self.file = open(self.file_location, 'w')
    self.counter = 0
    self.current_batch_size = 0

  def add(self, record):
    print(self.name,': adding nr ', self.counter)
    print(self.converter(record), file=self.file)
    self.counter += 1
    self.current_batch_size += 1
    if self.current_batch_size >= self.max_batch_size:
      self.commit()

  def commit(self):
    if self.current_batch_size > 0:
      self.file.close()
      hdfs_helpers.put_in_hdfs(self.name + "/{}b/data".format(self.file_name), self.file_location)
      self._handle_tables()
      # TODO: remove csv file from hdfs too
      os.remove(self.file_location)
      self._reset()

  def _handle_tables(self):
    hive_handler.create_csv_table(self.file_name, self.name, self.schema_string)
    hive_handler.insert_from_table(self.name, self.file_name)
    hive_handler.drop_table(self.file_name)

  def _reset(self):
    self.file_name = "{}_from_{}".format(self.name, self.counter)
    self.file_location = "tmp/{}.csv".format(self.file_name)
    self.file = open(self.file_location, 'w')
    self.current_batch_size = 0

class UserExportHandler(ExportHandler):
  def _handle_tables(self):
    hive_handler.create_user_csv_table(self.file_name)
    hive_handler.insert_into_users_from_table(self.file_name)
    hive_handler.drop_table(self.file_name)

class ConnectionExportHandler(ExportHandler):
  pass
