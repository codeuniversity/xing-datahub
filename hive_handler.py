from pyhive import hive

user_schema_string = """
  id INT,
  first_name STRING,
  last_name STRING,
  gender STRING,
  wants Array<STRING>,
  haves Array<STRING>,
  languages Array<STRING>,
  business_address struct<country: STRING, zipcode: STRING, city: STRING, street: STRING>,
  primary_company struct<title: STRING, name: STRING>
  """

connection_schema_string = """
  a INT,
  b INT
  """

users_create_query = """
  CREATE TABLE IF NOT EXISTS users
  ({}) STORED AS ORC
  """.format(user_schema_string)

connections_create_query = """
  CREATE TABLE IF NOT EXISTS connections
  ({}) STORED AS ORC
  """.format(connection_schema_string)

cursor = hive.connect('localhost').cursor()
cursor.execute(users_create_query)
cursor.execute(connections_create_query)

def create_csv_table(name, location_prefix, schema_string):
    create_table = """
      CREATE EXTERNAL TABLE {}
      ({})
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
      collection items terminated by '|'
      STORED AS TEXTFILE
      LOCATION '/user/cloudera/{}/{}b'
      """\
        .format(name, schema_string,location_prefix, name)
    print('creating csv table: ', name, '...')
    cursor.execute(create_table)
    print('...done!')


def insert_from_table(into_table_name, from_table_name):
    insert = "INSERT INTO {} SELECT * from {}".format(into_table_name,from_table_name)
    print('inserting ', into_table_name,' from csv: ', from_table_name)
    cursor.execute(insert)
    print('...done!')


def drop_table(name):
    q = "DROP TABLE {}".format(name)
    print('dropping ', name, '...')
    cursor.execute(q)
    print('...done!')


