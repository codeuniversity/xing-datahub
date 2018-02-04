from pyhive import hive

cursor = hive.connect('localhost').cursor()

def create_table(name, schema_string):
  query = """
  CREATE TABLE IF NOT EXISTS {}
  ({}) STORED AS ORC
  """.format(name, schema_string)
  cursor.execute(query)

user_schema_string = """
  id INT,
  jobroles Array<INT>,
  career_level INT,
  discipline_id INT,
  industry_id INT,
  country STRING,
  region int,
  experience_n_entries_class INT,
  experience_years_experience INT,
  experience_years_in_current INT,
  edu_degree INT,
  edu_fieldofstudies Array<INT>,
  wtcj INT,
  premium INT
  """

create_table('users', user_schema_string)

item_schema_string = """
  id INT,
  title INT,
  career_level INT,
  discipline_id INT,
  industry_id INT,
  country STRING,
  is_payed INT,
  region INT,
  latitude FLOAT,
  longitude FLOAT,
  employment INT,
  tags Array<int>,
  created_at INT
  """
create_table('items', item_schema_string)

interaction_schema_string = """
  user_id INT,
  item_id INT,
  interaction_type INT,
  created_at INT
  """
create_table('interactions', interaction_schema_string)

target_user_schema_string = """
  user_id INT
"""
create_table('target_users', target_user_schema_string)

target_item_schema_string = """
  item_id INT
"""
create_table('target_items', target_item_schema_string)

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


