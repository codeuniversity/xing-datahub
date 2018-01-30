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


def array_helper(array):
    if len(array) == 0:
        return 'collect_set(STRING(NULL))'
    else:
        s = ""
    for item in array:
        if not s == "":
            s = s + ', '
        if type(item) == str:
            s = s + "'{}'".format(item)
        elif type(item) == int:
            s = s + "{}".format(item)
        else:
            raise Exception("Unexpeted type of item in array_helper: {}".format(type(item)))

    return "Array({})".format(s)


def address_helper(struct):
    return "named_struct('country', '{}','zipcode', '{}','city', '{}','street', '{}')".format(
        struct.country, struct.city, struct.zipcode, struct.street
    )


def company_helper(struct):
    return "named_struct('title', '{}','name', '{}')".format(
        struct.title, struct.name
    )


def attr_helper(attr):
    if attr is None or attr == '':
        return "NULL"
    elif type(attr) == str:
        return "'{}'".format(attr)
    elif type(attr) == int:
        return "{}".format(attr)
    else:
        raise Exception("Unexpeted type of attr in attr_helper: {}".format(type(attr)))


def insert_user(user):
    q = "INSERT INTO users {}".format(single_user_sql_string(user))
    cursor.execute(q)
    print('Did:', q)


def insert_users(users):
    q = "INSERT INTO users {}".format(multiple_users_sql_string(users))
    cursor.execute(q)
    print('Did:', q)


def single_user_sql_string(user):
    return "select {}, {}, {}, {}, {}, {}, {}, {}, {}".format(
        attr_helper(user.id),
        attr_helper(user.first_name),
        attr_helper(user.last_name),
        attr_helper(user.gender),
        array_helper(user.wants),
        array_helper(user.haves),
        array_helper(user.languages),
        address_helper(user.business_address),
        company_helper(user.primary_company)
    )


def multiple_users_sql_string(users):
    s = ""
    for user in users:
        if not s == "":
            s += ' UNION ALL '
        s += single_user_sql_string(user)
    return s


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


