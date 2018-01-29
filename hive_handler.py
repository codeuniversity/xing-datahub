from pyhive import hive

cursor = hive.connect('localhost').cursor()
query = """
  CREATE TABLE IF NOT EXISTS users
  (
    id INT,
    first_name STRING,
    last_name STRING,
    gender STRING,
    wants Array<STRING>,
    haves Array<STRING>,
    languages Array<STRING>
  )
  """

cursor.execute(query)

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
  q = "INSERT INTO users select {}, {}, {}, {}, {}, {}, {}".format(
    attr_helper(user.id),
    attr_helper(user.first_name),
    attr_helper(user.last_name),
    attr_helper(user.gender),
    array_helper(user.wants),
    array_helper(user.haves),
    array_helper(user.languages),
  )
  cursor.execute(q)
  print('Did:', q)
