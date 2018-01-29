from pyhive import hive

cursor = hive.connect('localhost').cursor()
cursor.execute("CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3,2)) CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC")
cursor.execute("INSERT INTO TABLE students VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32)")
cursor.execute('SELECT * from students')
print(cursor.fetchall())
