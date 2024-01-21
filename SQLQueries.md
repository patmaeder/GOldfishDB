# Example Requests

The following example requests are intended to illustrate the possibilities of the database. 
Feel free playing around.

### Create Table

```bash
echo "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, age INTEGER);" | nc localhost 8080
```

Running this command twice returns an error, as the table already exists.
This can be prevented by adding IF NOT EXISTS after TABLE.

### DROP Table

```bash
echo "DROP TABLE users;" | nc localhost 8080
```

Running this command twice returns an error, as the table does not exist.
This can be prevented by adding IF EXISTS after TABLE

### INSERT

```bash
echo "INSERT INTO users (name, email, age) VALUES
  ('Emily Johnson', 'emily.johnson@example.com', 25),
  ('Thomas Smith', 'thomas.smith@example.com', 32),
  ('Sarah Williams', 'sarah.williams@example.com', NULL),
  ('Peter Jones', 'peter.jones@example.com', 29),
  ('Alice Brown', 'alice.brown@example.com', NULL);" | nc localhost 8080
```

Inserting values in a UNIQUE column that already exist will throw an error.
```bash
echo "INSERT INTO users (name, email, age) VALUES
  ('Emily Johnson', 'emily.johnson@example.com', 25);" | nc localhost 8080
```

### UPDATE

```bash
echo "UPDATE users 
  SET email = 'emily.johnson.new@example.com',
  age = 26
  WHERE id = 1;" | nc localhost 8080
```

Now try running the above command that previously failed again.
This time it should work as expected.

### SELECT

```bash
echo "SELECT * FROM users LIMIT 3;" | nc localhost 8080
```

```bash
echo "SELECT * FROM users ORDER BY age DESC;" | nc localhost 8080
```

```bash
echo "SELECT name FROM users WHERE age > 28 ORDER BY name;" | nc localhost 8080
```

### DELETE

```bash
echo "DELETE FROM users WHERE name = 'Emily Johnson';" | nc localhost 8080
```
