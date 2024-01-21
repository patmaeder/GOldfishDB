# Go DB

Go DB is a lightweight and easy-to-use database written entirely in Go. 
It provides a basic set of operations for managing data, including CREATE, DROP, INSERT, UPDATE, DELETE, and SELECT. 
Go DB can be accessed over TCP, making it a versatile tool for various applications.

## 1. Supported DataTypes

The following four data types are natively supported by Go DB
Dates can be saved as REAL as UNIX Timestamps.

| Datatype | Native representation |
|----------|-----------------------|
| INTEGER  | int32                 |
| REAL     | int64                 |
| BOOLEAN  | bool                  |
| TEXT     | [1024]byte            |

## 2. Language Support 
Go BD supports a small feature set of selected SQL methods. 
These include

### Supported Methods

**Data Definition Language** (DDL)
- CREATE TABLE
- DROP TABLE

**Data Manipulation Language** (DML)
- INSERT
- UPDATE
- DELETE

**Data Query Language** (DQL)
- SELECT

### Supported modifiers
- WHERE
- ORDER BY
- LIMIT

## 3. Getting started
To start the TCP server on `http://localhost:8080` open a terminal window and navigate to the root directory of
this repository.
Run the following command:

```bash
go run DBMS
```

BY default tables are saved in the `./data` directory. 
However, the storage location can be changed flexibly before runtime using the DATA_DIR property in the .env file.

## 4. Demo
A couple of example queries, that showcase the possibilities of this DB can be found in the file [SQLQueries.md](SQLQueries.md)