package database

import (
	"DBMS/fs"
	"errors"
	"os"
	"regexp"
)

func CreateDatabase(dbName string) error {
	_, err := os.Stat(os.Getenv("DATA_DIR") + "/" + dbName)
	if err == nil {
		return errors.New("database already exists")
	}

	if match, _ := regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9_.-]{1,64}$`, dbName); !match {
		return errors.New("invalid database name. Must start with a letter and be between 1 and 64 characters long")
	}

	return fs.Create(os.Getenv("DATA_DIR") + "/" + dbName)
}

func DeleteDatabase(db Database) error {
	return fs.Delete(db.Path)
}
