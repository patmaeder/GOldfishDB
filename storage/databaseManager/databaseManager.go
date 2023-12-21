package databaseManager

import (
	"DBMS/fs"
	"DBMS/storage"
	"errors"
	"os"
	"regexp"

	_ "github.com/joho/godotenv/autoload"
)

var dataDirectory = os.Getenv("DATA_DIR")

func CreateDatabase(db storage.Database) error {
	if DoesExist(db) {
		return errors.New("database already exists")
	}

	if match, _ := regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9_.-]{1,64}$`, db.Name); !match {
		return errors.New("invalid database name. Must start with a letter and be between 1 and 64 characters long")
	}

	return fs.Create(db.GetPath())
}

func DeleteDatabase(db storage.Database) error {
	if !DoesExist(db) {
		return errors.New("database does not exist")
	}

	return fs.Delete(db.GetPath())
}

func DoesExist(db storage.Database) bool {
	_, err := os.Stat(db.GetPath())
	if err != nil {
		return false
	}
	return true
}
