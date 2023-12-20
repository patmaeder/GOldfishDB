package databaseManager

import (
	"DBMS/fs"
	"errors"
	"os"
	"regexp"

	_ "github.com/joho/godotenv/autoload"
)

var dataDirectory = os.Getenv("DATA_DIR")

func CreateDatabase(dbName string) error {
	if match, _ := regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9_.-]{1,64}$`, dbName); !match {
		return errors.New("invalid database name. Must start with a letter and be between 1 and 64 characters long")
	}

	if err := fs.Create(dataDirectory + "/" + dbName); err != nil {
		if os.IsExist(err) {
			return errors.New("database already exists")
		}
		return err
	}

	return nil
}

func DeleteDatabase(dbName string) error {
	if err := fs.Delete(dataDirectory + "/" + dbName); err != nil {
		if os.IsNotExist(err) {
			return errors.New("database does not exist")
		}
		return err
	}

	return nil
}
