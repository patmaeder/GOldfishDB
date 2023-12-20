package tableManager

import (
	"DBMS/fs"
	"errors"
	"os"
	"regexp"

	_ "github.com/joho/godotenv/autoload"
)

var dataDirectory = os.Getenv("DATA_DIR")

func CreateTable(dbName string, tableName string) error {
	if _, err := os.Stat(dataDirectory + "/" + dbName); err != nil {
		if os.IsNotExist(err) {
			return errors.New("database does not exist")
		}
	}

	if _, err := os.Stat(dataDirectory + "/" + dbName + "/" + tableName + ".frm"); err == nil {
		return errors.New("table with the given name already exists")
	}

	validTableNamePattern := `^[a-zA-Z_][a-zA-Z0-9_\-]{1,64}$`
	if match, _ := regexp.MatchString(validTableNamePattern, tableName); !match {
		return errors.New("invalid table name. Must start with a letter or an underscore and be between 1 and 64 characters long")
	}

	if err := fs.Create(dataDirectory + "/" + dbName + "/" + tableName + ".frm"); err != nil {
		return err
	}

	if err := fs.Create(dataDirectory + "/" + dbName + "/" + tableName + ".idb"); err != nil {
		return err
	}

	return nil
}
