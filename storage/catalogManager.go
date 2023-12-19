package storage

import (
	"DBMS/fs"
	"os"

	_ "github.com/joho/godotenv/autoload"
)

var dataDirectory = os.Getenv("DATA_DIR")

func CreateDatabase(dbName string) error {
	err := fs.Create(dataDirectory + "/" + dbName + "/index.txt")
	if err != nil {
		return err
	}

	return nil
}
