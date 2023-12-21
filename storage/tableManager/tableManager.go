package tableManager

import (
	"DBMS/fs"
	"DBMS/storage"
	"DBMS/storage/databaseManager"
	"errors"
	"os"
	"regexp"
)

func CreateTable(table storage.Table) error {
	if DoesExist(table) {
		return errors.New("table already exists")
	}

	if match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_\-]{1,64}$`, table.Name); !match {
		return errors.New("invalid table name. Must start with a letter or an underscore and be between 1 and 64 characters long")
	}

	if err := fs.Create(table.GetFrmPath()); err != nil {
		return err
	}
	if err := fs.Create(table.GetIdbPath()); err != nil {
		return err
	}

	return nil
}

func DeleteTable(table storage.Table) error {
	if !DoesExist(table) {
		return errors.New("table does not exist")
	}

	if err := fs.Delete(table.GetFrmPath()); err != nil {
		return err
	}
	if err := fs.Delete(table.GetIdbPath()); err != nil {
		return err
	}

	return nil
}

func DoesExist(table storage.Table) bool {
	if !databaseManager.DoesExist(table.Database) {
		return false
	}

	_, err := os.Stat(table.GetFrmPath())
	if err != nil {
		return false
	}

	return true
}
