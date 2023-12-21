package table

import (
	"DBMS/fs"
	"DBMS/storage/database"
	"errors"
	"os"
	"regexp"
)

func CreateTable(db database.Database, tableName string) error {
	_, err := os.Stat(db.Path + "/" + tableName + ".frm")
	if err == nil {
		return errors.New("table already exists")
	}

	if match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_\-]{1,64}$`, tableName); !match {
		return errors.New("invalid table name. Must start with a letter or an underscore and be between 1 and 64 characters long")
	}

	err1 := fs.Create(db.Path + "/" + tableName + ".frm")
	err2 := fs.Create(db.Path + "/" + tableName + ".idb")

	if err := errors.Join(err1, err2); err != nil {
		return err
	}

	return nil
}

func DeleteTable(table Table) error {
	err1 := fs.Delete(table.FrmPath)
	err2 := fs.Delete(table.IdbPath)

	if err := errors.Join(err1, err2); err != nil {
		return err
	}

	return nil
}
