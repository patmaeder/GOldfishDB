package table

import (
	"DBMS/storage/database"
	"errors"
	"os"
)

type Table struct {
	Database database.Database
	Name     string
	FrmPath  string
	IdbPath  string
}

func NewTable(dbName string, name string) (Table, error) {
	db, err := database.New(dbName)
	if err != nil {
		return Table{}, err
	}

	_, err = os.Stat(db.Path + "/" + name + ".frm")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Table{}, errors.New("table does not exist")
		}
		return Table{}, err
	}

	return Table{
		Database: db,
		Name:     name,
		FrmPath:  db.Path + "/" + name + ".frm",
		IdbPath:  db.Path + "/" + name + ".idb",
	}, nil
}
