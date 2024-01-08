package command

import (
	"DBMS/storage"
	"errors"
	"os"
)

type DropTableCommand struct {
	Table    storage.Table
	IfExists bool
}

func DropTable(table storage.Table, ifExists bool) DropTableCommand {
	return DropTableCommand{
		Table:    table,
		IfExists: ifExists,
	}
}

func (c DropTableCommand) Execute() error {
	if !c.Table.Exists() {
		if c.IfExists {
			return nil
		}
		return errors.New("a table with the provided name does not exists")
	}

	err1 := os.Remove(c.Table.GetFrmFilePath())
	err2 := os.Remove(c.Table.GetIdbFilePath())
	if err := errors.Join(err1, err2); err != nil {
		return err
	}

	return nil
}
