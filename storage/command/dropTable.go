package command

import (
	"DBMS/storage"
	"errors"
	"os"
)

type DropTable struct {
	Table    storage.Table
	IfExists bool
}

func (c DropTable) Validate() any {
	if !c.Table.Exists() {
		if c.IfExists {
			return "CODE 200: deleted"
		}
		return errors.New("a table with the name " + c.Table.Name + " does not exists")
	}

	return nil
}

func (c DropTable) Execute() any {
	err := c.Validate()
	if err != nil {
		return err
	}

	err1 := os.Remove(c.Table.GetFrmFilePath())
	err2 := os.Remove(c.Table.GetIdbFilePath())
	if err := errors.Join(err1, err2); err != nil {
		return err
	}

	return "CODE 200: deleted"
}
