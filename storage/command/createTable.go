package command

import (
	"DBMS/storage"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

type CreateTableCommand struct {
	Table       storage.Table
	IfNotExists bool
}

func CreateTable(table storage.Table, ifNotExists bool) CreateTableCommand {
	return CreateTableCommand{
		Table:       table,
		IfNotExists: ifNotExists,
	}
}

func (c CreateTableCommand) Execute() error {
	// TODO: Move this to parser
	/*if match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_\-]{1,64}$`, c.Name); !match {
		return errors.New("invalid table name. Must start with a letter or an underscore and be between 1 and 64 characters long")
	}*/

	if c.Table.Exists() {
		if c.IfNotExists {
			return nil
		}
		return errors.New("a table with the provided name already exists")
	}

	frmFile, err1 := os.Create(c.Table.GetFrmFilePath())
	_, err2 := os.Create(c.Table.GetIdbFilePath())
	if err := errors.Join(err1, err2); err != nil {
		return err
	}
	defer frmFile.Close()

	buffer := new(bytes.Buffer)
	err := binary.Write(buffer, binary.LittleEndian, c.Table.RowLength)
	if err != nil {
		return err
	}

	for _, column := range c.Table.Columns {
		err := binary.Write(buffer, binary.LittleEndian, column)
		if err != nil {
			return err
		}
	}

	_, err = frmFile.Write(buffer.Bytes())
	if err != nil {
		return err
	}

	return nil
}
