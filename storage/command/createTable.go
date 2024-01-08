package command

import (
	"DBMS/storage"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

type CreateTableCommand struct {
	Tablename   string
	IfNotExists bool
	Columns     []storage.Column
}

func CreateTable(name string, ifNotExists bool, columns []storage.Column) *CreateTableCommand {
	return &CreateTableCommand{
		Tablename:   name,
		IfNotExists: ifNotExists,
		Columns:     columns,
	}
}

func (c *CreateTableCommand) Execute() error {
	// TODO: Move this to parser
	/*if match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_\-]{1,64}$`, c.Name); !match {
		return errors.New("invalid table name. Must start with a letter or an underscore and be between 1 and 64 characters long")
	}*/

	FrmFilePath, IdbFilePath, err := storage.GetPathsFromTablename(c.Tablename)
	if err == nil {
		if c.IfNotExists {
			return nil
		}
		return errors.New("a table with the provided name already exists")
	}

	frmFile, err1 := os.Create(FrmFilePath)
	_, err2 := os.Create(IdbFilePath)
	if err := errors.Join(err1, err2); err != nil {
		return err
	}
	defer frmFile.Close()

	buffer := new(bytes.Buffer)
	for _, column := range c.Columns {
		err = binary.Write(buffer, binary.LittleEndian, column)
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
