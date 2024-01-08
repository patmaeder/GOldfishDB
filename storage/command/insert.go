package command

import (
	"DBMS/storage"
	"DBMS/storage/value"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

type InsertCommand struct {
	Table storage.Table
	Rows  []storage.Row
}

func Insert(table storage.Table, rows []storage.Row) InsertCommand {
	return InsertCommand{
		Table: table,
		Rows:  rows,
	}
}

func (c InsertCommand) Execute() error {
	if !c.Table.Exists() {
		return errors.New("a table with the given name does not exist")
	}

	idbFile, err := os.OpenFile(c.Table.GetIdbFilePath(), os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	defer idbFile.Close()

	buffer := new(bytes.Buffer)
	for _, row := range c.Rows {
		for _, column := range c.Table.Columns {
			entryValue, exists := row.Entries[column.Name]
			if exists {
				binary.Write(buffer, binary.LittleEndian, entryValue)
				continue
			}

			switch column.Type {
			case storage.INTEGER:
				binary.Write(buffer, binary.LittleEndian, value.IntegerNull())
			case storage.REAL:
				binary.Write(buffer, binary.LittleEndian, value.RealNull())
			case storage.BOOLEAN:
				binary.Write(buffer, binary.LittleEndian, value.BooleanNull())
			case storage.TEXT:
				binary.Write(buffer, binary.LittleEndian, value.TextNull())
			}
		}
	}

	_, err = idbFile.Write(buffer.Bytes())
	if err != nil {
		return err
	}

	return nil
}
