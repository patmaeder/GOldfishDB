package command

import (
	"DBMS/storage"
	"DBMS/storage/value"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

type Insert struct {
	Table storage.Table
	Rows  []storage.Row
}

func (c Insert) Validate() any {
	for _, row := range c.Rows {
		for _, column := range c.Table.Columns {
			if column.NotNullable && !column.Autoincrement {
				if _, exists := row.Entries[column.Name]; !exists {
					return errors.New("column " + string(column.Name[:]) + " cannot be null")
				}
			}

			// TODO: Check for UNIQUE constraint
		}
	}

	return nil
}

func (c Insert) Execute() any {
	err := c.Validate()
	if err != nil {
		return err
	}

	idbFile, err := os.OpenFile(c.Table.GetIdbFilePath(), os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	writer := bufio.NewWriter(idbFile)
	defer func() {
		idbFile.Close()
		syscall.Flock(int(idbFile.Fd()), syscall.LOCK_UN)
	}()

	// Lock file to other goroutines
	err = syscall.Flock(int(idbFile.Fd()), syscall.LOCK_EX)
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer([]byte{})
	for _, row := range c.Rows {
		for _, column := range c.Table.Columns {

			// TODO: AUTOINCREMENT

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

	// TODO: SEEK to EOF
	_, err = writer.Write(buffer.Bytes())
	if err != nil {
		return err
	}
	return "CODE 201: inserted " + string(len(c.Rows)) + " records"
}
