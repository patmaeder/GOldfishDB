package command

import (
	"DBMS/storage"
	"DBMS/utils"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"regexp"
	"slices"
	"syscall"
)

type CreateTable struct {
	Table       storage.Table
	IfNotExists bool
}

func (c CreateTable) Validate() any {
	if matched, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_\-]{1,63}$`, c.Table.Name); !matched {
		return errors.New("invalid table name. Must start with a letter or an underscore and be between 1 and 64 characters long")
	}

	if c.Table.Exists() {
		if c.IfNotExists {
			return "CODE 201: created"
		}
		return errors.New("a table with the name " + c.Table.Name + " already exists")
	}

	previousColumnNames := make([][128]byte, 0)
	for _, column := range c.Table.Columns {
		if matched, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_\-]{1,31}$`, utils.ByteArrayToString(column.Name[:])); !matched {
			return errors.New("invalid column name. Must start with a letter or an underscore and be between 1 and 32 characters long")
		}

		if slices.Contains(previousColumnNames, column.Name) {
			return errors.New("column with name " + utils.ByteArrayToString(column.Name[:]) + " already declared previously")
		}

		if column.Autoincrement && !(column.Type == storage.INTEGER || column.Type == storage.REAL) {
			return errors.New("only columns of type INTEGER or REAL can be AUTOINCREMENT")
		}

		previousColumnNames = append(previousColumnNames, column.Name)
	}

	return nil
}

func (c CreateTable) Execute() any {
	err := c.Validate()
	if err != nil {
		return err
	}

	frmFile, err1 := os.Create(c.Table.GetFrmFilePath())
	_, err2 := os.Create(c.Table.GetIdbFilePath())
	if err := errors.Join(err1, err2); err != nil {
		return err
	}
	defer func() {
		frmFile.Close()
		frmFile.Sync()
		syscall.Flock(int(frmFile.Fd()), syscall.LOCK_UN)
	}()

	err = syscall.Flock(int(frmFile.Fd()), syscall.LOCK_EX)
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer([]byte{})
	err = binary.Write(buffer, binary.LittleEndian, c.Table.RowLength)
	if err != nil {
		return err
	}

	for _, column := range c.Table.Columns {
		err := binary.Write(buffer, binary.LittleEndian, column)
		if err != nil {
			return err
		}
	}

	writer := bufio.NewWriter(frmFile)
	_, err = writer.Write(buffer.Bytes())
	writer.Flush()
	if err != nil {
		return err
	}
	return "CODE 201: created"
}
