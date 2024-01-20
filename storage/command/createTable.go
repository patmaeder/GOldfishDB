package command

import (
	"DBMS/storage"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"regexp"
	"slices"
)

type CreateTable struct {
	Table       storage.Table
	IfNotExists bool
}

func (c CreateTable) Validate() any {
	if match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_\-]{1,64}$`, c.Table.Name); !match {
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

		if match, _ := regexp.MatchString(`^[_a-zA-Z][a-zA-Z0-9_]*$`, string(column.Name[:])); !match {
			return errors.New("invalid column name. Must start with a letter or an underscore and be between 1 and 32 characters long")
		}

		if slices.Contains(previousColumnNames, column.Name) {
			return errors.New("column with name " + string(column.Name[:]) + " already declared previously")
		}

		if column.Primary && !column.NotNullable {
			return errors.New("a primary column must not be nullable")
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
	defer frmFile.Close()

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
	if err != nil {
		return err
	}
	return "CODE 201: created"
}
