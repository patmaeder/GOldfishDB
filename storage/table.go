package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

type Table struct {
	Name    string
	Columns []Column
}

func NewTable(tableName string, columns []Column) (*Table, error) {
	return &Table{
		Name:    tableName,
		Columns: columns,
	}, nil
}

func GetTable(tableName string) (*Table, error) {
	frmFilePath, _, err := GetPathsFromTablename(tableName)
	if err != nil {
		return nil, err
	}

	frmFile, err := os.Open(frmFilePath)
	if err != nil {
		return nil, err
	}
	defer frmFile.Close()

	frmFileStat, _ := frmFile.Stat()
	if err != nil {
		return nil, err
	}

	columns := make([]Column, 0)
	columnCount := int64(0)
	buffer := make([]byte, columnLength)

	for columnCount*columnLength < frmFileStat.Size() {
		_, err = frmFile.ReadAt(buffer, columnCount*columnLength)
		if err != nil {
			return nil, err
		}

		var column Column
		err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &column)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
		columnCount++
	}

	return NewTable(tableName, columns)
}

func GetPathsFromTablename(tableName string) (string, string, error) {
	frmFilePath := os.Getenv("DATA_DIR") + "/" + tableName + ".frm"
	idbFilePath := os.Getenv("DATA_DIR") + "/" + tableName + ".idb"

	_, err1 := os.Stat(frmFilePath)
	_, err2 := os.Stat(idbFilePath)
	if err := errors.Join(err1, err2); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return frmFilePath, idbFilePath, errors.New("a table with the provided does not exists")
		}
		return frmFilePath, idbFilePath, err
	}

	return frmFilePath, idbFilePath, nil
}
