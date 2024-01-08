package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

type Table struct {
	Name    string
	Columns []*Column
}

func NewTable(tableName string, columns []*Column) (Table, error) {
	return Table{
		Name:    tableName,
		Columns: columns,
	}, nil
}

func (t *Table) Exists() bool {
	_, err1 := os.Stat(t.GetFrmFilePath())
	_, err2 := os.Stat(t.GetIdbFilePath())
	if err := errors.Join(err1, err2); err == nil {
		return true
	}
	return false
}

func (t *Table) GetFrmFilePath() string {
	return os.Getenv("DATA_DIR") + "/" + t.Name + ".frm"
}

func (t *Table) GetIdbFilePath() string {
	return os.Getenv("DATA_DIR") + "/" + t.Name + ".idb"
}

func GetTable(tableName string) (Table, error) {
	table := Table{Name: tableName}
	if !table.Exists() {
		return Table{}, errors.New("a table with the given name does not exist")
	}

	frmFile, _ := os.Open(table.GetFrmFilePath())
	defer frmFile.Close()
	frmFileStat, _ := frmFile.Stat()

	columns := make([]*Column, 0)
	columnCount := int64(0)

	for columnCount*columnLength < frmFileStat.Size() {
		buffer := make([]byte, columnLength)
		_, err := frmFile.ReadAt(buffer, columnCount*columnLength)
		if err != nil {
			return Table{}, err
		}

		var column Column
		err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &column)
		if err != nil {
			return Table{}, err
		}

		columns = append(columns, &column)
		columnCount++
	}

	table.Columns = columns
	return table, nil
}
