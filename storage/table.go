package storage

import (
	"DBMS/storage/value"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

const RowLengthOffset = 8

type Table struct {
	Name      string
	RowLength int64
	Columns   []Column
}

func NewTable(tableName string, columns []*Column) (Table, error) {

	rowLenght := int64(0)
	dereferencedColumns := make([]Column, 0)
	for _, column := range columns {
		column.Offset = rowLenght

		switch column.Type {
		case INTEGER:
			rowLenght += value.IntegerLength
		case REAL:
			rowLenght += value.RealLength
		case BOOLEAN:
			rowLenght += value.BooleanLength
		case TEXT:
			rowLenght += value.TextLength
		}

		dereferencedColumns = append(dereferencedColumns, *column)
	}

	return Table{
		Name:      tableName,
		RowLength: rowLenght,
		Columns:   dereferencedColumns,
	}, nil
}

// Exists checks if the given table exists
func (t Table) Exists() bool {
	_, err1 := os.Stat(t.GetFrmFilePath())
	_, err2 := os.Stat(t.GetIdbFilePath())
	if err := errors.Join(err1, err2); err == nil {
		return true
	}
	return false
}

// GetFrmFilePath returns the local path to the .frm file of the table
func (t Table) GetFrmFilePath() string {
	return os.Getenv("DATA_DIR") + "/" + t.Name + ".frm"
}

// GetIdbFilePath returns the local path to the .idb file of the table
func (t Table) GetIdbFilePath() string {
	return os.Getenv("DATA_DIR") + "/" + t.Name + ".idb"
}

func GetTable(tableName string) (Table, error) {
	table := Table{Name: tableName}
	if !table.Exists() {
		return Table{}, errors.New("a table with the given name does not exist")
	}

	frmFile, _ := os.OpenFile(table.GetFrmFilePath(), os.O_RDONLY, 0444)
	reader := bufio.NewReader(frmFile)
	defer frmFile.Close()
	frmFileStat, _ := frmFile.Stat()

	// Read RowLength
	buffer := make([]byte, RowLengthOffset)
	_, err := reader.Read(buffer)
	if err != nil {
		return Table{}, err
	}

	var rowLength int64
	err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &rowLength)
	if err != nil {
		return Table{}, err
	}

	// Read Columns
	columns := make([]Column, 0)
	columnCount := int64(0)

	for columnCount*columnLength+RowLengthOffset < frmFileStat.Size() {
		buffer := make([]byte, columnLength)
		frmFile.Seek(columnCount*columnLength+RowLengthOffset, 0)
		_, err := reader.Read(buffer)
		if err != nil {
			return Table{}, err
		}

		var column Column
		err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &column)
		if err != nil {
			return Table{}, err
		}

		columns = append(columns, column)
		columnCount++
	}

	table.RowLength = rowLength
	table.Columns = columns
	return table, nil
}

// ConvertColumnsToMap converts columns slice to a map with the column names as keys
func (t Table) ConvertColumnsToMap() map[[128]byte]Column {
	columnMap := make(map[[128]byte]Column)
	for _, column := range t.Columns {
		columnMap[column.Name] = column
	}

	return columnMap
}
