package command

import (
	"DBMS/storage"
	"DBMS/storage/value"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"slices"
)

type SelectCommand struct {
	Table  storage.Table
	Fields [][128]byte
}

func Select(table storage.Table, fields [][128]byte) SelectCommand {
	/*convertedFields := make([][128]byte, 0)

	for _, field := range fields {
		fieldBytes := []byte(field)
		fieldByteArray := append(fieldBytes, make([]byte, 128-len(fieldBytes))...)
		convertedFields = append(convertedFields, [128]byte(fieldByteArray))
	}*/

	return SelectCommand{
		Table:  table,
		Fields: fields,
	}
}

func (c SelectCommand) Execute() ([]storage.Row, error) {
	if !c.Table.Exists() {
		return nil, errors.New("a table with the given name does not exist")
	}

	idbFile, _ := os.Open(c.Table.GetIdbFilePath())
	idbFileStat, _ := idbFile.Stat()
	defer idbFile.Close()

	rowLenght := int64(0)
	rowCount := int64(0)
	offsets := make(map[int64]*storage.Column)

	for _, column := range c.Table.Columns {
		if slices.Contains(c.Fields, column.Name) {
			offsets[rowLenght] = column
		}

		switch column.Type {
		case storage.INTEGER:
			rowLenght += value.IntegerLength
		case storage.REAL:
			rowLenght += value.RealLength
		case storage.BOOLEAN:
			rowLenght += value.BooleanLength
		case storage.TEXT:
			rowLenght += value.TextLength
		}
	}

	result := make([]storage.Row, 0)
	for rowCount*rowLenght < idbFileStat.Size() {
		row := storage.NewRow(make(map[[128]byte]any))

		for offset, column := range offsets {
			bufferSize := int64(0)
			var mappingValue interface{}

			switch column.Type {
			case storage.INTEGER:
				bufferSize = value.IntegerLength
				mappingValue = new(value.IntegerValue)
			case storage.REAL:
				bufferSize = value.RealLength
				mappingValue = new(value.RealValue)
			case storage.BOOLEAN:
				bufferSize = value.BooleanLength
				mappingValue = new(value.BooleanValue)
			case storage.TEXT:
				bufferSize = value.TextLength
				mappingValue = new(value.TextValue)
			}

			buffer := make([]byte, bufferSize)
			_, err := idbFile.ReadAt(buffer, rowCount*rowLenght+offset)
			if err != nil {
				return nil, err
			}

			err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, mappingValue)
			if err != nil {
				return nil, err
			}

			row.Entries[column.Name] = mappingValue
		}

		result = append(result, row)
		rowCount++
	}

	return result, nil
}
