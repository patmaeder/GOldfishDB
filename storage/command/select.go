package command

import (
	"DBMS/storage"
	"DBMS/storage/processors"
	"DBMS/storage/value"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

type SelectCommand struct {
	Table  storage.Table
	Fields [][128]byte
	Where  map[[128]byte]value.Constraint
}

func Select(table storage.Table, fields [][128]byte, where map[[128]byte]value.Constraint) SelectCommand {
	return SelectCommand{
		Table:  table,
		Fields: fields,
		Where:  where,
	}
}

func (c SelectCommand) Execute() ([]storage.Row, error) {
	// TODO: Move this to validator
	if !c.Table.Exists() {
		return nil, errors.New("a table with the given name does not exist")
	}

	idbFile, _ := os.OpenFile(c.Table.GetIdbFilePath(), os.O_RDONLY, 0444)
	defer idbFile.Close()

	columnMap := c.Table.ConvertColumnsToMap()
	queryResult := make([]storage.Row, 0)
	where := processors.Where(&c.Table, c.Where)

	yield := make(chan struct{})
	for rowId := range where.Process(yield) {
		row := storage.NewRow(map[[128]byte]value.Value{})
		for _, field := range c.Fields {
			column := columnMap[field]
			bufferSize := int64(0)
			var mappingValue value.Value

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
			_, err := idbFile.ReadAt(buffer, rowId*c.Table.RowLength+column.Offset)
			if err != nil {
				return nil, err
			}

			err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, mappingValue)
			if err != nil {
				return nil, err
			}

			row.Entries[column.Name] = mappingValue
		}

		queryResult = append(queryResult, row)
	}

	return queryResult, nil
}
