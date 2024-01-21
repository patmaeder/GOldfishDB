package command

import (
	"DBMS/storage"
	"DBMS/storage/value"
	"DBMS/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

type Insert struct {
	Table storage.Table
	Rows  []storage.Row
}

func (c Insert) Validate() any {
	idbFile, _ := os.OpenFile(c.Table.GetIdbFilePath(), os.O_RDONLY, 0444)
	idbFileStat, _ := idbFile.Stat()
	defer idbFile.Close()

	for _, row := range c.Rows {
		for _, column := range c.Table.Columns {
			if column.Autoincrement {
				if _, exists := row.Entries[column.Name]; exists {
					return errors.New("value of column " + utils.ByteArrayToString(column.Name[:]) + " cannot be set manually due to AUTOINCREMENT constraint")
				}
			}

			if column.NotNullable && !column.Autoincrement {
				if _, exists := row.Entries[column.Name]; !exists {
					return errors.New("value of column " + utils.ByteArrayToString(column.Name[:]) + " cannot be null")
				}
			}

			if column.NotNullable && column.Unique && !column.Autoincrement && idbFileStat.Size() > c.Table.RowLength {
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

				rowCount := int64(0)
				for rowCount*c.Table.RowLength < idbFileStat.Size() {
					buffer := make([]byte, bufferSize)
					_, err := idbFile.ReadAt(buffer, rowCount*c.Table.RowLength+column.Offset)
					if err != nil {
						return err
					}

					err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, mappingValue)
					if err != nil {
						return err
					}

					if mappingValue.Equals(row.Entries[column.Name]) {
						return errors.New("record with the value " + row.Entries[column.Name].ToString() + " in UNIQUE column " + utils.ByteArrayToString(column.Name[:]) + " already exists")
					}

					rowCount++
				}
			}
		}
	}

	return nil
}

func (c Insert) Execute() any {
	err := c.Validate()
	if err != nil {
		return err
	}

	idbFile, _ := os.OpenFile(c.Table.GetIdbFilePath(), os.O_RDWR, 0644)
	idbFileStat, _ := idbFile.Stat()
	defer func() {
		idbFile.Close()
		syscall.Flock(int(idbFile.Fd()), syscall.LOCK_UN)
	}()

	err = syscall.Flock(int(idbFile.Fd()), syscall.LOCK_EX)
	if err != nil {
		return err
	}

	writeBuffer := bytes.NewBuffer([]byte{})
	for i, row := range c.Rows {

		for _, column := range c.Table.Columns {
			if column.Autoincrement {
				bufferSize := int64(0)
				var mappingValue value.Value

				switch column.Type {
				case storage.INTEGER:
					bufferSize = value.IntegerLength
					mappingValue = value.Integer(1)
				case storage.REAL:
					bufferSize = value.RealLength
					mappingValue = value.Real(1)
				}

				readBuffer := make([]byte, bufferSize)
				_, err = idbFile.ReadAt(readBuffer, idbFileStat.Size()-c.Table.RowLength+column.Offset)
				if err == nil {
					switch column.Type {
					case storage.INTEGER:
						mappingValue = new(value.IntegerValue)
					case storage.REAL:
						mappingValue = new(value.RealValue)
					}
					err = binary.Read(bytes.NewReader(readBuffer), binary.LittleEndian, mappingValue)
					if err != nil {
						return err
					}
				}

				binary.Write(writeBuffer, binary.LittleEndian, mappingValue.Increment(i))
				continue
			}

			entryValue, exists := row.Entries[column.Name]
			if exists {
				binary.Write(writeBuffer, binary.LittleEndian, entryValue)
				continue
			}

			switch column.Type {
			case storage.INTEGER:
				binary.Write(writeBuffer, binary.LittleEndian, value.IntegerNull())
			case storage.REAL:
				binary.Write(writeBuffer, binary.LittleEndian, value.RealNull())
			case storage.BOOLEAN:
				binary.Write(writeBuffer, binary.LittleEndian, value.BooleanNull())
			case storage.TEXT:
				binary.Write(writeBuffer, binary.LittleEndian, value.TextNull())
			}
		}
	}

	_, err = idbFile.WriteAt(writeBuffer.Bytes(), idbFileStat.Size())
	if err != nil {
		return err
	}
	return "CODE 201: inserted " + fmt.Sprint(len(c.Rows)) + " record(s)"
}
