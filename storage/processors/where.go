package processors

import (
	"DBMS/storage"
	"DBMS/storage/value"
	"bytes"
	"encoding/binary"
	"os"
)

type WhereProcessor struct {
	Table          *storage.Table
	Constraints    map[[128]byte]value.Constraint
	limitProcessor LimitProcessor
	reverse        bool
}

func Where(table *storage.Table, constraints map[[128]byte]value.Constraint) WhereProcessor {
	return WhereProcessor{
		Table:       table,
		Constraints: constraints,
	}
}

func (p WhereProcessor) Limit(limitProcessor LimitProcessor) *WhereProcessor {
	p.limitProcessor = limitProcessor
	return &p
}

func (p WhereProcessor) Reverse() *WhereProcessor {
	p.reverse = true
	return &p
}

func (p WhereProcessor) Process(resultChanel <-chan struct{}) <-chan int64 {
	ch := make(chan int64)

	go func() {
		defer close(ch)

		idbFile, _ := os.OpenFile(p.Table.GetIdbFilePath(), os.O_RDONLY, 0444)
		defer idbFile.Close()
		idbFileStat, _ := idbFile.Stat()

		columnMap := p.Table.ConvertColumnsToMap()
		rowCount := int64(0)
		hits := 0

		if p.reverse {
			rowCount = idbFileStat.Size() / p.Table.RowLength
		}

		for rowCount*p.Table.RowLength < idbFileStat.Size() {

			if p.limitProcessor.Limit > 0 && hits >= p.limitProcessor.Limit {
				break
			}

			rowValid := true
			for field, constraint := range p.Constraints {
				var buffer []byte
				var fieldValue value.Value

				switch columnMap[field].Type {
				case storage.INTEGER:
					buffer = make([]byte, value.IntegerLength)
					fieldValue = new(value.IntegerValue)
				case storage.REAL:
					buffer = make([]byte, value.RealLength)
					fieldValue = new(value.RealValue)
				case storage.BOOLEAN:
					buffer = make([]byte, value.BooleanLength)
					fieldValue = new(value.BooleanValue)
				case storage.TEXT:
					buffer = make([]byte, value.TextLength)
					fieldValue = new(value.TextValue)
				}

				_, err := idbFile.ReadAt(buffer, rowCount*p.Table.RowLength+columnMap[field].Offset)
				if err != nil {
					return
				}

				err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, fieldValue)
				if err != nil {
					return
				}

				if !fieldValue.Passes(constraint) {
					rowValid = false
					break
				}
			}

			if rowValid {
				ch <- rowCount
				hits++
			}

			if p.reverse {
				rowCount--
			} else {
				rowCount++
			}
		}
	}()

	return ch
}
