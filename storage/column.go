package storage

type DataType uint8

const (
	INTEGER DataType = iota + 1
	REAL
	TEXT
	BOOLEAN
)

const columnLength = 128 + 1 + 1 + 1 + 1 + 1

type Column struct {
	Name          [128]byte
	Type          DataType
	Primary       bool
	Autoincrement bool
	Unique        bool
	Nullable      bool
	//Default       any
	// TODO: Add support for foreign key
}

func NewColumn(name string, dataType DataType, primary bool) Column {
	nameBytes := []byte(name)
	nameByteArray := append(nameBytes, make([]byte, 128-len(nameBytes))...)
	return Column{
		Name:    [128]byte(nameByteArray),
		Type:    dataType,
		Primary: primary,
	}
}
