package storage

type DataType uint8

const (
	INTEGER DataType = iota + 1
	REAL
	TEXT
	BOOLEAN
)

const columnLength = 128 + 1 + 8 + 1 + 1 + 1 + 1

type Column struct {
	Name          [128]byte
	Type          DataType
	Offset        int64
	Primary       bool
	Autoincrement bool
	Unique        bool
	Nullable      bool
	//Foreign Key
	//Default
}

func NewColumn(name [128]byte, dataType DataType, primary bool, autoincrement bool, unique bool, nullable bool) Column {
	return Column{
		Name:          name,
		Type:          dataType,
		Primary:       primary,
		Autoincrement: autoincrement,
		Unique:        unique,
		Nullable:      nullable,
	}
}
