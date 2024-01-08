package value

const TextLength = 1024

type TextValue struct {
	Value [1024]byte
}

func Text(value string) TextValue {
	valueBytes := []byte(value)
	valueByteArray := append(valueBytes, make([]byte, 1024-len(valueBytes))...)
	return TextValue{
		Value: [1024]byte(valueByteArray),
	}
}

func TextNull() TextValue {
	return TextValue{}
}
