package value

const TextLength = 1024

type TextValue struct {
	Value [1024]byte
}

func Text(value [1024]byte) TextValue {
	return TextValue{
		Value: value,
	}
}

func TextNull() TextValue {
	return TextValue{}
}
