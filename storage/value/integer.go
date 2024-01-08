package value

const IntegerLength = 32

type IntegerValue struct {
	Value int32
}

func Integer(value int) IntegerValue {
	return IntegerValue{
		Value: int32(value),
	}
}

func IntegerNull() IntegerValue {
	return IntegerValue{}
}
