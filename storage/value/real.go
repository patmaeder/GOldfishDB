package value

const RealLength = 8

type RealValue struct {
	Value int64
}

func Real(value int) RealValue {
	return RealValue{
		Value: int64(value),
	}
}

func RealNull() RealValue {
	return RealValue{}
}
