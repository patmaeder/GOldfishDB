package value

const BooleanLength = 1

type BooleanValue struct {
	Value bool
}

func Boolean(value bool) BooleanValue {
	return BooleanValue{
		Value: value,
	}
}

func BooleanNull() BooleanValue {
	return BooleanValue{}
}
