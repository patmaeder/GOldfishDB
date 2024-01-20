package value

import "strconv"

const IntegerLength = 4

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

func (v IntegerValue) Passes(constraint Constraint) bool {
	switch constraint.Operator {
	case EQUAL:
		return v.Value == constraint.Value.(IntegerValue).Value
	case NOT_EQUAL:
		return v.Value != constraint.Value.(IntegerValue).Value
	case LT:
		return v.Value < constraint.Value.(IntegerValue).Value
	case LT_EQUAL:
		return v.Value <= constraint.Value.(IntegerValue).Value
	case GT:
		return v.Value > constraint.Value.(IntegerValue).Value
	case GT_EQUAL:
		return v.Value >= constraint.Value.(IntegerValue).Value
	}
	return false
}

func (v IntegerValue) IsNULL() bool {
	if v == IntegerNull() {
		return true
	}
	return false
}

func (v IntegerValue) ToString() string {
	return strconv.Itoa(int(v.Value))
}
