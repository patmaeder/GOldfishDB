package value

import "strconv"

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

func (v RealValue) Passes(constraint Constraint) bool {
	switch constraint.Operator {
	case EQUAL:
		return v.Value == constraint.Value.(RealValue).Value
	case NOT_EQUAL:
		return v.Value != constraint.Value.(RealValue).Value
	case LT:
		return v.Value < constraint.Value.(RealValue).Value
	case LT_EQUAL:
		return v.Value <= constraint.Value.(RealValue).Value
	case GT:
		return v.Value > constraint.Value.(RealValue).Value
	case GT_EQUAL:
		return v.Value >= constraint.Value.(RealValue).Value
	}
	return false
}

func (v RealValue) ToString() string {
	return strconv.Itoa(int(v.Value))
}
