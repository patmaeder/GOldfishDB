package value

import "strconv"

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

func (v BooleanValue) Passes(constraint Constraint) bool {
	switch constraint.Operator {
	case EQUAL:
		return v.Value == constraint.Value.(BooleanValue).Value
	case NOT_EQUAL:
		return v.Value != constraint.Value.(BooleanValue).Value
	case LT:
		return false
	case LT_EQUAL:
		return false
	case GT:
		return false
	case GT_EQUAL:
		return false
	}
	return false
}

func (v BooleanValue) IsNULL() bool {
	if v == BooleanNull() {
		return true
	}
	return false
}

func (v BooleanValue) ToString() string {
	return strconv.FormatBool(v.Value)
}
