package value

import "DBMS/utils"

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

func (v TextValue) Passes(constraint Constraint) bool {
	switch constraint.Operator {
	case EQUAL:
		return v.Value == constraint.Value.(TextValue).Value
	case NOT_EQUAL:
		return v.Value != constraint.Value.(TextValue).Value
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

func (v TextValue) Equals(value Value) bool {
	return v.Value == value.(TextValue).Value
}

func (v TextValue) Smaller(value Value) bool {
	textValue := value.(*TextValue)
	return utils.ByteArrayToString(v.Value[:]) < utils.ByteArrayToString(textValue.Value[:])
}

func (v TextValue) Greater(value Value) bool {
	textValue := value.(*TextValue)
	return utils.ByteArrayToString(v.Value[:]) > utils.ByteArrayToString(textValue.Value[:])
}

func (v TextValue) IsNULL() bool {
	if v == TextNull() {
		return true
	}
	return false
}

func (v TextValue) ToString() string {
	return "'" + utils.ByteArrayToString(v.Value[:]) + "'"
}

func (v TextValue) Increment(step int) any {
	return v
}
