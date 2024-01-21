package value

type Value interface {
	Passes(constraint Constraint) bool
	Equals(value Value) bool
	IsNULL() bool
	Smaller(value Value) bool
	Greater(value Value) bool
	ToString() string
	Increment(step int) any
}

type Constraint struct {
	Operator ConstraintOperator
	Value    Value
}

type ConstraintOperator int

const (
	EQUAL ConstraintOperator = iota
	NOT_EQUAL
	LT
	LT_EQUAL
	GT
	GT_EQUAL
	IN      // Currently not supported
	LIKE    // Currently not supported
	BETWEEN // Currently not supported
)
