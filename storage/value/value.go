package value

type Value interface {
	Passes(constraint Constraint) bool
	IsNULL() bool
	ToString() string
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
