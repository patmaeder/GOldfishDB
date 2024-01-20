package Parser

import (
	"slices"
	"strings"
)

func isMethod(tokenValue string) bool {
	sqlMethods := []string{"CREATE", "ALTER", "DROP", "INSERT", "SELECT", "DELETE", "ADD", "RENAME"}
	return slices.Contains(sqlMethods, strings.ToUpper(tokenValue))
}

func isMethodModifier(tokenValue string) bool {
	methodModifier := []string{"TABLE", "COLUMN"}
	return slices.Contains(methodModifier, strings.ToUpper(tokenValue))
}

func isMethodCondition(tokenValue string) bool {
	return strings.ToUpper(tokenValue) == "IF"
}

func isDatatype(tokenValue string) bool {
	dataTypes := []string{"BOOLEAN", "INTEGER", "REAL", "TEXT"}
	return slices.Contains(dataTypes, strings.ToUpper(tokenValue))
}

func isSet(tokenValue string) bool {
	return strings.ToUpper(tokenValue) == "SET"
}

func isValues(tokenValue string) bool {
	return strings.ToUpper(tokenValue) == "VALUES"
}

func isWhere(tokenValue string) bool {
	return strings.ToUpper(tokenValue) == "WHERE"
}

func isOrderBy(tokenValue string) bool {
	return strings.ToUpper(tokenValue) == "WHERE"
}
