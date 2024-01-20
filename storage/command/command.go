package command

type Command interface {
	Validate() any
	Execute() any
}
