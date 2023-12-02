package cmd

type Command interface {
	Start() error
	Stop() error
}
