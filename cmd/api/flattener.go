package api

import "github.com/jeremywohl/flatten"

type FlattenFunc func(string) (string, error)

func Flatten(input string) (string, error) {
	flat, err := flatten.FlattenString(input, "", flatten.UnderscoreStyle)
	return flat, err
}
