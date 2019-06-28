package main

import (
	"fmt"
	"strings"
	"errors"
)

type pluginInterface string

func (g pluginInterface) Runnable(args ...interface{}) (result interface{}, err error) {

	if len(args) <= 2 {
		// we don't support less than this, break out
		return nil, errors.New("Number of arguments passed is to little, please check.")
	}

	var seperator string
	var passback string

	switch strings.ToUpper(args[0].(string)) {
	case "COMMA":
		seperator = ","
	case "SEMICOLON":
		seperator = ";"
	case "COLON":
		seperator = ":"
	case "DASH":
		fallthrough
	case "HYPTHEN":
		seperator = "-"
	case "DOT":
		fallthrough
	case "PERIOD":
		seperator = "."
	}

	switch x := args[1].(type) {
	case []interface{}:
		fmt.Printf("Length of stuff...%v\n", len(x))
		for i := 0; i < len(x); i++ {
			if i != 0 {
				passback += seperator
			}

			passback += fmt.Sprintf("%v", x[i])
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unsupported type: %T\n", x))
	}

	return passback, nil
}

func (g pluginInterface) Name() string {
	return "JOIN"
}

func (g pluginInterface) TypeName() string {
	return "FUNCTION"
}

// PluginInterface exported
var PluginInterface pluginInterface
