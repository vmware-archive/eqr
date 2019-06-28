package main

import (
	"reflect"
)

type pluginInterface string

func (g pluginInterface) Runnable(args ...interface{}) (result interface{}, err error) {

	// JSON_EXT(augmentedBehaviorEvent.deviceType, deviceEventLog.deviceType)
	// get the two values, which ever is not NULL first send back, otherwise ""

	if len(args) <= 1 {
		return "", nil
	}

	var F1BitFlag bool
	var F2BitFlag bool

	fieldOne := args[0]
	fieldTwo := args[1]

	if fieldOne == nil && fieldTwo == nil {
		return "", nil
	}

	if reflect.TypeOf(fieldOne).Kind() == reflect.String {
		if fieldOne.(string) == "null" {
			F1BitFlag = true
		}
	}

	if reflect.TypeOf(fieldTwo).Kind() == reflect.String {
		if fieldTwo.(string) == "null" {
			F2BitFlag = true
		}
	}

	if F1BitFlag && F2BitFlag {
		return "", nil
	} else if F1BitFlag {
		return fieldTwo, nil
	} else {
		return fieldOne, nil
	}
}

func (g pluginInterface) Name() string {
	return "FIRST_NON_NULL"
}

func (g pluginInterface) TypeName() string {
	return "FUNCTION"
}

// PluginInterface exported
var PluginInterface pluginInterface
