package main

import (
	"errors"
	"fmt"
)

type destpluginInterface string


func (g destpluginInterface) Initialize(args ...interface{}) (result interface{}, err error) {

	return nil, nil
}

func (g destpluginInterface) Consume(args ...interface{}) (err error) {
	return errors.New("consume not implemented")
}

func (g destpluginInterface) DoCheckpoint() bool {
	return false
}

func (g destpluginInterface) Publish(args ...interface{}) (result bool, err error) {
	fmt.Printf("The value(s) to be sent downstream -> \n`%v`\n", string(*args[1].(*[]byte)))

	return true, nil
}


func (g destpluginInterface) Name() string {
	return "DUMMY.DEST"
}

func (g destpluginInterface) TypeName() string {

	return "CONSUMER"
}

// PluginInterface exported
var IOPluginInterface destpluginInterface

