/*
# The MIT License (MIT)
#
# Copyright (c) 2019  Carbon Black
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
*/
package main

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/carbonblack/eqr/logging"
)

var log = logging.GetLogger()

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
	fmt.Printf("The value(s) to be sent downstream -> \n`%v`\n", )

	log.WithFields(logrus.Fields{
		"plugin": g.Name(),
		"Data": string(args[1].([]byte)),
	}).Info("Dummy Destination Publish Function")

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

