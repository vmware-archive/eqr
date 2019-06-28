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
	"github.com/sirupsen/logrus"

	cash "github.com/carbonblack/eqr/ruleset/cacher"
	"github.com/carbonblack/eqr/logging"
)

type pluginInterface string

var log = logging.GetLogger()

func (g pluginInterface) Runnable(args ...interface{}) (result interface{}, err error) {

	if len(args) == 0 {
		return false, errors.New("Empty Cache Function")
	}

	log.WithFields(logrus.Fields{
	    "arg1": args[0],
	    "arg2": args[1],
	}).Debug("Cache operator arguments")

	cash.AddCache(args[0].(string), args[1].(string))

	return true, nil
}

func (g pluginInterface) Name() string {
	return "CACHE"
}

func (g pluginInterface) TypeName() string {
	return "FUNCTION"
}

// PluginInterface exported
var PluginInterface pluginInterface
