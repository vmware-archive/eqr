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
	"reflect"
	"strings"
)

type pluginInterface string

func (g pluginInterface) Runnable(args ...interface{}) (result interface{}, err error) {

	if len(args) <= 1 {
		return "", nil
	} else if args[0] == nil {
		return "", nil
	} else if reflect.TypeOf(args[0]).Kind() != reflect.String {
		return "", nil
	} else if strings.Contains(args[0].(string), "JSON(") {
		return "", nil
	} else if strings.Contains(args[0].(string), "null") {
		return "", nil
	}
	var slashDir string
	if strings.ToUpper(strings.TrimSpace(args[1].(string))) == "WINDOWS" {
		slashDir = "\\"
	} else {
		slashDir = "/"
	}

	var index = strings.LastIndex(args[0].(string), slashDir )

	return args[0].(string)[:index], nil
}

func (g pluginInterface) Name() string {
	return "GETDIR"
}

func (g pluginInterface) TypeName() string {
	return "FUNCTION"
}

// PluginInterface exported
var PluginInterface pluginInterface
