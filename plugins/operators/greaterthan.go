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
	"strconv"
	"strings"
)

type pluginInterface string

func (g pluginInterface) Runnable(args ...interface{}) (result interface{}, err error) {

	var left float64
	var right float64

	switch i := args[0].(type) {
	case int:
		left = float64(i)
	case int8:
		left = float64(i)
	case int16:
		left = float64(i)
	case int32:
		left = float64(i)
	case int64:
		left = float64(i)
	case float32:
		left = float64(i)
	case float64:
		left = float64(i)
	case string:
		left, _ = strconv.ParseFloat(strings.TrimSpace(i), 64)
	}

	switch i := args[1].(type) {
	case int:
		right = float64(i)
	case int8:
		right = float64(i)
	case int16:
		right = float64(i)
	case int32:
		right = float64(i)
	case int64:
		right = float64(i)
	case float32:
		right = float64(i)
	case float64:
		right = float64(i)
	case string:
		right, _ = strconv.ParseFloat(strings.TrimSpace(i), 64)
	}

	return (left > right), nil
}

func (g pluginInterface) Name() string {
	return ">"
}

func (g pluginInterface) TypeName() string {
	return "OPERATOR"
}

// PluginInterface exported
var PluginInterface pluginInterface
