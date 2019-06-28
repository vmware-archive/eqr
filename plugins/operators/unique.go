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
	"strconv"
	"strings"

	cft "github.com/carbonblack/eqr/ruleset/cuckooFilter"
)

type pluginInterface string

func (g pluginInterface) Runnable(args ...interface{}) (result interface{}, err error) {

	if len(args) == 0 {
		return "false", errors.New("Empty Unique Function")
	}

	/*
	 * This If statement is to account for when recursion happens after the record has been added to the filter
	 * UNIQUE gets called again e.g. UNIQUE(true) != nil, so we don't want to add true or false to the filter
	 * This if statement also accounts for when there is only 1 argument that needs to be checked
	 */
	if len(args) == 2 {
		if strings.Compare(args[0].(string), "true") != 0 && strings.Compare(args[0].(string), "false") != 0 {
			return strconv.FormatBool(cft.AddFilterVal(args[0].(string))), nil
		}
		return args[0].(string), nil
	}

	var argVals []string
	for _, arg := range args[:len(args)-1] {
		tmp := fmt.Sprintf("%v", arg)
		argVals = append(argVals, tmp)
	}

	var hashKey string = strings.Join(argVals, "-")

	if cft.LookupFilterVal(hashKey) == false {
		return strconv.FormatBool(cft.AddFilterVal(hashKey)), nil
	}

	return "false", nil

}

func (g pluginInterface) Name() string {
	return "UNIQUE"
}

func (g pluginInterface) TypeName() string {
	return "FUNCTION"
}

// PluginInterface exported
var PluginInterface pluginInterface
