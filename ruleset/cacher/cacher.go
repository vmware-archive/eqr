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
package ruleset

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/carbonblack/eqr/logging"
)

var cache = make(map[string]interface{}, 0)
var logger = logging.GetLogger()

func AddCache(key string, value interface{}) (result bool, err error) {

	if len(strings.TrimSpace(key)) == 0 {
		return true, nil
	}

	logger.WithFields(logrus.Fields{
	    "key": key,
		"value": value,
	}).Debug("Inserting records into cache")

	cache[key] = value

	return false, nil
}

func GetCache(key string) (interface{}) {

	if CheckCache(key) == false {
		return "null"
	}

	return cache[key]
}

func CheckCache(key string) bool {
	if _, ok := cache[key]; ok {
		return true
	}

	return false
}

func RemoveCache(key string) {
	delete(cache, key)
}

func ClearCache() {
	cache = make(map[string]interface{}, 0)
}

func PrintCache() {

	for key, cash := range cache {
		fmt.Printf("Cache Object (%v) -> %v\n", key, cash)
		fmt.Printf("Key -> %v\nValue -> %v\n", key, cash)
	}

}