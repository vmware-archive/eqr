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

/*
 * Utilizes the Cuckoo Filter, which is an improved version of the Bloom Filter
 * More compact than a Bloom filter and allows for deletion of items
 */

import (
	"fmt"
	"github.com/seiflotfy/cuckoofilter"
	"github.com/zond/god/murmur" //Used for hashing the fields to insert into the filter
)

// Initialize Filter with a size of 1000, this value can be modified in the future
var cft = cuckoo.NewFilter(1000)

func GenerateHash(key string) []byte {
	return murmur.HashString(key)
}

/*
 * AddFilterVal
 * Returns False if record exists in filter
 * Returns True if record isn't in filter and inserts it
 */
func AddFilterVal(key string) bool  {

	result := cft.InsertUnique(GenerateHash(key))
	return result
}

func LookupFilterVal(key string) bool {
	return cft.Lookup(GenerateHash(key))
}

func RemoveFilterVal(key string) {
	cft.Delete(GenerateHash(key))
}

func GetNumElementsFilter() uint {
	return cft.Count()
}

func ClearFilter() {
	cft.Reset()
}

func PrintFilterCount() {
	count := cft.Count()
	fmt.Println(count)
}
