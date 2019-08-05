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
	"fmt"
	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
	"strconv"
	"strings"

	cash "github.com/carbonblack/eqr/ruleset/cacher"
	cft "github.com/carbonblack/eqr/ruleset/cuckooFilter"
	rs "github.com/carbonblack/eqr/ruleset"
	rl "github.com/carbonblack/eqr/ruleset/rulebase"
	"github.com/carbonblack/eqr/ruleset"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	logging.Init()
	metrics.Init(true)
	ruleset.LoadOperatorPlugins()
	ruleset.LoadDestinationPlugins()
	//ruleset.LoadRules()
}

/*
func TestFindOperator(t *testing.T) {

	var result *plg.PluginInterface
	var err error

	ops := []string{
		"==",
		"!=",
		">=",
		"<=",
		"<",
		">",
	}

	for i := 0; i < len(ops); i++ {
		fmt.Printf("Looking for operator `%s`.", ops[i])
		result, err = ruleset.FindOperator(ops[i])

		if err != nil || result == nil {
			t.Errorf("Operator `%s` does not exist.", ops[i])
		}

		fmt.Println(" Success.")
	}
}
*/
/*
func TestGreaterThan(t *testing.T) {

	//record := "{ organizationId: 50}"
	rule := "60 > 50"
	operator, errs := ruleset.FindOperator(rule)

	if errs != nil || operator == nil {
		t.Errorf("Failed to get operator for the rule = `%s`\n", rule)
	}

	fmt.Println("Running rule on record")

	sides := strings.Split(rule, operator.Name())

	if len(sides) < 2 {
		t.Errorf("Failed to split strings on Operator Name (%s) on rule (%s)\n",
			operator.Name(), rule)
	}

	res, err := operator.Runnable(&sides[0], &sides[1])

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 1")
	}

	res, err = operator.Runnable("30", "50")

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 2")
	}

	res, err = operator.Runnable("20", "20")

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 3")
	}

	res, err = operator.Runnable("1", "20")

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 4")
	}
}
*/
func TestOpsManually(t *testing.T) {
	t.Logf("Parsing Int 1")
	var arg1 interface{} = "20"
	var arg2 interface{} = "20"
	left, err := strconv.ParseInt(strings.Trim(arg1.(string), " "), 10, 64)

	if err != nil {
		t.Errorf("Failed %v", err)
	}

	t.Logf("Parsing Int 2")
	right, err := strconv.ParseInt(strings.Trim(arg2.(string), " "), 10, 64)

	if err != nil {
		t.Errorf("Failed %v", err)
	}

	t.Logf("GREATER THAN - LEFT = `%v`\n", left)
	t.Logf("GREATER THAN - RIGHT = `%v`\n", right)

	res := (left >= right)

	if res == false {
		t.Errorf("FAILED")
	} else {
		t.Logf("SUCCESS")
	}
}

func TestGreaterThanEqual(t *testing.T) {
	//record := "{ organizationId: 50}"
	rule := "60 >= 50"
	operator, errs := ruleset.FindOperator(rule)

	if errs != nil || operator == nil {
		t.Errorf("Failed to get operator for the rule = `%s`\n", rule)
	}

	fmt.Printf("Running rule on record `%s`\n", (*operator).Name())

	sides := strings.Split(rule, (*operator).Name())

	if len(sides) < 2 {
		t.Errorf("Failed to split strings on Operator Name (%s) on rule (%s)\n",
			(*operator).Name(), rule)
	}

	res, err := (*operator).Runnable(sides[0], sides[1])

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 1")
	}

	res, err = (*operator).Runnable(30, 50)

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 2")
	}

	res, err = (*operator).Runnable(20, "20")

	fmt.Printf("RES = %v\n", res.(bool))
	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 3")
	}

	res, err = (*operator).Runnable("1", "20")

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 4")
	}

	t.Logf("SUCCESS")
}

/*
func TestLessThan(t *testing.T) {

	//record := "{ organizationId: 50}"
	rule := "60 < 50"
	operator, errs := ruleset.FindOperator(rule)

	if errs != nil || operator == nil {
		t.Errorf("Failed to get operator for the rule = `%s`\n", rule)
	}

	fmt.Println("Running rule on record")

	sides := strings.Split(rule, operator.Name())

	if len(sides) < 2 {
		t.Errorf("Failed to split strings on Operator Name (%s) on rule (%s)\n",
			operator.Name(), rule)
	}

	res, err := operator.Runnable(sides[0], sides[1])

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 1")
	}

	res, err = operator.Runnable("30", "50")

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 2")
	}

	res, err = operator.Runnable("20", "20")

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 3")
	}

	res, err = operator.Runnable("1", "20")

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 4")
	}
}

func TestLessThanEqual(t *testing.T) {

	//record := "{ organizationId: 50}"
	rule := "60 <= 50"
	operator, errs := ruleset.FindOperator(rule)

	if errs != nil || operator == nil {
		t.Errorf("Failed to get operator for the rule = `%s`\n", rule)
	}

	fmt.Println("Running rule on record")

	sides := strings.Split(rule, operator.Name())

	if len(sides) < 2 {
		t.Errorf("Failed to split strings on Operator Name (%s) on rule (%s)\n",
			operator.Name(), rule)
	}

	res, err := operator.Runnable(sides[0], sides[1])

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 1")
	}

	res, err = operator.Runnable("30", "50")

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 2")
	}

	res, err = operator.Runnable("20", "20")

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 3")
	}

	res, err = operator.Runnable("1", "20")

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 4")
	}
}

func TestEquals(t *testing.T) {

	//record := "{ organizationId: 50}"
	rule := "60 == 50"
	operator, errs := ruleset.FindOperator(rule)

	if errs != nil || operator == nil {
		t.Errorf("Failed to get operator for the rule = `%s`\n", rule)
	}

	fmt.Println("Running rule on record")

	sides := strings.Split(rule, operator.Name())

	if len(sides) < 2 {
		t.Errorf("Failed to split strings on Operator Name (%s) on rule (%s)\n",
			operator.Name(), rule)
	}

	res, err := operator.Runnable(sides[0], sides[1])

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 1")
	}

	res, err = operator.Runnable("20", "20")

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 2")
	}

	res, err = operator.Runnable("1", "20")

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 3")
	}
}

func TestNotEquals(t *testing.T) {
	//record := "{ organizationId: 50}"
	rule := "60 != 50"
	operator, errs := ruleset.FindOperator(rule)

	if errs != nil || operator == nil {
		t.Errorf("Failed to get operator for the rule = `%s`\n", rule)
	}

	fmt.Println("Running rule on record")

	sides := strings.Split(rule, operator.Name())

	if len(sides) < 2 {
		t.Errorf("Failed to split strings on Operator Name (%s) on rule (%s)\n",
			operator.Name(), rule)
	}

	res, err := operator.Runnable(sides[0], sides[1])

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 1")
	}

	res, err = operator.Runnable("20", "20")

	if err != nil || res.(bool) == true {
		t.Errorf("Failed on test 2")
	}

	res, err = operator.Runnable("1", "20")

	if err != nil || res.(bool) == false {
		t.Errorf("Failed on test 3")
	}
}
*/

func TestJsonGetFunction(t *testing.T) {

	funk, args, err := ruleset.FindFunction("JSON(organizationId)")

	if err != nil || funk == nil {
		t.Errorf("Failed to fetch the JSON command, args returned = %v\n", args)
	}

	fmt.Printf("args: %v", args)

	record := `{ "organizationId": 50, "environment": "testing", "augmentedBehaviorEvent":{"name":"Wahoo", "version":99} }`

	res, resErr := (*funk).Runnable(args, record)

	if resErr != nil || res != float64(50) {
		t.Errorf("Failed to get the correct value %v", res)
	}

	fmt.Printf("res: `%v` == 50\n", res)
}

func TestTimeFunction(t *testing.T) {
	funk, retArgs, err := ruleset.FindFunction("TIME(H)")

	if err != nil || funk == nil {
		t.Errorf("Failed to fetch the TIME command, args returned = %v\n", retArgs)
	}

	var args = "H"

	res, resErr := (*funk).Runnable(args)

	myTime := time.Now()

	if resErr != nil || res.(int) == myTime.Hour() == false {
		t.Errorf("Hours don't match NOW = %d, Function = %v", myTime.Hour(), res.(string))
	}

	t.Logf("NOW = %d, Func = %v", myTime.Hour(), res.(int))

	args = "M"
	res, resErr = (*funk).Runnable(args)

	myTime = time.Now()

	if resErr != nil || res.(int) == myTime.Minute() == false {
		t.Errorf("Minutes don't match NOW = %d, Function = %v", myTime.Minute(), res.(string))
	}

	t.Logf("NOW = %d, Func = %v", myTime.Minute(), res.(int))

	args = "S"
	res, resErr = (*funk).Runnable(args)

	myTime = time.Now()

	if resErr != nil || res.(int) == myTime.Second() == false {
		t.Errorf("Seconds don't match NOW = %d, Function = %v", myTime.Second(), res.(string))
	}

	t.Logf("NOW = %d, Func = %v", myTime.Second(), res.(int))

	args = "MS"
	res, resErr = (*funk).Runnable(args)
	if resErr != nil {
		t.Errorf("Milliseconds is broken")
	} else {
		t.Logf("Func = %v", res.(int64))
	}

}

func TestMalfomedRule(t *testing.T) {

	var defaultRule = `DUMMY.DEST[]( {
		JSON(test1) < 50;			
		JSON(test2) >= 30;},
		{
			SEND[
					JSON(test1) AS value_one;
					JSON(test2) AS value_two;
				]
		})`

	var record = `{ "test1" : 1234, "test2" : 55 }`

	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)
	ruleset.DeleteRule("Test")
}

func TestSingleNestedFunc(t *testing.T) {
	var defaultRule = `DUMMY.DEST[]( {
		JSON(test1) >= 50;			
		JSON(test2) >= 30;},
		{
			SEND[
					JSON(test1) AS value_one;
					JSON(test2) AS value_two;
				]
		})`

	var record = `{ "test1" : 1234, "test2" : 55 }`

	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)
	ruleset.DeleteRule("Test")
}

func TestCacheRuleOne(t *testing.T) {

	var defaultRule = `DUMMY.DEST[]( {
		JSON(test1) >= 50;			
		JSON(test2) >= 30;}
		{
			CACHE[
				JSON(test1) AS value_one;
				JSON(test2) AS value_two;
				SPRINTF(STR(%v-%v), JSON(test1),JSON(test2)) AS myID;
			]
		})`

	var record = `{ "test1" : 1234, "test2" : 55 }`

	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)

	cash.PrintCache()

	// get the cached value
	if cash.CheckCache("value_one") == false {
		t.Errorf("Cached `value_one` value doesnt exist")
	}

	if cash.CheckCache("value_two") == false {
		t.Errorf("Cached `value_two` value doesn't exist")
	}

	val_one := cash.GetCache("value_one")
	val_two := cash.GetCache("value_two")

	if val_one != 1234.0 {
		t.Errorf("First Value doesn't match = %v", val_one)
	}

	if val_two != 55.0 {
		t.Errorf("Second Value doesn't match = %v", val_two)
	}

	ruleset.DeleteRule("Test")
}

func TestFetchRuleOne(t *testing.T) {

	var defaultRule = `DUMMY.DEST[]( {

		},
		{
			SEND[
				FETCH(myID) AS anID;
			]
		})`

	var record = `{ "test1" : 1234, "test2" : 55 }`

	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)
	cash.ClearCache()
	ruleset.DeleteRule("Test")
}

func TestSprintF(t *testing.T) {

	var defaultRule = `DUMMY.DEST[]( {}
		{
			SEND[
				SPRINTF(STR(%v-%v), JSON(test1), JSON(test2)) AS printer;
			]
		})`

	var record = `{ "test1" : 1234, "test2" : 55 }`

	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)

	ruleset.DeleteRule("Test")
}

func TestUniqueSimple(t *testing.T) {

	var defaultRule = `DUMMY.DEST[]( {
		UNIQUE(JSON(test1), JSON(test2)) != null;
		},
		{})`

	var record = `{ "test1" : 1234, "test2" : 55 }`

	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)
	ruleset.DeleteRule("Test")
	cft.ClearFilter()
}

func TestJoinFunction(t *testing.T) {
	var defaultRule = `DUMMY.DEST[]( {},
		{
			SEND[
				JOIN(STR(COMMA), JSON(myArray)) AS arr;
				
			]
		})`

	var record = `{ "test1" : 1234, "myArray" : [ "one", "two", "three" ] }`


	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)
	ruleset.DeleteRule("Test")
}

func TestJsonExtract(t *testing.T) {
	var defaultRule = `DUMMY.DEST[]( {},
		{
			SEND[
				FIRST_NON_NULL(JSON(nonExistent), JSON(test1)) AS arr;
			]
		})`

	var record = `{ "test1" : 1234, "myArray" : [ "one", "two", "three" ] }`

	ruleset.AddRule("Test", defaultRule, false)
	runTest(record)
	ruleset.DeleteRule("Test")
}

func runTest(payload string) {
	allRecordRules, _ := ruleset.InitWorkerRuleset("one")

	for _, rule := range *allRecordRules {
		match, outbound, err := rs.RuleMatch(rule, payload)
		if match && err == nil && outbound != nil {
			success, err := rl.RunRule(rule, "00001111122222", outbound)
			if !success && err != nil {
				fmt.Print(err.Error())
			}
		}
	}

	fmt.Println("EQR Worker Shutting Down")
}
