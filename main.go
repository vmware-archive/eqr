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
	"os"

	"github.com/sirupsen/logrus"

	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
	"github.com/carbonblack/eqr/ruleset"
)

func main() {
	// Init logging
	logging.Init()
	metrics.Init(false)
	l := logging.GetLogger()

	if len(os.Args) < 3 {
		errors.New("Not enough parameters, must provide the RuleSetName and PluginConfigs.json")
	}

	// Specify your own ruleset here, replace default with whatever name of the rule FILE!
	l.WithFields(logrus.Fields{
	    "rulesetName": os.Args[1],
	    "pluginConfigs": os.Args[2],
	  }).Info("Pulling Plugins and Rules....")

	ruleset.PullPluginsAndRules(os.Args[1], os.Args[2])
	ruleset.RunRecordRules()
}
