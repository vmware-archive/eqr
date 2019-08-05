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
	"bytes"
	"container/list"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"plugin"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tkanos/gonfig"

	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
	plg "github.com/carbonblack/eqr/ruleset/pluginInterfaces"
	bldr "github.com/carbonblack/eqr/ruleset/rule"
	rl "github.com/carbonblack/eqr/ruleset/rulebase"
	batch "github.com/carbonblack/eqr/s3Batcher"
)

// This class is the Master Ruler. Holds all rules/operators/functions/destinations/pointers
// so that we can run through them.

type DestConfigs struct {
	DestRegion    string
	DestBucket    string
	DestS3Path    string
	DestLocalPath string
}

type OpConfigs struct {
	OpRegion    string
	OpBucket    string
	OpS3Path    string
	OpLocalPath string
}

type RuleConfigs struct {
	RuleRegion    string
	RuleBucket    string
	RulesetS3Path string
	RulesS3Path   string
}

var checkpointRules = make(map[string]bool)
var recordRules = make(map[string]string, 0)
var rules = make(map[string]string, 0)
var downstreamInterface map[string]interface{}
var operators = make(map[string]*plg.PluginInterface)
var consumers = make(map[string]*plg.IOPluginInterface)
var destinations = make(map[string]*plg.IOPluginInterface)
var destinationPointers = make(map[string]*interface{})
var functions = make(map[string]*plg.PluginInterface)
var operatorKeys = make([]string, 0)
var functionKeys = make([]string, 0)
var buffer bytes.Buffer
var logger = logging.GetLogger()

// Adds a rule.
func AddRule(name string, rule string, checkpoint bool) {
	rules[name] = rule
	checkpointRules[name] = checkpoint
}

func AddRecordRule(name string, rule string) {
	recordRules[name] = rule
	checkpointRules[name] = false
}

func InitRecordRuleset() *map[string]*rl.Rulebase {
	var builtRecordRules = make(map[string]*rl.Rulebase)
	for name, rule := range recordRules {
		res, err := initRule(name, rule, "", &builtRecordRules)
		if err != nil || !res {
			logger.WithFields(logrus.Fields{
				"rule": name,
				"err":  err.Error(),
			}).Fatal("EQR error initing record rule")
		}
	}
	return &builtRecordRules
}

func InitWorkerRuleset(shardID string) (*map[string]*rl.Rulebase, int) {
	var builtRules = make(map[string]*rl.Rulebase)
	var checkpointRefs int
	for name, rule := range rules {
		res, err := initRule(name, rule, shardID, &builtRules)
		if err != nil || !res {
			logger.WithFields(logrus.Fields{
				"rule": name,
				"err":  err.Error(),
			}).Fatal("EQR error initing worker rule")
		}
		if checkpointRules[name] {
			checkpointRefs += 1
		}
	}
	return &builtRules, checkpointRefs
}

func PullPluginsAndRules(rulesetName string, path string) {
	logger.WithFields(logrus.Fields{
		"path":    path,
		"loading": "dest",
	}).Info("Loading destination plugins")

	destConfigs := &DestConfigs{}
	err := gonfig.GetConf(path, destConfigs)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"path":    path,
			"loading": "dest",
			"err":     err.Error(),
		}).Fatal("EQR unable to load dest configs for loading plugins")
	}

	logger.WithFields(logrus.Fields{
		"Region":    destConfigs.DestRegion,
		"Bucket":    destConfigs.DestBucket,
		"S3Path":    destConfigs.DestS3Path,
		"LocalPath": destConfigs.DestLocalPath,
	}).Info("Loaded destination plugins")

	logger.WithFields(logrus.Fields{
		"path":    path,
		"loading": "oper",
	}).Info("Loading operator plugins")

	opConfigs := &OpConfigs{}
	err = gonfig.GetConf(path, opConfigs)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"path":    path,
			"loading": "oper",
			"err":     err.Error(),
		}).Fatal("EQR unable to load operator configs for loading plugins")
	}

	logger.WithFields(logrus.Fields{
		"Region":    opConfigs.OpRegion,
		"Bucket":    opConfigs.OpBucket,
		"S3Path":    opConfigs.OpS3Path,
		"LocalPath": opConfigs.OpLocalPath,
	}).Info("Loaded operator plugins")

	logger.WithFields(logrus.Fields{
		"path":    path,
		"loading": "rules",
	}).Info("Loading rules")

	ruleConfigs := &RuleConfigs{}
	err = gonfig.GetConf(path, ruleConfigs)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"path":    path,
			"loading": "rules",
			"err":     err.Error(),
		}).Fatal("EQR unable to load rule configs for loading rules")
	}

	logger.WithFields(logrus.Fields{
		"Region":        ruleConfigs.RuleRegion,
		"Bucket":        ruleConfigs.RuleBucket,
		"RulesetS3Path": ruleConfigs.RulesetS3Path,
		"RulesS3Path":   ruleConfigs.RulesS3Path,
	}).Info("Loaded rules")

	pullS3Dir(destConfigs.DestRegion, destConfigs.DestBucket, destConfigs.DestS3Path,
		destConfigs.DestLocalPath)
	pullS3Dir(opConfigs.OpRegion, opConfigs.OpBucket, opConfigs.OpS3Path,
		opConfigs.OpLocalPath)

	cmd := exec.Command("./buildplugins.sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Fatal("EQR unable to build plugins")

	}

	LoadDestinationPlugins()
	LoadOperatorPlugins()

	buff := &aws.WriteAtBuffer{}
	downloader := s3manager.NewDownloader(
		session.Must(session.NewSession(&aws.Config{Region: aws.String(ruleConfigs.RuleRegion)})))

	numBytes, err := downloader.Download(buff,
		&s3.GetObjectInput{
			Bucket: aws.String(ruleConfigs.RuleBucket),
			Key:    aws.String(ruleConfigs.RulesetS3Path + rulesetName),
		})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":      err.Error(),
			"rule":     rulesetName,
			"numBytes": numBytes,
		}).Fatal("EQR unable to download the ruleset")
	}

	rulesetContent := string(buff.Bytes())
	logger.WithFields(logrus.Fields{
		"contents": rulesetContent,
	}).Info("Ruleset contents")

	recordRule := gjson.Get(rulesetContent, "RecordGenerator")
	rulesetRules := gjson.Get(rulesetContent, "RecordRules")

	// download the record rule
	buff = &aws.WriteAtBuffer{}
	numBytes, err = downloader.Download(buff,
		&s3.GetObjectInput{
			Bucket: aws.String(ruleConfigs.RuleBucket),
			Key:    aws.String(ruleConfigs.RulesS3Path + recordRule.String()),
		})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":      err.Error(),
			"rule":     recordRule.String(),
			"numBytes": numBytes,
		}).Fatal("EQR unable to download the record rule")
	}

	logger.WithFields(logrus.Fields{
		"rule": string(buff.Bytes()),
	}).Info("The record rule")
	// add the rule
	AddRecordRule("__RECORD__", string(buff.Bytes()))

	for _, ruleLine := range rulesetRules.Array() {
		buff = &aws.WriteAtBuffer{}
		logger.WithFields(logrus.Fields{
			"rule": ruleLine,
		}).Info("The rule to pull")

		// pull the file down from AWS
		numBytes, err = downloader.Download(buff,
			&s3.GetObjectInput{
				Bucket: aws.String(ruleConfigs.RuleBucket),
				Key:    aws.String(ruleConfigs.RulesS3Path + gjson.Get(ruleLine.String(), "Rule").String()),
			})

		if err != nil {
			logger.WithFields(logrus.Fields{
				"err":      err.Error(),
				"rule":     gjson.Get(ruleLine.String(), "Rule").String(),
				"numBytes": numBytes,
			}).Fatal("EQR unable to download the worker rule")
		}

		content := string(buff.Bytes())
		checkpoint := gjson.Get(ruleLine.String(), "checkpoint").Bool()

		logger.WithFields(logrus.Fields{
			"rule": content,
		}).Debug("The rule is the following")

		AddRule(gjson.Get(ruleLine.String(), "Rule").String(), content, checkpoint)
	}
}

func pullS3Dir(region string, bucket string, path string, downloadDir string) {

	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String(region)}))

	buff := &aws.WriteAtBuffer{}
	// lets download all the plugins
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	svc := s3.New(sess)

	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket),
		Prefix: aws.String(path)})

	logger.WithFields(logrus.Fields{
		"resp":   resp.Contents,
		"bucket": bucket,
		"region": region,
		"key":    path,
		"path":   downloadDir,
	}).Debug("List objects in S3")

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Fatal("Unable to list objects from S3")
	}

	for _, item := range resp.Contents {
		buff = &aws.WriteAtBuffer{}
		logger.WithFields(logrus.Fields{
			"item": item,
		}).Debug("The item to pull")

		// key stuff
		keyName := *item.Key
		keyName = keyName[strings.LastIndex(keyName, "/")+1:]

		if len(keyName) <= 0 {
			continue
		}

		// pull the file down from AWS
		_, err := downloader.Download(buff,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(path + keyName),
			})

		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err.Error(),
			}).Fatal("Unable to download objects from S3")
		}

		os.MkdirAll(downloadDir, os.ModePerm)
		downloadFile, err := os.Create(downloadDir + "/" + keyName)

		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err.Error(),
			}).Fatal("Unable to create local directory")
		}

		downloadFile.Write(buff.Bytes())
		downloadFile.Close()
	}
}

func initRule(name string, rule string, shardID string, builtRules *map[string]*rl.Rulebase) (result bool, err error) {
	builder := bldr.Rule{}

	plugin, remainder, worker, err := FindInitPredicate(rule)
	if err != nil {
		return false, err
	}

	if strings.Contains((*plugin).Name(), "S3.BATCH") {
		worker.(*batch.S3BufferClient).Shard_Id = shardID
	}

	// init
	builder.InitRule()

	logger.WithFields(logrus.Fields{
		"plugin": plugin,
		"worker": worker,
	}).Debug("Init Rule plugin and worker details")

	// set the destination
	builder.SetDestination(plugin, worker)

	logger.WithFields(logrus.Fields{
		"rule": rule,
	}).Debug("Current init worker rule")

	// Predicate parsing
	var startIndex = strings.Index(rule, "{")
	var endIndex = strings.Index(rule, "}")
	var fullPredicate = rule[startIndex+1 : endIndex+1]
	fullPredicate = strings.Trim(fullPredicate, "{} ")

	logger.WithFields(logrus.Fields{
		"startIndex": startIndex,
		"endIndex":   endIndex,
		"remainder":  remainder,
	}).Debug("Remaining predicate and start/end indices")

	logger.WithFields(logrus.Fields{
		"predicate": fullPredicate,
	}).Debug("Full predicate")

	var ruleArr = strings.Split(fullPredicate, ";")

	for _, str := range ruleArr {
		if len(strings.TrimSpace(str)) == 0 {
			logger.WithFields(logrus.Fields{
				"predicate": str,
			}).Debug("Predicate string")
			continue
		}

		if strings.Contains(str, " OR ") {
			var inlineSplit = strings.Split(str, "OR")
			for _, inSplit := range inlineSplit {
				oper, err := FindOperator(inSplit)
				if err == nil {
					res, err := GetOperationSteps(inSplit, oper, &builder, true)
					if res == false {
						return false, err
					}
				}
			}
		} else {
			logger.WithFields(logrus.Fields{
				"string": str,
			}).Debug("Looking for operator")

			oper, err := FindOperator(str)
			if err == nil && oper != nil {
				res, err := GetOperationSteps(str, oper, &builder, false)
				if res == false || err != nil {
					return false, err
				}
			} else {
				return false, err
			}
		}
	}

	if strings.Contains(rule, "CACHE") {
		var index = strings.Index(rule, "CACHE[")
		var senderVals = rule[index+len("CACHE["):]
		senderVals = strings.Trim(senderVals, " []{}\n\t")
		var end = strings.Index(senderVals, "]")
		senderVals = senderVals[:end]
		var senderValsSplit = strings.Split(senderVals, ";")
		for i := 0; i < len(senderValsSplit); i++ {
			splitTrim := strings.Trim(senderValsSplit[i], " []{}()) ")
			splitTrim = strings.TrimSpace(splitTrim)
			if len(splitTrim) == 0 {
				continue
			}

			oldLine := splitTrim
			var opName string
			var changeID = " AS "
			if strings.Contains(oldLine, changeID) {
				indx := strings.Index(oldLine, changeID)
				oldLine = oldLine[indx+len(changeID):]
				opName = strings.Trim(oldLine, " ")
				logger.WithFields(logrus.Fields{
					"name": opName,
				}).Debug("OPNAME")
			}

			logger.WithFields(logrus.Fields{
				"function": "cache",
			}).Debug("Added function")
			builder.AddCache(GetFunction("CACHE"), opName)

			logger.WithFields(logrus.Fields{
				"length": len(splitTrim),
				"string": splitTrim,
			}).Debug("Looking for function")
			complete, _, _, _ := FindFuncRecursive(splitTrim, &builder, "CACHE", opName, -1, false)

			if complete != true {
				logger.WithFields(logrus.Fields{
					"function": "cache",
				}).Info("Something bad happened adding function")
				continue
			}
		}
	}

	if strings.Contains(rule, "SEND") {
		var index = strings.Index(rule, "SEND[")
		var senderVals = rule[index+len("SEND["):]
		senderVals = strings.Trim(senderVals, " []{}\n\t")
		var end = strings.Index(senderVals, "]")
		senderVals = senderVals[:end]
		var senderValsSplit = strings.Split(senderVals, ";")
		for i := 0; i < len(senderValsSplit); i++ {

			splitTrim := strings.Trim(senderValsSplit[i], " []{}()) ")
			splitTrim = strings.TrimSpace(splitTrim)
			if len(splitTrim) == 0 {
				continue
			}

			oldLine := splitTrim
			var opName string
			var changeID = " AS "
			if strings.Contains(oldLine, changeID) {
				indx := strings.Index(oldLine, changeID)
				oldLine = oldLine[indx+len(changeID):]
				opName = strings.Trim(oldLine, " ")
				logger.WithFields(logrus.Fields{
					"name": opName,
				}).Debug("OPNAME")
			}

			logger.WithFields(logrus.Fields{
				"function": "send",
			}).Debug("Added function")
			builder.AddProjection()

			logger.WithFields(logrus.Fields{
				"length": len(splitTrim),
				"string": splitTrim,
			}).Debug("Looking for function")
			complete, _, _, _ := FindFuncRecursive(splitTrim, &builder, "PROJECTION", opName, -1, false)

			if complete != true {
				logger.WithFields(logrus.Fields{
					"function": "send",
				}).Info("Something bad happened adding function")
				continue
			}

		}
	}

	checkpoint := checkpointRules[name]
	var metricSender = metrics.GetSfxClient()
	tmp := builder.GetRule(name, metricSender.(*metrics.SfxClient))
	tmp.Checkpoint = checkpoint
	(*builtRules)[name] = &tmp

	return true, nil
}

func RunRecordRules() {
	allRecordRules := InitRecordRuleset()
	fatalErr := make(chan error)
	normErr := make(chan error)

	for name, rule := range *allRecordRules {
		logger.WithFields(logrus.Fields{
			"rule": name,
		}).Info("Running Record Generation")
		err := rl.RunRecordGeneration(rule, fatalErr)

		if err != nil {
			logger.WithFields(logrus.Fields{
				"rule": name,
				"err":  err.Error(),
			}).Fatal("EQR failed to start consuming")
		}
	}

	var err error
	for {
		select {
		case err = <-fatalErr:
			logger.WithFields(logrus.Fields{
				"err": err.Error(),
			}).Fatal("Fatal error")
			return
		case err = <-normErr:
			//log and send metric here
		}
	}
}

func RuleMatch(rule *rl.Rulebase, record string) (bool, []byte, error) {
	var outbound []byte
	pred, _, err := rl.RunProjection(rule, record, "PREDICATE")
	if err != nil {
		return false, outbound, err
	}

	cache, _, err := rl.RunProjection(rule, record, "CACHE")

	if err != nil {
                return false, outbound, err
        }

	proj, outbound, err := rl.RunProjection(rule, record, "PROJECTION")

	if err != nil {
                return false, outbound, err
        }

	return pred && cache && proj, outbound, nil
}

func DeleteRule(name string) {
	//delete(builtRules, name)
}

// loads all the destinations
func LoadDestinationPlugins() {
	logger.WithFields(logrus.Fields{
		"plugin": "destination",
	}).Info("Loading plugins")

	files, err := ioutil.ReadDir("./plugins/dest/")
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Fatal("Unable to load destination plugins")
	}

	var (
		opPlugin *plg.IOPluginInterface
		plug     *plugin.Plugin
		sym      plugin.Symbol
	)
	for _, file := range files {

		if !strings.HasSuffix(file.Name(), ".so") {
			logger.WithFields(logrus.Fields{
				"filename": file.Name(),
			}).Debug("Continuing on")
			continue
		}

		plug, err = plugin.Open("./plugins/dest/" + file.Name())
		if err != nil {
			logger.WithFields(logrus.Fields{
				"filename": file.Name(),
				"err":      err.Error(),
			}).Fatal("Unable to open plugin")
		}

		// 2. look up a symbol (an exported function or variable)
		// in this case, variable Greeter
		sym, err = plug.Lookup("IOPluginInterface")
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err.Error(),
			}).Fatal("Unable to lookup symbol")
		}

		// 3. Assert that loaded symbol is of a desired type
		// in this case interface type Greeter (defined above)
		opP, ok := sym.(plg.IOPluginInterface)
		opPlugin = &opP
		if !ok {
			logger.WithFields(logrus.Fields{
				"opPlugin": (*opPlugin).Name(),
			}).Fatal("unexpected type from module symbol")
		}

		logger.WithFields(logrus.Fields{
			"opPlugin": (*opPlugin).Name(),
			"typename": (*opPlugin).TypeName(),
		}).Debug("Typename")

		switch (*opPlugin).TypeName() {
		case "DESTINATION":
			destinations[(*opPlugin).Name()] = opPlugin
			break
		case "CONSUMER":
			consumers[(*opPlugin).Name()] = opPlugin
			break
		default:
			logger.WithFields(logrus.Fields{
				"opPlugin": (*opPlugin).Name(),
			}).Fatal("Not a destination or a consumer")
		}

		logger.WithFields(logrus.Fields{
			"opPlugin": (*opPlugin).Name(),
			"typename": (*opPlugin).TypeName(),
		}).Info("Loading")
	}
}

// loads everything else
func LoadOperatorPlugins() {
	logger.WithFields(logrus.Fields{
		"plugin": "operator",
	}).Info("Loading plugins")

	files, err := ioutil.ReadDir("./plugins/operators/")
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Fatal("Unable to load operator plugins")
	}

	var (
		opPlugin *plg.PluginInterface
		plug     *plugin.Plugin
		sym      plugin.Symbol
	)
	for _, file := range files {

		if !strings.HasSuffix(file.Name(), ".so") {
			logger.WithFields(logrus.Fields{
				"filename": file.Name(),
			}).Debug("Continuing on")
			continue
		}

		plug, err = plugin.Open("./plugins/operators/" + file.Name())
		if err != nil {
			logger.WithFields(logrus.Fields{
				"filename": file.Name(),
				"err":      err.Error(),
			}).Fatal("Unable to open plugin")
		}

		// 2. look up a symbol (an exported function or variable)
		// in this case, variable Greeter
		sym, err = plug.Lookup("PluginInterface")
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err.Error(),
			}).Fatal("Unable to lookup symbol")
		}

		// 3. Assert that loaded symbol is of a desired type
		// in this case interface type Greeter (defined above)
		opP, ok := sym.(plg.PluginInterface)
		opPlugin = &opP
		if !ok {
			logger.WithFields(logrus.Fields{
				"opPlugin": (*opPlugin).Name(),
			}).Fatal("unexpected type from module symbol")
		}

		logger.WithFields(logrus.Fields{
			"opPlugin": (*opPlugin).Name(),
			"typename": (*opPlugin).TypeName(),
		}).Debug("Typename")

		switch (*opPlugin).TypeName() {
		case "OPERATOR":
			operators[(*opPlugin).Name()] = opPlugin
			operatorKeys = append(operatorKeys, (*opPlugin).Name())
			break
		case "FUNCTION":
			functions[(*opPlugin).Name()] = opPlugin
			functionKeys = append(functionKeys, (*opPlugin).Name())
			break
		default:
			logger.WithFields(logrus.Fields{
				"opPlugin": (*opPlugin).Name(),
			}).Fatal("Not an operator or a function")
		}

		logger.WithFields(logrus.Fields{
			"opPlugin": (*opPlugin).Name(),
			"typename": (*opPlugin).TypeName(),
		}).Info("Loading")
	}
}

// loads all the destinations
func LoadRules() {
	logger.WithFields(logrus.Fields{
		"dir": "./rules",
	}).Info("Loading rules")

	files, err := ioutil.ReadDir("./basicRules/")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {

		if !strings.HasSuffix(file.Name(), ".eqr") {
			continue
		}

		data, err := ioutil.ReadFile("./basicRules/" + file.Name())

		if err != nil {
			logger.WithFields(logrus.Fields{
				"filename": file.Name(),
			}).Fatal("Reading rule failed")
		}

		logger.WithFields(logrus.Fields{
			"filename": file.Name(),
		}).Debug("Loading rule")

		AddRule(file.Name(), string(data), false)
	}
}

func findPredicate(record string, stream *map[string]*plg.IOPluginInterface) (plugin *plg.IOPluginInterface, remainder string, worker interface{}, err error) {
	var tmp = strings.Trim(record, " {}")
	for _, strm := range *stream {
		if strings.Contains(tmp, (*strm).Name()) {
			startIndex := strings.Index(tmp, (*strm).Name()) + 1
			substr := tmp[startIndex+len((*strm).Name()):]
			endIndex := strings.Index(substr, "]")
			substr = substr[:endIndex]
			logger.WithFields(logrus.Fields{
				"vals":        substr,
				"destination": (*strm).Name(),
			}).Debug("Init vals")

			worker, err = (*strm).Initialize(substr)

			if err != nil {
				logger.WithFields(logrus.Fields{
					"vals":        substr,
					"destination": (*strm).Name(),
					"err":         err.Error(),
				}).Fatal("Failed to initialize rule")
			}

			return strm, substr, worker, nil
		}
	}

	return nil, "", nil, errors.New("BROKE")
}

// finds the type of destination, initialize it
func FindInitPredicate(record string) (plugin *plg.IOPluginInterface, remainder string, worker interface{}, err error) {

	io, remainder, worker, err := findPredicate(record, &destinations)

	if err != nil {
		op, remainder, worker, err := findPredicate(record, &consumers)
		return op, remainder, worker, err
	}

	return io, remainder, worker, nil
}

func FindOperator(rule string) (operator *plg.PluginInterface, err error) {

	var result *plg.PluginInterface

	// find operator
	// we do it like this because we are parsing each rule as a whole line
	// e.g `JSON(example) == 0` - So we need to see if the rule contains an operator
	// we also need to make sure we search them all because a rule that has `>=` will
	// also find a mapping too '>' && '>='
	for _, op := range operatorKeys {
		if strings.Contains(rule, op) {
			result = operators[op]
		}
	}

	if result == nil {
		return nil, errors.New("operator not found")
	}

	logger.WithFields(logrus.Fields{
		"operator": (*result).Name(),
	}).Debug("Operator found")

	return result, nil
}

func GetFunction(funcName string) *plg.PluginInterface {
	return functions[funcName]
}

func FindFuncRecursive(rule string, builder *bldr.Rule, isProject string, realID string, isMulti int, isOr bool) (bool, bool, *plg.PluginInterface, string) {
	var wasMulti bool
	if strings.Contains(rule, "(") {
		// have a sporting chance this is a function
		funcName := rule[:strings.Index(rule, "(")]
		function := GetFunction(strings.TrimSpace(funcName))

		if function == nil {
			logger.WithFields(logrus.Fields{
				"function": funcName,
			}).Info("Null function")
		}

		remainder := rule[strings.Index(rule, "(")+1:]
		lastPara := strings.LastIndex(remainder, ")")
		if lastPara != -1 {
			remainder = remainder[:lastPara]
		}
		remainder = strings.TrimSpace(remainder)
		if strings.Index(remainder, "(") == 0 {
			remainder = strings.Trim(remainder, " ()")
		}

		logger.WithFields(logrus.Fields{
			"remainder": remainder,
		}).Debug("Find Function Remainder")

		funcCounter := CountFunctions(remainder)

		sep := strings.Count(remainder, ",")

		logger.WithFields(logrus.Fields{
			"funcCounter": funcCounter,
			"separators":  sep,
		}).Debug("FuncCounter and Separators")

		var firstFunk string
		var firstFunkPlg *plg.PluginInterface
		var bit bool
		if funcCounter >= sep+1 {

			if funcCounter == sep+1 {
				firstFunkPlg = function
				bit = true
			} else {
				// there is a func up front, strip it, add it after the recursion
				tIndex := strings.Index(remainder, "(")
				firstFunk = remainder[:tIndex]
				remainder = remainder[tIndex+1:]
				firstFunkPlg = GetFunction(firstFunk)
			}

			logger.WithFields(logrus.Fields{
				"name": (*firstFunkPlg).Name(),
			}).Debug("First Function Plugin")

			tStep := &rl.Step{
				Plugin:    firstFunkPlg,
				Value:     &remainder,
				ID:        &realID,
				GetPrev:   false,
				MultiArgs: list.New(),
			}

			if strings.EqualFold(isProject, "PROJECTION") {
				builder.AddProjectStep(tStep, false)
			} else if strings.EqualFold(isProject, "CACHE") {
				builder.AddCacheStep(tStep, false)
			} else if strings.EqualFold(isProject, "PREDICATE") {
				builder.AddPredicate(tStep)
			}
		}

		multiargs := strings.Split(remainder, ",")

		var ID string
		var val string
		var res bool

		logger.WithFields(logrus.Fields{
			"length":    len(multiargs),
			"multiargs": multiargs,
		}).Debug("MultiArgs")

		wasMulti = len(multiargs) > 1
		if isMulti == -1 {
			isMulti = len(multiargs)
		}
		for i := 0; i < len(multiargs); i++ {

			multiargs[i] = strings.Trim(multiargs[i], " )\t\n")

			res, _, _, ID = FindFuncRecursive(multiargs[i], builder, isProject, realID, isMulti, isOr)
			val = multiargs[i]
			if realID != "" {
				ID = realID
			} else {
				ID = val
			}
		}

		if strings.EqualFold(isProject, "PROJECTION") && bit == false {
			step := &rl.Step{
				Plugin:    function,
				Value:     &val,
				ID:        &ID,
				GetPrev:   res,
				MultiArgs: list.New(),
			}

			logger.WithFields(logrus.Fields{
				"functionName":    (*function).Name(),
				"stepValue":       step.Value,
				"projectValue":    val,
				"functionCounter": funcCounter,
				"length":          len(multiargs),
				"isMulti":         isMulti,
			}).Debug("Pushing back Step")

			builder.AddProjectStep(step, len(multiargs) < isMulti && isMulti > 1)
		} else if strings.EqualFold(isProject, "PREDICATE") {
			step := &rl.Step{
				Plugin:    function,
				Value:     &val,
				ID:        &ID,
				GetPrev:   res,
				MultiArgs: list.New(),
			}

			if isMulti != 0 {
				if isOr {
					builder.OrPredicate(step, len(multiargs) < isMulti && isMulti > 1)
				} else {
					builder.AddStep(step, len(multiargs) < isMulti && isMulti > 1)
				}
			}

			logger.WithFields(logrus.Fields{
				"functionName":    (*function).Name(),
				"stepValue":       step.Value,
				"projectValue":    val,
				"functionCounter": funcCounter,
				"length":          len(multiargs),
				"isMulti":         isMulti,
			}).Debug("Pushing back Step")

		} else if strings.EqualFold(isProject, "CACHE") && bit == false {
			step := &rl.Step{
				Plugin:    function,
				Value:     &val,
				ID:        &ID,
				GetPrev:   res,
				MultiArgs: list.New(),
			}

			logger.WithFields(logrus.Fields{
				"functionName":    (*function).Name(),
				"stepValue":       step.Value,
				"projectValue":    val,
				"functionCounter": funcCounter,
				"length":          len(multiargs),
				"isMulti":         isMulti,
			}).Debug("Pushing back Cache Step")

			builder.AddCacheStep(step, len(multiargs) < isMulti && isMulti > 1)
		} else {
			return false, wasMulti, function, ID
		}
		return true, wasMulti, function, ID
	}

	return false, wasMulti, nil, rule
}

func CountFunctions(rule string) int {
	var counter int
	for _, funk := range functionKeys {
		if strings.Contains(rule, funk) {
			counter += strings.Count(rule, funk)
		}
	}

	return counter
}

func FindFunction(rule string) (operator *plg.PluginInterface, args string, err error) {
	for _, funk := range functionKeys {
		if strings.Contains(rule, funk) {

			var index = strings.Index(rule, (*functions[funk]).Name()) + 1
			var remainder = rule[index+len((*functions[funk]).Name()):]
			remainder = remainder[:strings.Index(remainder, ")")]

			remainder = strings.Trim(remainder, " \t")
			logger.WithFields(logrus.Fields{
				"function":  funk,
				"remainder": remainder,
			}).Debug("Find Function and Remainder")

			return functions[funk], remainder, nil
		}
	}

	return nil, rule, errors.New("Function not found " + rule)
}

func GetOperationSteps(rule string, operator *plg.PluginInterface, builder *bldr.Rule, isOr bool) (result bool, err error) {
	var sides = strings.Split(rule, (*operator).Name())

	if len(sides) < 2 {
		return false, errors.New("too little arguments")
	}

	complete, wasMulti, _, args := FindFuncRecursive(rule, builder, "PREDICATE", rule, -1, isOr)
	args = strings.Trim(args, " \t")

	logger.WithFields(logrus.Fields{
		"predicate": complete,
		"wasMulti":  wasMulti,
	}).Debug("Complete predicate")

	// add the final operation
	typeValue := "OPERATOR"
	step := &rl.Step{
		Plugin:  operator,
		Value:   &typeValue,
		Result:  strings.TrimSpace(sides[1]),
		GetPrev: wasMulti,
	}

	logger.WithFields(logrus.Fields{
		"name": (*step.Plugin).Name(),
		"step": step,
	}).Debug("Adding Step")
	builder.AddStep(step, wasMulti)

	return true, nil
}
