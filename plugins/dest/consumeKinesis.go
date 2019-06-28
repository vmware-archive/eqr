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
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/carbonblack/eqr/kinsumer"
	"github.com/carbonblack/eqr/logging"
	"github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
)

type destpluginInterface string

var log = logging.GetLogger()

func (g destpluginInterface) Initialize(args ...interface{}) (result interface{}, err error) {

	if len(args) == 0 {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
			"args":   args,
		}).Error("Nothing to generate")
		return nil, errors.New("nothing to generate")
	}

	var k *kinsumer.Kinsumer
	splits := strings.Split(args[0].(string), ",")
	log.WithFields(logrus.Fields{
		"plugin": g.Name(),
		"args":   splits,
		"num":    len(splits),
	}).Debug("Parsing Consume Kinesis parameters")
	var kinesisStreamName = strings.TrimSpace(splits[0])
	var regionName string

	numParams := len(splits)

	if numParams > 1 {
		regionName = strings.TrimSpace(splits[1])
	} else {
		regionName = "us-east-1"
	}

	if len(kinesisStreamName) == 0 {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
			"args":   splits,
		}).Error("stream name is required")
		return nil, errors.New("stream name is required")
	}

	config := kinsumer.NewConfig()
	session := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(regionName),
	}))

	// kinsumer needs a way to differentiate between running clients, generally you want to use information
	// about the machine it is running on like ip. For this example we'll use a uuid
	name := uuid.NewV4().String()
	consumerName := strings.TrimSpace(splits[2])
	roleToConsume := strings.TrimSpace(splits[3])
	consumerARNvalue := strings.TrimSpace(splits[4])
	k, err = kinsumer.NewWithSession(session, kinesisStreamName, consumerName, name, config, roleToConsume, consumerARNvalue)
	if err != nil {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
			"args":   splits,
			"err":    err.Error(),
		}).Error("Error creating kinsumer")
		return nil, err
	}

	if strings.EqualFold(strings.TrimSpace(splits[4]), "true") {
		err = k.CreateRequiredTables()
		if err != nil {
			log.WithFields(logrus.Fields{
				"plugin": g.Name(),
				"args":   splits,
				"err":    err.Error(),
			}).Error("Error creating kinsumer dynamo db tables")
			return nil, err
		}
	}

	log.WithFields(logrus.Fields{
		"plugin": g.Name(),
		"args":   splits,
		"num":    len(splits),
	}).Info("Successfully inited consume kinesis")

	return k, nil
}

func (g destpluginInterface) Consume(args ...interface{}) (err error) {

	workerNode := args[0].(*kinsumer.Kinsumer)
	fatalErr := args[1].(chan error)
	if workerNode == nil {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		}).Error("No kinsumer to use")
		return errors.New("no kinsumer to use")
	}

	err = workerNode.Run(fatalErr)
	return err
}

func (g destpluginInterface) DoCheckpoint() bool {
	return false
}

func (g destpluginInterface) Publish(args ...interface{}) (result bool, err error) {
	return false, errors.New("publish method not implemented")
}

func (g destpluginInterface) Name() string {
	return "CONSUME.KINESIS"
}

func (g destpluginInterface) TypeName() string {
	return "CONSUMER"
}

// PluginInterface exported
var IOPluginInterface destpluginInterface
