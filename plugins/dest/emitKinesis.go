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
	"bytes"
	"compress/gzip"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
)

type destpluginInterface string

type KinesisWorker struct {
	Worker *kinesis.Kinesis
	StreamName *string
}

var log = logging.GetLogger()
var metricSender = metrics.GetSfxClient()

func (g destpluginInterface) Initialize(args ...interface{}) (result interface{}, err error) {

	if len(args) == 0 {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		    "args": args,
		}).Error("Nothing to generate")
		return nil, errors.New("nothing to generate")
	}

	var k *kinesis.Kinesis

	splits := strings.Split(args[0].(string), ",")
	log.WithFields(logrus.Fields{
		"plugin": g.Name(),
	    "args": splits,
	    "num": len(splits),
	}).Debug("Parsing Emit Kinesis parameters")
	streamName := strings.TrimSpace(splits[0])
	role := strings.TrimSpace(splits[1])

	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	if role != "" {
		creds := stscreds.NewCredentials(s, role)
		k = kinesis.New(s, &aws.Config{Credentials: creds})
	} else {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		    "args": splits,
		    "num": len(splits),
		}).Debug("EMIT: No role assigned")
		k = kinesis.New(s)
	}

	worker := &KinesisWorker{
		Worker: k,
		StreamName: &streamName,
	}

	log.WithFields(logrus.Fields{
		"plugin": g.Name(),
	    "args": splits,
	    "num": len(splits),
	}).Info("Successfully inited emit kinesis")

	return worker, nil
}

func (g destpluginInterface) Consume(args ...interface{}) (err error) {
	return errors.New("consume method not implemented")
}

func (g destpluginInterface) DoCheckpoint() bool {
	return false
}

func (g destpluginInterface) Publish(args ...interface{}) (result bool, err error) {
	
	workerNode := args[0].(*KinesisWorker)
	data := args[1].([]byte)
	dimensions := args[2].(map[string]string)
	partitionKey := uuid.New()

	var b bytes.Buffer
	gz := gzip.NewWriter(&b)

	// Fatals for all the errors to prevent a checkpoint when we cannot send the way we want.
	if _, err := gz.Write(data); err != nil {
		log.WithFields(logrus.Fields{
		  "err": err.Error(),
		}).Fatal("eqr emit kinesis gzip failed to write")
	}
	if err := gz.Flush(); err != nil {
		log.WithFields(logrus.Fields{
		  "err": err.Error(),
		}).Fatal("eqr emit kinesis gzip failed to flush")
	}
	if err := gz.Close(); err != nil {
		log.WithFields(logrus.Fields{
		  "err": err.Error(),
		}).Fatal("eqr emit kinesis gzip failed to close")
	}

	startTime := time.Now()
	_, err = (*workerNode.Worker).PutRecord(&kinesis.PutRecordInput{
		Data:         b.Bytes(),
		StreamName:   aws.String(*workerNode.StreamName),
		PartitionKey: aws.String(partitionKey.String()),
	})

	endTime := time.Now()
	diff := endTime.Sub(startTime)
	publishTime := float64(diff) / float64(time.Millisecond)
	metricSender.SendGauge("emit_publish_time", publishTime, dimensions)
	metricSender.SendGauge("emit_buffer_size", len(b.Bytes()), dimensions)


	if err == nil {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		}).Debug("Successfully published to kinesis")
		return true, nil
	}

	return false, err
}

func (g destpluginInterface) Name() string {
	return "EMIT.KINESIS"
}

func (g destpluginInterface) TypeName() string {
	return "DESTINATION"
}

// PluginInterface exported
var IOPluginInterface destpluginInterface
