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
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"

	s3batcher "github.com/carbonblack/eqr/s3Batcher"
	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
)

type destpluginInterface string

var log = logging.GetLogger()
var metricSender = metrics.GetSfxClient()

func (g destpluginInterface) Initialize(args ...interface{}) (result interface{}, err error) {

	if len(args) == 0 {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		    "args": args,
		}).Error("Nothing to generate")
		return nil, errors.New("Nothing to generate")
	}

	splits := strings.Split(args[0].(string), ",")
	log.WithFields(logrus.Fields{
		"plugin": g.Name(),
	    "args": splits,
	    "num": len(splits),
	}).Debug("Parsing S3 Batch parameters")

	// Env, ShardName, S3 Name, Region, Flush Interval, Buffer Size, Config
	if len(splits) < 6 {
		// not enough params
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		    "args": splits,
		    "num": len(splits),
		}).Error("S3 Params are not complete, not enough params")
		return nil, errors.New("S3 Params are not complete, not enough params")
	}

	// need a new session
	mySession := session.Must(session.NewSession(aws.NewConfig().WithMaxRetries(3)))
	myUploader := s3manager.NewUploader(mySession)

	//Env string, Shard_Id string, S3_Bucket string, Region string, Flush_Interval int64, MAX_BUFFER_SIZE int64, Config string

	flush, _ := strconv.ParseInt(strings.TrimSpace(splits[4]), 10, 64)
	buffer, _ := strconv.ParseInt(strings.TrimSpace(splits[5]), 10, 64)
	myBufferClient, _ := s3batcher.NewBufferClient(strings.TrimSpace(splits[0]), strings.TrimSpace(splits[1]),
		strings.TrimSpace(splits[2]), strings.TrimSpace(splits[3]),
		flush, buffer, "no config", &metricSender)

	myBufferClient.Init()
	go myBufferClient.Begin_Background_Worker(myUploader)

	log.WithFields(logrus.Fields{
		"plugin": g.Name(),
	    "args": splits,
	    "num": len(splits),
	}).Info("Successfully inited S3 batch")

	return myBufferClient, nil
}

func (g destpluginInterface) Consume(args ...interface{}) (err error) {
	return nil
}

func (g destpluginInterface) DoCheckpoint() bool {
	return false
}

func (g destpluginInterface) Publish(args ...interface{}) (result bool, err error) {

	if len(args) <= 1 {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		}).Error("No records to send")
		return false, errors.New("No Record to send.")
	}

	bufferClient := args[0].(*s3batcher.S3BufferClient)

	// the first argument is buffer client; we can ignore this
	// the second is the byte array
	record := *args[1].(*[]byte)

	if record == nil {
		log.WithFields(logrus.Fields{
			"plugin": g.Name(),
		}).Error("Projection byte array is null.")
		return false, errors.New("Projection byte array is null.")
	}

	file := bufferClient.PutRecordInBuffer(record, s3batcher.Sequence{0, 2})

	for {
		if file == bufferClient.Last_Flush_File {
			bufferClient.Remove_Flush()
			log.WithFields(logrus.Fields{
				"plugin": g.Name(),
			}).Debug("Successfully flushed S3 batch")
			return true, nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return false, errors.New("Flush timed out")
}

func (g destpluginInterface) Name() string {
	return "S3.BATCH"
}

func (g destpluginInterface) TypeName() string {
	return "DESTINATION"
}

// PluginInterface exported
var IOPluginInterface destpluginInterface
