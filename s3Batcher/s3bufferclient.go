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
package s3batcher

import (
	"bytes"
	//"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/sirupsen/logrus"
	"github.com/twinj/uuid"

	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
)

type Buffer struct {
	b bytes.Buffer
	m sync.Mutex
}

var logger = logging.GetLogger()
var fileMutex sync.Mutex

func (b *Buffer) LockBuffer() {
	b.m.Lock()
}
func (b *Buffer) UnlockBuffer() {
	b.m.Unlock()
}
func (b *Buffer) Read(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Read(p)
}
func (b *Buffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}
func (b *Buffer) String() string {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.String()
}
func (b *Buffer) ReadFrom(r io.Reader) (n int64, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.ReadFrom(r)
}
func (b *Buffer) Reset() {
	b.b.Reset()
}

type Sequence struct {
	Sequence     int
	Sub_Sequence int
}

type S3BufferClient struct {
	Env              string
	Shard_Id         string
	S3_Bucket        string
	Flush_Interval   int64
	Config           string
	MAX_BUFFER_SIZE  int64
	Compress_Level   int
	Buffer_Seq       Sequence
	Flushed_Seq_List []Sequence
	Buffer_Size      int64
	Buffer_Age       int64
	Last_Flush       int64
	Worker_Is_Alive  bool
	RecordBuffer     Buffer
	GzipBuffer       Buffer
	MetricSender     *metrics.MetricSender
	TmpFile          string
	Last_Flush_File  string
	// add compress level for gzip
}

func (s *S3BufferClient) Init() {
	os.Mkdir("/tmp/eqr", 0777)
	s.Reset_Buffer()
	s.Worker_Is_Alive = true
	s.Compress_Level = 4
	// begin batching
}


func NewBufferClient(Env string, Shard_Id string, S3_Bucket string, Region string, Flush_Interval int64, MAX_BUFFER_SIZE int64, Config string, metricSender *metrics.MetricSender) (*S3BufferClient, error) {

	BufferClient := &S3BufferClient{
		Env:             Env,
		Shard_Id:        Shard_Id,
		S3_Bucket:       S3_Bucket,
		Flush_Interval:  Flush_Interval,
    	MAX_BUFFER_SIZE: MAX_BUFFER_SIZE,
		Config:          Config,
		MetricSender:    metricSender,
	}

	logger.WithFields(logrus.Fields{
	  "shardId": Shard_Id,
	  "bucket": S3_Bucket,
	  "flushInterval": Flush_Interval,
	}).Info("Successfully created buffer client")

	return BufferClient, nil
}

func (s *S3BufferClient) PutRecordInBuffer(record []byte, seq Sequence) string {
	record = append(record, []byte("\n")...)
	tmp, err := os.OpenFile(s.TmpFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	if err != nil { 
		logger.WithFields(logrus.Fields{
		  "shardId": s.Shard_Id,
		  "tmpFile": s.TmpFile,
		  "err": err.Error(),
		}).Error("Error opening tmp file")
        panic(err) 
	}

    defer func() {
        if err := tmp.Close(); err != nil {
        	logger.WithFields(logrus.Fields{
			  "shardId": s.Shard_Id,
			  "tmpFile": s.TmpFile,
			  "err": err.Error(),
			}).Error("Error closing tmp file")
            panic(err)
        }
    }()
	if _, err := tmp.Write(record); err != nil {
		logger.WithFields(logrus.Fields{
		  "shardId": s.Shard_Id,
		  "tmpFile": s.TmpFile,
		  "err": err.Error(),
		}).Error("Error writing to tmp file")
        panic(err)
    }

	s.Buffer_Size += int64(len(record)) + 1
	s.Buffer_Seq = seq

	return s.TmpFile
}

func (s *S3BufferClient) Begin_Background_Worker(s3u s3manageriface.UploaderAPI) {
	for s.Worker_Is_Alive {
		if s.ReadyToFlush() {
			logger.WithFields(logrus.Fields{
			  "shardId": s.Shard_Id,
			}).Debug("Starting the Flush!")
			err := s.FlushToS3(s3u)
			if err != nil {
				logger.WithFields(logrus.Fields{
				  "shardId": s.Shard_Id,
				  "err": err.Error(),
				}).Error("Flush received an error")
				s.Worker_Is_Alive = false
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (s *S3BufferClient) ReadyToFlush() bool {
	now := int64(time.Now().Unix())
	s.Buffer_Age = now - s.Last_Flush
	if s.Buffer_Size > s.MAX_BUFFER_SIZE {
		return true
	}
	if s.Buffer_Age >= s.Flush_Interval {
		return true
	}
	return false
}

func (s *S3BufferClient) FlushToS3(s3u s3manageriface.UploaderAPI) error {
	dimensions := make(map[string]string)
	dimensions["shardId"] = s.Shard_Id
	if s.Buffer_Size <= 0 {
		logger.WithFields(logrus.Fields{
		  "shardId": s.Shard_Id,
		}).Debug("Attempting to flush empty buffer")
		s.Reset_Buffer()
		return nil
	}
	startTime := time.Now()
	fileMutex.Lock()
    defer fileMutex.Unlock()

	cmd := exec.Command("gzip", "-f", s.TmpFile)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()

	if err != nil {
		if s.TmpFile == s.Last_Flush_File {
			logger.WithFields(logrus.Fields{
				"tmp": s.TmpFile,
			}).Debug("Already flushed file")
			return nil
		} else {
			logger.WithFields(logrus.Fields{
				"tmp": s.TmpFile,
				"err": err.Error(),
			}).Fatal("EQR Unable to GZIP Temp File")
		}
	}

	key := s.Get_Key()

	s.Flushed_Seq_List = append(s.Flushed_Seq_List, s.Buffer_Seq)

	gzFileName := fmt.Sprintf("%v.gz", s.TmpFile)
	data, err := ioutil.ReadFile(gzFileName)
	bytesReader := bytes.NewReader(data)

	err = s.UploadToS3(s3u, bytesReader, s.S3_Bucket, key)
	logger.WithFields(logrus.Fields{
	  "shardId": s.Shard_Id,
	}).Debug("Uploaded to S3")
	if err != nil {
		logger.WithFields(logrus.Fields{
		  "shardId": s.Shard_Id,
		  "err": err.Error(),
		}).Fatal("Error uploading file to S3")
	}

	endTime := time.Now()
	diff := endTime.Sub(startTime)
	batchTime := float64(diff) / float64(time.Millisecond)
	(*s.MetricSender).SendGauge("s3_batch_time", batchTime, dimensions)
	(*s.MetricSender).SendGauge("s3_buffer_size", s.Buffer_Size, dimensions)

	s.Reset_Buffer()
	return err
}

func (s *S3BufferClient) UploadToS3(s3u s3manageriface.UploaderAPI, file io.Reader, bucket string, key string) error {
	encoding := "gzip"
	contentType := "application/json"
	_, err := s3u.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
		ContentEncoding: &encoding,
		ContentType: &contentType,
	})
	return err
}

func (s *S3BufferClient) Reset_Buffer() {
	if s.TmpFile != "" {
		os.Remove(s.TmpFile)
		s.Last_Flush_File = s.TmpFile
	}
	s.TmpFile = fmt.Sprintf("/tmp/eqr/%v", uuid.NewV4().String())
	s.GzipBuffer.Reset()
	s.Buffer_Size = 0
	s.Last_Flush = int64(time.Now().Unix())
	runtime.GC()
}

func (s *S3BufferClient) Background_Worker_Is_Dead() bool {
	return !(s.Worker_Is_Alive)
}

func (s *S3BufferClient) Remove_Flush() {
	// returns last element of sequence list and removes it from the list
	// replaces python pop()

	s.Flushed_Seq_List = make([]Sequence, 0)
}

func (s *S3BufferClient) Get_Key_Prefix() string {
	t := time.Now()
	prefix := fmt.Sprintf("%d-%02d-%02d/%02d%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute())
	return prefix
}

func (s *S3BufferClient) Get_Key() string {
	key_prefix := s.Get_Key_Prefix()
	timestamp := time.Now().Unix()
	suffix := ".gz"
	key := fmt.Sprintf("%s/%s-%d%s", key_prefix, s.Shard_Id, timestamp, suffix)
	return key
}
