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
	MetricSender     *metrics.SfxClient
	TmpFile          string
	Last_Flush_File  string
	Next_Flush_File  string
	LockFile	 bool
	LockFileBuffer	 [][]byte
	FilePointer	 *os.File
	// add compress level for gzip
}

func (s *S3BufferClient) Init() {
	os.Mkdir("/tmp/eqr", 0777)
	s.Next_Flush_File = fmt.Sprintf("/tmp/eqr/%v", uuid.NewV4().String())
	s.Reset_Buffer()
	s.Worker_Is_Alive = true
	s.Compress_Level = 4
	// begin batching
}


func NewBufferClient(Env string, Shard_Id string, S3_Bucket string, Region string, Flush_Interval int64, MAX_BUFFER_SIZE int64, Config string, metricSender *metrics.SfxClient) (*S3BufferClient, error) {

	BufferClient := &S3BufferClient{
		Env:             Env,
		Shard_Id:        Shard_Id,
		S3_Bucket:       S3_Bucket,
		Flush_Interval:  Flush_Interval,
    		MAX_BUFFER_SIZE: MAX_BUFFER_SIZE,
		Config:          Config,
		MetricSender:    metricSender,
		LockFileBuffer:  make([][]byte, 0),
	}

	logger.WithFields(logrus.Fields{
	  "shardId": Shard_Id,
	  "bucket": S3_Bucket,
	  "flushInterval": Flush_Interval,
	}).Info("Successfully created buffer client")

	return BufferClient, nil
}

func PutRecordInBuffer(s *S3BufferClient, record []byte, seq Sequence, isOverflowData bool) (bool, error) {
	record = append(record, []byte("\n")...)

	if s.LockFile == true {
		if isOverflowData == false {
			s.LockFileBuffer = append(s.LockFileBuffer, record)
			return true, nil
		} else {
			return false, fmt.Errorf("OVERFLOW_LOCK")
		}
	}

	if s.FilePointer == nil {
		tmpFile, err := os.OpenFile(s.TmpFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			logger.WithFields(logrus.Fields{
			  "shardId": s.Shard_Id,
			  "tmpFile": s.TmpFile,
			  "err": err.Error(),
			}).Error("Error opening tmp file")
			return false, err
		}

		// set the file pointer
		s.FilePointer = tmpFile
	}

	fileMutex.Lock()
	defer fileMutex.Unlock()

	if s.LockFile == true {
		if isOverflowData == false {
			s.LockFileBuffer = append(s.LockFileBuffer, record)
			return true, nil
		} else {
			return false, fmt.Errorf("OVERFLOW_LOCK")
		}
	}

	if _, err := s.FilePointer.Write(record); err != nil {

		if s.LockFile == false { 
			logger.WithFields(logrus.Fields{
			  "shardId": s.Shard_Id,
			  "tmpFile": s.TmpFile,
			  "err": err.Error(),
			}).Error("Error writing to tmp file")
			return false, err
		} else {
			if isOverflowData == false {
				s.LockFileBuffer = append(s.LockFileBuffer, record)

				logger.WithFields(logrus.Fields{
					"shardID" : s.Shard_Id,
					"tmpFile": s.TmpFile,
				}).Info("File was flushed before writing")
				return true, nil
			} else {
				return false, fmt.Errorf("OVERFLOW_LOCK")
			}
		}
	}

	s.Buffer_Size += int64(len(record)) + 1
	s.Buffer_Seq = seq

	return true, nil
}

func (s *S3BufferClient) Begin_Background_Worker(s3u s3manageriface.UploaderAPI) {
	for s.Worker_Is_Alive {
		if s.ReadyToFlush() {
			logger.WithFields(logrus.Fields{
			  "shardId": s.Shard_Id,
			  "buffer": s.Buffer_Size,
			  "Max Buffer": s.MAX_BUFFER_SIZE,
			  "age": s.Buffer_Age,
			  "last age": s.Last_Flush,
			}).Debug("Starting the Flush!")
			err := FlushToS3(s, s3u)
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

func FlushToS3(s *S3BufferClient, s3u s3manageriface.UploaderAPI) error {
	dimensions := make(map[string]string)
	dimensions["shardId"] = s.Shard_Id
	if s.Buffer_Size <= 0 || s.FilePointer == nil {
		logger.WithFields(logrus.Fields{
		  "shardId": s.Shard_Id,
		}).Debug("Attempting to flush empty buffer")
		s.Reset_Buffer()
		return nil
	}

	if _, err := os.Stat(s.TmpFile); os.IsNotExist(err) {
		logger.WithFields(logrus.Fields{
			"shardID" : s.Shard_Id,
			"tmpfile" : s.TmpFile,
			"buffer" : s.Buffer_Size,
			"Filepointer" : s.FilePointer,
			"lock" : s.LockFile,
			"overflow len" : len(s.LockFileBuffer),
		}).Info("File doesnt exist yet, small race")
		return nil
	}

	// if we are already locked, return
	if s.LockFile == true {
		logger.WithFields(logrus.Fields{
			"tmpfile" : s.TmpFile,
			"overflow len" : len(s.LockFileBuffer),
		}).Info("File already locked")
		return nil
	}

	s.LockFile = true

	time.Sleep( 250  * time.Millisecond)
	closeErr := s.FilePointer.Close();
	if closeErr != nil {
		logger.WithFields(logrus.Fields{
			"file" : s.TmpFile,
			"err" : closeErr,
		}).Fatal("Failed to close the file")

		panic(closeErr)
	}
	s.FilePointer = nil

	startTime := time.Now()

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
	
	if _, err := os.Stat(gzFileName); os.IsNotExist(err) {
		logger.WithFields(logrus.Fields{
			"file": s.TmpFile,
			"gzfile" : gzFileName,
		}).Debug("gzip file doesn't exist")
	}

	data, err := ioutil.ReadFile(gzFileName)
	bytesReader := bytes.NewReader(data)

	if err != nil {
		logger.WithFields(logrus.Fields{
			"gzfile" : gzFileName,
			"err" : err.Error(),
		}).Debug("GZIP Read ERROR")
	}

	err = UploadToS3(s3u, bytesReader, s.S3_Bucket, key)
	logger.WithFields(logrus.Fields{
	  "gzfile": gzFileName,
	}).Debug("Uploaded to S3")
	if err != nil {
		logger.WithFields(logrus.Fields{
		  "shardId": s.Shard_Id,
		  "err": err.Error(),
		}).Debug("Error uploading file to S3")
	}

	endTime := time.Now()
	diff := endTime.Sub(startTime)
	batchTime := float64(diff) / float64(time.Millisecond)
	s.MetricSender.SendGauge("s3_batch_time", batchTime, dimensions)
	s.MetricSender.SendGauge("s3_buffer_size", s.Buffer_Size, dimensions)

	s.Reset_Buffer()
	return err
}

func UploadToS3(s3u s3manageriface.UploaderAPI, file io.Reader, bucket string, key string) error {
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
		gzFile := fmt.Sprintf("%v.gz", s.TmpFile)
		os.Remove(gzFile)
		s.Last_Flush_File = s.TmpFile
	}
	s.TmpFile = s.Next_Flush_File
	s.Next_Flush_File = fmt.Sprintf("/tmp/eqr/%v", uuid.NewV4().String())
	s.GzipBuffer.Reset()
	s.Buffer_Size = 0
	s.Last_Flush = int64(time.Now().Unix())

	s.LockFile = false
	go s.releaseLockFileBuffer()

	runtime.GC()
}

func (s *S3BufferClient) releaseLockFileBuffer() {
	overflow := make([][]byte, 0)
	for _, record := range s.LockFileBuffer {
		_, err := PutRecordInBuffer(s, record, Sequence{}, true)
		if err != nil && err.Error() == "OVERFLOW_LOCK" {
			overflow = append(overflow, record)
		}
	}

	s.LockFileBuffer = make([][]byte, 0)
	copy(s.LockFileBuffer, overflow)
}

func (s *S3BufferClient) Background_Worker_Is_Dead() bool {
	return !(s.Worker_Is_Alive)
}

func (s *S3BufferClient) Remove_Flush() {
	// returns last element of sequence list and removes it from the list
	// replaces python pop()

	s.Flushed_Seq_List = make([]Sequence, 0)
}

func Get_Key_Prefix() string {
	t := time.Now()
	prefix := fmt.Sprintf("%d-%02d-%02d/%02d%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute())
	return prefix
}

func (s *S3BufferClient) Get_Key() string {
	key_prefix := Get_Key_Prefix()
	timestamp := time.Now().Unix()
	suffix := ".gz"
	key := fmt.Sprintf("%s/%s-%d%s", key_prefix, s.Shard_Id, timestamp, suffix)
	return key
}
