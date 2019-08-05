package kinsumer

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	chk "github.com/carbonblack/eqr/checkpoint"
	rec "github.com/carbonblack/eqr/records"
	"github.com/carbonblack/eqr/ruleset"
	rl "github.com/carbonblack/eqr/ruleset/rulebase"
	"github.com/carbonblack/eqr/snappy"
)

const (
	// getRecordsLimit is the max number of records in a single request. This effectively limits the
	// total processing speed to getRecordsLimit*5/n where n is the number of parallel clients trying
	// to consume from the same kinesis stream
	getRecordsLimit = 1000 // 10,000 is the max according to the docs

	// maxErrorRetries is how many times we will retry on a shard error
	maxErrorRetries = 3

	// errorSleepDuration is how long we sleep when an error happens, this is multiplied by the number
	// of retries to give a minor backoff behavior
	errorSleepDuration = 1 * time.Second
)

// getShardIterator gets a shard iterator after the last sequence number we read or at the start of the stream
func getShardIterator(k kinesisiface.KinesisAPI, streamName string, shardID string, sequenceNumber string) (string, error) {
	shardIteratorType := kinesis.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" {
		shardIteratorType = kinesis.ShardIteratorTypeTrimHorizon
		ps = nil
	} else if sequenceNumber == "LATEST" {
		shardIteratorType = kinesis.ShardIteratorTypeLatest
		ps = nil
	}

	resp, err := k.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shardID),
		ShardIteratorType:      &shardIteratorType,
		StartingSequenceNumber: ps,
		StreamName:             aws.String(streamName),
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":               err.Error(),
			"shardId":           shardID,
			"shardIteratorType": shardIteratorType,
			"streamName":        streamName,
		}).Error("eqr unable to get shard iterator")

		return aws.StringValue(resp.ShardIterator), err
	}

	logger.WithFields(logrus.Fields{
		"shardId":           shardID,
		"shardIteratorType": shardIteratorType,
		"shardIterator":     resp.ShardIterator,
		"streamName":        streamName,
	}).Debug("Successfully loaded shard iterator")

	return aws.StringValue(resp.ShardIterator), err
}

// getRecords returns the next records and shard iterator from the given shard iterator
func getRecords(k kinesisiface.KinesisAPI, iterator string) (allRecords []kinesis.Record, nextIterator string, lag int64, err error) {
	params := &kinesis.GetRecordsInput{
		Limit:         aws.Int64(getRecordsLimit),
		ShardIterator: aws.String(iterator),
	}

	output, err := k.GetRecords(params)

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":           err.Error(),
			"shardIterator": iterator,
		}).Error("eqr unable to get records")
		return nil, "", 0, err
	}

	nextIterator = aws.StringValue(output.NextShardIterator)
	lag = aws.Int64Value(output.MillisBehindLatest)

	allRecords, err = expandProtoRecords(output.Records)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Error("Error getting Protobuf records")
		return nil, "", 0, err
	}

	return allRecords, nextIterator, lag, nil
}

func expandProtoRecords(records []*kinesis.Record) (allRecords []kinesis.Record, err error) {
	magic := fmt.Sprintf("%q", []byte("\xf3\x89\x9a\xc2"))

	for _, record := range records {
		header := fmt.Sprintf("%q", record.Data[:4])
		if header == magic {
			protoRecords, err := getProtoRecords(record)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"err": err.Error(),
				}).Error("Error Expanding Proto Records")
				return nil, err
			}
			allRecords = append(allRecords, protoRecords...)
		} else {
			allRecords = append(allRecords, *record)
		}
	}
	return allRecords, nil
}

func getProtoRecords(record *kinesis.Record) ([]kinesis.Record, error) {
	records := make([]kinesis.Record, 0)
	md5Buffer := 15
	msg := record.Data[4 : len(record.Data)-1-md5Buffer]
	aggRecord := &rec.AggregatedRecord{}
	err := proto.Unmarshal(msg, aggRecord)

	if err != nil {
		return records, err
	}

	for _, aggrec := range aggRecord.Records {
		r := kinesis.Record{
			ApproximateArrivalTimestamp: record.ApproximateArrivalTimestamp,
			Data:                        (*aggrec).Data,
			EncryptionType:              record.EncryptionType,
			PartitionKey:                record.PartitionKey,
			SequenceNumber:              record.SequenceNumber,
		}
		records = append(records, r)
	}

	return records, nil
}

// captureShard blocks until we capture the given shardID
func (k *Kinsumer) captureShard(shardID string) (*checkpointer, error) {
	// Attempt to capture the shard in dynamo
	for {
		// Ask the checkpointer to capture the shard
		checkpointer, err := capture(
			shardID,
			k.checkpointTableName,
			k.dynamodb,
			k.clientName,
			k.clientID,
			k.maxAgeForClientRecord,
			k.config.stats)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"shardId": shardID,
				"err":     err.Error(),
			}).Error("Unable to capture shard")
			return nil, err
		}

		if checkpointer != nil {
			logger.WithFields(logrus.Fields{
				"shardId": shardID,
			}).Debug("Successfully captured shard")
			return checkpointer, nil
		}

		// Throttle requests so that we don't hammer dynamo
		select {
		case <-k.stop:
			// If we are told to stop consuming we should stop attempting to capture
			return nil, nil
		case <-time.After(k.config.throttleDelay):
		}
	}
}

func processRecords(records []kinesis.Record, rules *map[string]*rl.Rulebase, cp *checkpointer, shardID string) {
	lastSeqNum := *records[len(records)-1].SequenceNumber
	for _, record := range records {
		tmp := make([]byte, len(record.Data))
		copy(tmp, record.Data)
		buffer, err := snappy.Decode(nil, tmp)
		payload := string(buffer)

		tmpSeqNum := *record.SequenceNumber
		if err != nil {
			logger.WithFields(logrus.Fields{
				"shardId": shardID,
				"err":     err.Error(),
			}).Fatal("Error running rule")
		} else if len(buffer) > 0 {
			for _, rule := range *rules {
				match, outbound, err := ruleset.RuleMatch(rule, payload)
				if match && err == nil && outbound != nil {
					success, err := rl.RunRule(rule, tmpSeqNum, outbound)
					if !success && err != nil {
						logger.WithFields(logrus.Fields{
							"shardId": shardID,
							"err":     err.Error(),
						}).Fatal("Error running rule")
					}
				}
			}
		}
	}
	cp.update(lastSeqNum)
}

// consume is a blocking call that captures then consumes the given shard in a loop.
// It is also responsible for writing out the checkpoint updates to dynamo.
// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consume(shardID string) {
	dimensions := make(map[string]string)
	dimensions["shardId"] = shardID
	defer k.waitGroup.Done()
	// need to init the rules here so we get individual workers per shard (i.e. individual kinsumers for emit and batch)
	builtRules, _ := ruleset.InitWorkerRuleset(shardID)

	// commitTicker is used to periodically commit, so that we don't hammer dynamo every time
	// a shard wants to be check pointed
	commitTicker := time.NewTicker(k.config.commitFrequency)
	defer commitTicker.Stop()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"shardId": shardID,
			"action":  "captureShard",
			"err":     err.Error(),
		}).Error("Shard Error")
		metricSender.SendCounter("critical_errors_count", int64(1), dimensions)
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "captureShard", err: err}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.sequenceNumber

	// finished means we have reached the end of the shard but haven't necessarily processed/committed everything
	finished := false
	// Make sure we release the shard when we are done.
	defer func() {
		innerErr := checkpointer.release()
		if innerErr != nil {
			logger.WithFields(logrus.Fields{
				"shardId": shardID,
				"action":  "checkpointer.release",
				"err":     innerErr.Error(),
			}).Error("Shard Error")
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.release", err: innerErr}
			return
		}
	}()

	// Get the starting shard iterator
	iterator, err := getShardIterator(k.kinesis, k.streamName, shardID, sequenceNumber)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"shardId": shardID,
			"action":  "getShardIterator",
			"err":     err.Error(),
		}).Error("Shard Error")
		metricSender.SendCounter("critical_errors_count", int64(1), dimensions)
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "getShardIterator", err: err}
		return
	}

	// no throttle on the first request.
	nextThrottle := time.After(0)

	retryCount := 0

	var lastSeqNum string
	var records []kinesis.Record
	var next string
	var millisBehind int64

	finishCommit := make(chan bool)

	go func() {
		for {
			select {
			case <-commitTicker.C:
				logger.WithFields(logrus.Fields{
					"shardId": shardID,
				}).Debug("Finished Commit Ticker")
				finishCommitted, err := checkpointer.commit()
				if err != nil {
					logger.WithFields(logrus.Fields{
						"shardId": shardID,
						"action":  "checkpointer.commit",
						"err":     err.Error(),
					}).Error("Shard Error")
					metricSender.SendCounter("critical_errors_count", int64(1), dimensions)
					k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.commit", err: err}
					finishCommit <- true
					return
				}
				if finishCommitted {
					finishCommit <- true
					return
				}
				metricSender.SendCounter("checkpoint_count", int64(1), dimensions)
			}
		}
	}()

mainloop:
	for {
		// We have reached the end of the shard's data. Set Finished in dynamo and stop processing.
		if iterator == "" && !finished {
			logger.WithFields(logrus.Fields{
				"shardID": shardID,
				"iterator" : iterator,
				"finished" : finished,
			}).Info("Shard finished")
			checkpointer.finish(lastSeqNum)
			finished = true
			runtime.GC()
		}

		// Handle async actions, and throttle requests to keep kinesis happy
		select {
		case <-k.stop:
			return
		case <-finishCommit:
			return
                case <-nextThrottle:
		}

		// Reset the nextThrottle
		nextThrottle = time.After(k.config.throttleDelay)

		if finished {
			continue mainloop
		}

		// Get records from kinesis
		records, next, millisBehind, err = getRecords(k.kinesis, iterator)


		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				logger.WithFields(logrus.Fields{
					"shardId":         shardID,
					"action":          "getRecords",
					"err":             awsErr.Message(),
					"origErr":         awsErr.OrigErr(),
					"retryCount":      retryCount,
					"maxErrorRetries": maxErrorRetries,
				}).Error("Shard Error")
				metricSender.SendCounter("critical_errors_count", int64(1), dimensions)
				if retryCount < maxErrorRetries {
					retryCount++

					// casting retryCount here to time.Duration purely for the multiplication, there is
					// no meaning to retryCount nanoseconds
					time.Sleep(errorSleepDuration * time.Duration(retryCount))
					continue mainloop
				}
			}
			logger.WithFields(logrus.Fields{
				"shardId": shardID,
				"action":  "getRecords",
				"err":     err.Error(),
			}).Error("Shard Error")
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "getRecords", err: err}
			return
		}
		retryCount = 0

		if next == iterator {
			logger.WithFields(logrus.Fields{
				"iterator": iterator,
				"next":     next,
			}).Info("The iterator/Next are the same")
		}

		if len(records) > 0 {
			metricSender.SendCounter("record_count", int64(len(records)), dimensions)
			metricSender.SendGauge("millis_behind", millisBehind, dimensions)

			go processRecords(records, builtRules, checkpointer, shardID)

			// Update the last sequence number we saw, in case we reached the end of the stream.
			lastSeqNum = aws.StringValue(records[len(records)-1].SequenceNumber)
		}
		iterator = next
	}


}

// consume is a blocking call that captures then consumes the given shard in a loop.
// It is also responsible for writing out the checkpoint updates to dynamo.
// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consumeEnhancedFanout(shardID string) {
	dimensions := make(map[string]string)
	dimensions["shardId"] = shardID
	defer k.waitGroup.Done()
	// need to init the rules here so we get individual workers per shard (i.e. individual kinsumers for emit and batch)
	builtRules, _ := ruleset.InitWorkerRuleset(shardID)

	// commitTicker is used to periodically commit, so that we don't hammer dynamo every time
	// a shard wants to be check pointed
	commitTicker := time.NewTicker(k.config.commitFrequency)
	defer commitTicker.Stop()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"shardId": shardID,
			"action":  "captureShard",
			"err":     err.Error(),
		}).Error("Shard Error")
		metricSender.SendCounter("critical_error_count", int64(1), dimensions)
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "captureShard", err: err}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.sequenceNumber

	// finished means we have reached the end of the shard but haven't necessarily processed/committed everything
	finished := false
	// Make sure we release the shard when we are done.
	defer func() {
		innerErr := checkpointer.release()
		if innerErr != nil {
			logger.WithFields(logrus.Fields{
				"shardId": shardID,
				"action":  "checkpointer.release",
				"err":     innerErr.Error(),
			}).Error("Shard Error")
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.release", err: innerErr}
			return
		}
	}()


	startingPosition := &kinesis.StartingPosition{}
	var startPositionType = "AT_SEQUENCE_NUMBER"
	if strings.TrimSpace(sequenceNumber) == "" {
		startPositionType = "TRIM_HORIZON"
	} else {
		startingPosition.SequenceNumber = aws.String(sequenceNumber)
	}
	startingPosition.Type = aws.String( startPositionType )

	// no throttle on the first request.
	SubscribeToShardEventStream, err := k.kinesis.SubscribeToShard(&kinesis.SubscribeToShardInput{
		ConsumerARN:      aws.String(k.registeredConsumerARN),
		ShardId:          aws.String(shardID),
		StartingPosition: startingPosition,
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"shardId": shardID,
			"action":  "subtoShard",
			"err":     err.Error(),
		}).Error("Shard Error")
		return
	}
	SubscribeToShardEventChannel := SubscribeToShardEventStream.EventStream.Events()
	subtoshardevtptr := <-SubscribeToShardEventChannel
	SubscribeToShardEvent := subtoshardevtptr.(*kinesis.SubscribeToShardEvent)
	nextThrottle := time.After(0)

	var lastSeqNum string
	var millisBehind int64
	nilCaught := false

mainloop:
	for {
		lag := *SubscribeToShardEvent.MillisBehindLatest
		// We have reached the end of the shard's data. Set Finished in dynamo and stop processing.
		if (lag == 0 && !finished) || (nilCaught && !finished) {
			checkpointer.finish(lastSeqNum)
			finished = true
			runtime.GC()
		}

		// Handle async actions, and throttle requests to keep kinesis happy
		select {
		case <-k.stop:
			return
		case <-commitTicker.C:
			logger.WithFields(logrus.Fields{
				"shardId": shardID,
			}).Debug("Finished Commit Ticker")
			finishCommitted, err := checkpointer.commit()
			if err != nil {
				logger.WithFields(logrus.Fields{
					"shardId": shardID,
					"action":  "checkpointer.commit",
					"err":     err.Error(),
				}).Error("Shard Error")
				metricSender.SendCounter("critical_error_count", 1, dimensions)
				k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.commit", err: err}
				return
			}

			if checkpointer.sequenceNumber == sequenceNumber {
				startPositionType = "AFTER_SEQUENCE_NUMBER"
			} else {
				startPositionType = "AT_SEQUENCE_NUMBER"
			}
			sequenceNumber = checkpointer.sequenceNumber
			_, err = k.kinesis.SubscribeToShard(&kinesis.SubscribeToShardInput{
				ConsumerARN:      aws.String(k.registeredConsumerARN),
				ShardId:          aws.String(shardID),
				StartingPosition: &kinesis.StartingPosition{Type: aws.String(startPositionType), SequenceNumber: aws.String(sequenceNumber)},
			})
			if err != nil {
				logger.WithFields(logrus.Fields{
					"shardId": shardID,
					"action":  "subtoShard",
					"err":     err.Error(),
				}).Error("Shard Error")
			}
			if finishCommitted {
				return
			}
			metricSender.SendCounter("checkpoint_count", int64(1), dimensions)
			// Go back to waiting for a throttle/stop.
			continue mainloop
		case <-nextThrottle:
		}

		// Reset the nextThrottle
		nextThrottle = time.After(k.config.throttleDelay)

		if finished {
			continue mainloop
		}

		// Get records from kinesis
		recs := SubscribeToShardEvent.Records
		// Retry for records attempt???
		millisBehind = *SubscribeToShardEvent.MillisBehindLatest
		records, _ := expandProtoRecords(recs)

		if len(records) > 0 {
			metricSender.SendCounter("record_count", int64(len(records)), dimensions)
			metricSender.SendGauge("millis_behind", millisBehind, dimensions)
			for _, record := range records {
				// Create channel per record to prevent cross goroutine
				checkChan := make(chan int)
				buffer, err := snappy.Decode(nil, record.Data)
				payload := string(buffer)
				if err == nil {
					recordStruct := &chk.CheckpointIdentifier{
						Id:      record.SequenceNumber,
						ShardId: &shardID,
						Payload: &payload,
						Channel: checkChan,
					}

					// Run rules should take the inited rules above and execute those
					if len(*builtRules) > 0 && len(buffer) > 0  && recordStruct != nil {
						//commented out because of http and uint8 error
						//logger.WithFields(logrus.Fields{
						//	"rules":   builtRules,
						//	"shardId": shardID,
						//}).Debug("Running worker rules")
						//ruleset.RunRules(recordStruct, builtRules)
						//go checkpointer.update(record.SequenceNumber, checkChan, chkRefs)
					}
				} else {
					logger.WithFields(logrus.Fields{
						"shardId": shardID,
						"snappy":  string(record.Data),
						"decoded": payload,
					}).Error("Error decoding record")
				}
			}

			// Update the last sequence number we saw, in case we reached the end of the stream.
			lastSeqNum = aws.StringValue(records[len(records)-1].SequenceNumber)
		}
		SubscribeToShardEventChannel = SubscribeToShardEventStream.EventStream.Events()
		subtoshardevtptr = <-SubscribeToShardEventChannel
		if subtoshardevtptr != nil {
			SubscribeToShardEvent = subtoshardevtptr.(*kinesis.SubscribeToShardEvent)
		} else {
			nilCaught = true
		}
	}
}
