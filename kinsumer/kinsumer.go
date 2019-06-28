package kinsumer

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
)

type shardConsumerError struct {
	shardID string
	action  string
	err     error
}

type consumedRecord struct {
	record       *kinesis.Record // Record retrieved from kinesis
	checkpointer *checkpointer   // Object that will store the checkpoint back to the database
	retrievedAt  time.Time       // Time the record was retrieved from Kinesis
}

// Kinsumer is a Kinesis Consumer that tries to reduce duplicate reads while allowing for multiple
// clients each processing multiple shards
type Kinsumer struct {
	kinesis               kinesisiface.KinesisAPI    // interface to the kinesis service
	dynamodb              dynamodbiface.DynamoDBAPI  // interface to the dynamodb service
	s3manager             s3manageriface.UploaderAPI // interface to upload to s3
	streamName            string                     // name of the kinesis stream to consume from
	shardIDs              []string                   // all the shards in the stream, for detecting when the shards change
	stop                  chan struct{}              // channel used to signal to all the go routines that we want to stop consuming
	stoprequest           chan bool                  // channel used internally to signal to the main go routine to stop processing
	errors                chan error                 // channel used to communicate errors back to the caller
	waitGroup             sync.WaitGroup             // waitGroup to sync the consumers go routines on
	mainWG                sync.WaitGroup             // WaitGroup for the mainLoop
	shardErrors           chan shardConsumerError    // all the errors found by the consumers that were not handled
	clientsTableName      string                     // dynamo table of info about each client
	checkpointTableName   string                     // dynamo table of the checkpoints for each shard
	metadataTableName     string                     // dynamo table of metadata about the leader and shards
	clientID              string                     // identifier to differentiate between the running clients
	clientName            string                     // display name of the client - used just for debugging
	totalClients          int                        // The number of clients that are currently working on this stream
	thisClient            int                        // The (sorted by name) index of this client in the total list
	config                Config                     // configuration struct
	numberOfRuns          int32                      // Used to atomically make sure we only ever allow one Run() to be called
	isLeader              bool                       // Whether this client is the leader
	leaderLost            chan bool                  // Channel that receives an event when the node loses leadership
	leaderWG              sync.WaitGroup             // waitGroup for the leader loop
	maxAgeForClientRecord time.Duration              // Cutoff for client/checkpoint records we read from dynamodb before we assume the record is stale
	maxAgeForLeaderRecord time.Duration              // Cutoff for leader/shard cache records we read from dynamodb before we assume the record is stale
	registeredConsumerARN string                     // registers consumer with AWS to allow for use of enhanced fanout functionality
}

var logger = logging.GetLogger()
var metricSender = metrics.GetSfxClient()

// New returns a Kinsumer Interface with default kinesis and dynamodb instances, to be used in ec2 instances to get default auth and config
func New(streamName, applicationName, clientName string, config Config, role string, consumerARNvalue string) (*Kinsumer, error) {
	s, err := session.NewSession()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Error("error starting new AWS session")
		return nil, err
	}
	return NewWithSession(s, streamName, applicationName, clientName, config, role, consumerARNvalue)
}

// NewWithSession should be used if you want to override the Kinesis and Dynamo instances with a non-default aws session
func NewWithSession(session *session.Session, streamName, applicationName, clientName string, config Config, role string, consumerARNvalue string) (*Kinsumer, error) {
	var k *kinesis.Kinesis
	if role != "" {
		creds := stscreds.NewCredentials(session, role)
		k = kinesis.New(session, &aws.Config{Credentials: creds})
	} else {
		k = kinesis.New(session)
	}
	d := dynamodb.New(session)
	su := s3manager.NewUploader(session)

	return NewWithInterfaces(k, d, su, streamName, applicationName, clientName, config, role, consumerARNvalue)
}

// NewWithInterfaces allows you to override the Kinesis and Dynamo instances for mocking or using a local set of servers
func NewWithInterfaces(kinesisAPI kinesisiface.KinesisAPI, dynamodb dynamodbiface.DynamoDBAPI, s3manager s3manageriface.UploaderAPI, streamName, applicationName, clientName string, config Config, role string, consumerARNvalue string) (*Kinsumer, error) {
	if kinesisAPI == nil {
		logger.WithFields(logrus.Fields{
			"err": ErrNoKinesisInterface.Error(),
		}).Error("No kinesis interface")
		return nil, ErrNoKinesisInterface
	}
	if dynamodb == nil {
		logger.WithFields(logrus.Fields{
			"err": ErrNoDynamoInterface.Error(),
		}).Error("No dynamo interface")
		return nil, ErrNoDynamoInterface
	}
	if streamName == "" {
		logger.WithFields(logrus.Fields{
			"err": ErrNoStreamName.Error(),
		}).Error("No stream name provided")
		return nil, ErrNoStreamName
	}
	if applicationName == "" {
		logger.WithFields(logrus.Fields{
			"err": ErrNoApplicationName.Error(),
		}).Error("No application name provided")
		return nil, ErrNoApplicationName
	}
	if err := validateConfig(&config); err != nil {
		logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Error("Invalid configs for kinsumer")
		return nil, err
	}

	consumer := &Kinsumer{
		streamName:            streamName,
		kinesis:               kinesisAPI,
		dynamodb:              dynamodb,
		s3manager:             s3manager,
		stoprequest:           make(chan bool),
		errors:                make(chan error, 10),
		shardErrors:           make(chan shardConsumerError, 10),
		checkpointTableName:   applicationName + "_checkpoints",
		clientsTableName:      applicationName + "_clients",
		metadataTableName:     applicationName + "_metadata",
		clientID:              uuid.NewV4().String(),
		clientName:            clientName,
		config:                config,
		maxAgeForClientRecord: config.shardCheckFrequency * 5,
		maxAgeForLeaderRecord: config.leaderActionFrequency * 5,
		registeredConsumerARN: consumerARNvalue,
	}

	logger.WithFields(logrus.Fields{
		"stream":              streamName,
		"checkpointTableName": applicationName + "_checkpoints",
		"clientsTableName":    applicationName + "_clients",
		"metadataTableName":   applicationName + "_metadata",
		"name":                clientName,
		"clientID":            consumer.clientID,
	}).Info("Successfully created new kinsumer")

	return consumer, nil
}

func (k *Kinsumer) GetClientName() string {
	return k.clientName
}

// refreshShards registers our client, refreshes the lists of clients and shards, checks if we
// have become/unbecome the leader, and returns whether the shards/clients changed.
//TODO: Write unit test - needs dynamo _and_ kinesis mocking
func (k *Kinsumer) refreshShards() (bool, error) {
	var shardIDs []string

	if err := registerWithClientsTable(k.dynamodb, k.clientID, k.clientName, k.clientsTableName); err != nil {
		logger.WithFields(logrus.Fields{
			"clientsTableName": k.clientsTableName,
			"name":             k.clientName,
			"clientID":         k.clientID,
			"err":              err.Error(),
		}).Error("Error registering with clients table")
		return false, err
	}

	//TODO: Move this out of refreshShards and into refreshClients
	clients, err := getClients(k.dynamodb, k.clientID, k.clientsTableName, k.maxAgeForClientRecord)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"clientsTableName": k.clientsTableName,
			"clientID":         k.clientID,
			"err":              err.Error(),
		}).Error("Error getting clients")
		return false, err
	}

	totalClients := len(clients)
	thisClient := 0

	found := false
	for i, c := range clients {
		if c.ID == k.clientID {
			thisClient = i
			found = true
			break
		}
	}

	if !found {
		return false, ErrThisClientNotInDynamo
	}

	if thisClient == 0 && !k.isLeader {
		k.becomeLeader()
	} else if thisClient != 0 && k.isLeader {
		k.unbecomeLeader()
	}

	shardIDs, err = loadShardIDsFromDynamo(k.dynamodb, k.metadataTableName)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"metadataTableName": k.metadataTableName,
			"err":               err.Error(),
		}).Error("Error loading shardIds from dynamo")
		return false, err
	}

	changed := (totalClients != k.totalClients) ||
		(thisClient != k.thisClient) ||
		(len(k.shardIDs) != len(shardIDs))

	if !changed {
		for idx := range shardIDs {
			if shardIDs[idx] != k.shardIDs[idx] {
				changed = true
				break
			}
		}
	}

	if changed {
		k.shardIDs = shardIDs
	}

	k.thisClient = thisClient
	k.totalClients = totalClients

	logger.WithFields(logrus.Fields{
		"name":     k.clientName,
		"clientID": k.clientID,
	}).Info("Successfully refreshed shards")

	return changed, nil
}

// startConsumers launches a shard consumer for each shard we should own
// TODO: Can we unit test this at all?
func (k *Kinsumer) startConsumers() error {
	k.stop = make(chan struct{})
	assigned := false

	if k.thisClient >= len(k.shardIDs) {
		return nil
	}
	if k.registeredConsumerARN == "" {
		for i, shard := range k.shardIDs {
			if (i % k.totalClients) == k.thisClient {
				k.waitGroup.Add(1)
				assigned = true
				logger.WithFields(logrus.Fields{
					"shardId": shard,
				}).Debug("Begin consuming records on shard")
				go k.consume(shard)
			}
		}
	} else {
		for i, shard := range k.shardIDs {
			if (i % k.totalClients) == k.thisClient {
				k.waitGroup.Add(1)
				assigned = true
				logger.WithFields(logrus.Fields{
					"shardId": shard,
				}).Debug("Begin consuming records on shard")
				go k.consumeEnhancedFanout(shard)
			}
		}
	}

	if len(k.shardIDs) != 0 && !assigned {
		logger.WithFields(logrus.Fields{
			"err": ErrNoShardsAssigned.Error(),
		}).Error("Error no shards assigned")
		return ErrNoShardsAssigned
	}

	logger.WithFields(logrus.Fields{
		"numShards": len(k.shardIDs),
		"shards":    k.shardIDs,
	}).Info("Successfully started consuming shards")

	return nil
}

// stopConsumers stops all our shard consumers
func (k *Kinsumer) stopConsumers() {
	logger.WithFields(logrus.Fields{
		"numShards": len(k.shardIDs),
		"shards":    k.shardIDs,
	}).Info("Stopping shard consumers")

	close(k.stop)
	k.waitGroup.Wait()
}

func (k *Kinsumer) uploadToS3(file io.Reader, bucket string, key string) error {
	_, err := k.s3manager.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	return err
}

// Publish to write some data into stream.
func (k *Kinsumer) Publish(streamName, partitionKey string, data *[]byte) error {
	_, err := k.kinesis.PutRecord(&kinesis.PutRecordInput{
		Data:         *data,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(partitionKey),
	})
	return err
}

// dynamoTableActive returns an error if the given table is not ACTIVE
func (k *Kinsumer) dynamoTableActive(name string) error {
	out, err := k.dynamodb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":  err.Error(),
			"name": name,
		}).Error("Error describing dynamo table")
		return fmt.Errorf("error describing table %s: %v", name, err)
	}
	status := aws.StringValue(out.Table.TableStatus)
	if status != "ACTIVE" {
		logger.WithFields(logrus.Fields{
			"name":   name,
			"status": status,
		}).Error("Table is not in active state")
		return fmt.Errorf("table %s exists but state '%s' is not 'ACTIVE'", name, status)
	}
	return nil
}

// dynamoTableExists returns an true if the given table exists
func (k *Kinsumer) dynamoTableExists(name string) bool {
	_, err := k.dynamodb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	return err == nil
}

// dynamoCreateTableIfNotExists creates a table with the given name and distKey
// if it doesn't exist and will wait until it is created
func (k *Kinsumer) dynamoCreateTableIfNotExists(name, distKey string) error {
	if k.dynamoTableExists(name) {
		return nil
	}
	_, err := k.dynamodb.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{{
			AttributeName: aws.String(distKey),
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		}},
		KeySchema: []*dynamodb.KeySchemaElement{{
			AttributeName: aws.String(distKey),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		}},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(k.config.dynamoReadCapacity),
			WriteCapacityUnits: aws.Int64(k.config.dynamoWriteCapacity),
		},
		TableName: aws.String(name),
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"name":    name,
			"distKey": distKey,
			"err":     err.Error(),
		}).Error("Unable to create table in dynamo")
		return err
	}
	err = k.dynamodb.WaitUntilTableExistsWithContext(
		aws.BackgroundContext(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		},
		request.WithWaiterDelay(request.ConstantWaiterDelay(k.config.dynamoWaiterDelay)),
	)
	return err
}

// dynamoDeleteTableIfExists delete a table with the given name if it exists
// and will wait until it is deleted
func (k *Kinsumer) dynamoDeleteTableIfExists(name string) error {
	if !k.dynamoTableExists(name) {
		return nil
	}
	_, err := k.dynamodb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"name": name,
			"err":  err.Error(),
		}).Error("Unable to delete table in dynamo")
		return err
	}
	err = k.dynamodb.WaitUntilTableNotExistsWithContext(
		aws.BackgroundContext(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		},
		request.WithWaiterDelay(request.ConstantWaiterDelay(k.config.dynamoWaiterDelay)),
	)
	return err
}

// kinesisStreamReady returns an error if the given stream is not ACTIVE
func (k *Kinsumer) kinesisStreamReady() error {
	out, err := k.kinesis.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(k.streamName),
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"name": k.streamName,
			"err":  err.Error(),
		}).Error("Error describing stream")
		return fmt.Errorf("error describing stream %s: %v", k.streamName, err)
	}

	status := aws.StringValue(out.StreamDescription.StreamStatus)
	if status != "ACTIVE" {
		logger.WithFields(logrus.Fields{
			"name":   k.streamName,
			"status": status,
		}).Error("Kinesis Stream is not in active state")
		return fmt.Errorf("stream %s exists but state '%s' is not 'ACTIVE'", k.streamName, status)
	}

	return nil
}

// Run runs the main kinesis consumer process. This is a non-blocking call, use Stop() to force it to return.
// This goroutine is responsible for startin/stopping consumers, aggregating all consumers' records,
// updating checkpointers as records are consumed, and refreshing our shard/client list and leadership
//TODO: Can we unit test this at all?
func (k *Kinsumer) Run(fatalErr chan error) error {
	if err := k.dynamoTableActive(k.checkpointTableName); err != nil {
		logger.WithFields(logrus.Fields{
			"name": k.checkpointTableName,
			"err":  err.Error(),
		}).Error("Checkpoint table not active")
		return err
	}
	if err := k.dynamoTableActive(k.clientsTableName); err != nil {
		logger.WithFields(logrus.Fields{
			"name": k.clientsTableName,
			"err":  err.Error(),
		}).Error("Client table not active")
		return err
	}
	if err := k.kinesisStreamReady(); err != nil {
		logger.WithFields(logrus.Fields{
			"name": k.streamName,
			"err":  err.Error(),
		}).Error("Kinesis stream not ready")
		return err
	}

	allowRun := atomic.CompareAndSwapInt32(&k.numberOfRuns, 0, 1)
	if !allowRun {
		logger.WithFields(logrus.Fields{
			"name": k.streamName,
			"err":  ErrRunTwice.Error(),
		}).Error("Kinsumer.Run() can only be called once")
		return ErrRunTwice
	}

	if _, err := k.refreshShards(); err != nil {
		deregErr := deregisterFromClientsTable(k.dynamodb, k.clientID, k.clientsTableName)
		if deregErr != nil {
			logger.WithFields(logrus.Fields{
				"name":     k.streamName,
				"err":      err.Error(),
				"deregErr": deregErr.Error(),
			}).Error("error in kinsumer Run initial refreshShards, error deregistering from clients table")
			return fmt.Errorf("error in kinsumer Run initial refreshShards: (%v); "+
				"error deregistering from clients table: (%v)", err, deregErr)
		}
		logger.WithFields(logrus.Fields{
			"name": k.streamName,
			"err":  err.Error(),
		}).Error("error in kinsumer Run initial refreshShards")
		return fmt.Errorf("error in kinsumer Run initial refreshShards: %v", err)
	}

	k.mainWG.Add(1)
	go func() {
		defer k.mainWG.Done()

		defer func() {
			// Deregister is a nice to have but clients also time out if they
			// fail to deregister, so ignore error here.
			err := deregisterFromClientsTable(k.dynamodb, k.clientID, k.clientsTableName)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"clientId": k.clientID,
					"table":    k.clientsTableName,
					"err":      err.Error(),
				}).Error("error deregistering client")
				k.errors <- fmt.Errorf("error deregistering client: %s", err)
			}
			if k.isLeader {
				close(k.leaderLost)
				k.leaderLost = nil
				k.isLeader = false
			}
			// Do this outside the k.isLeader check in case k.isLeader was false because
			// we lost leadership but haven't had time to shutdown the goroutine yet.
			k.leaderWG.Wait()
		}()

		shardChangeTicker := time.NewTicker(k.config.shardCheckFrequency)
		defer func() {
			shardChangeTicker.Stop()
		}()

		if err := k.startConsumers(); err != nil {
			logger.WithFields(logrus.Fields{
				"clientId": k.clientID,
				"err":      err.Error(),
			}).Error("error starting consumers")
			fatalErr <- fmt.Errorf("error starting consumers: %s", err)
		}
		defer k.stopConsumers()

		for {
			select {
			case <-k.stoprequest:
				return
			case se := <-k.shardErrors:
				logger.WithFields(logrus.Fields{
					"shardId": se.shardID,
					"action":  se.action,
					"err":     se.err.Error(),
				}).Error("Shard Error")
				k.errors <- fmt.Errorf("shard error (%s) in %s: %s", se.shardID, se.action, se.err)
			case <-shardChangeTicker.C:
				changed, err := k.refreshShards()
				if err != nil {
					logger.WithFields(logrus.Fields{
						"err": err.Error(),
					}).Error("error refreshing shards")
					k.errors <- fmt.Errorf("error refreshing shards: %s", err)
				} else if changed {
					shardChangeTicker.Stop()
					k.stopConsumers()

					if err := k.startConsumers(); err != nil {
						logger.WithFields(logrus.Fields{
							"err": err.Error(),
						}).Error("error restarting consumers")
						k.errors <- fmt.Errorf("error restarting consumers: %s", err)
					}
					// We create a new shardChangeTicker here so that the time it takes to stop and
					// start the consumers is not included in the wait for the next tick.
					shardChangeTicker = time.NewTicker(k.config.shardCheckFrequency)
				}
			}
		}
	}()

	return nil
}

// Stop stops the consumption of kinesis events
//TODO: Can we unit test this at all?
func (k *Kinsumer) Stop() {
	k.stoprequest <- true
	k.mainWG.Wait()
}

// CreateRequiredTables will create the required dynamodb tables
// based on the applicationName
func (k *Kinsumer) CreateRequiredTables() error {
	g := &errgroup.Group{}

	g.Go(func() error {
		return k.dynamoCreateTableIfNotExists(k.clientsTableName, "ID")
	})
	g.Go(func() error {
		return k.dynamoCreateTableIfNotExists(k.checkpointTableName, "Shard")
	})
	g.Go(func() error {
		return k.dynamoCreateTableIfNotExists(k.metadataTableName, "Key")
	})

	return g.Wait()
}

// DeleteTables will delete the dynamodb tables that were created
// based on the applicationName
func (k *Kinsumer) DeleteTables() error {
	g := &errgroup.Group{}

	g.Go(func() error {
		return k.dynamoDeleteTableIfExists(k.clientsTableName)
	})
	g.Go(func() error {
		return k.dynamoDeleteTableIfExists(k.checkpointTableName)
	})
	g.Go(func() error {
		return k.dynamoDeleteTableIfExists(k.metadataTableName)
	})

	return g.Wait()
}
