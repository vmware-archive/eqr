
# EQR
### Core Developer Team
Anthony Merrill (cb-anthony-merrill), David Patawaran (dpat-cb), Jeremy Kelley (nod), Michael Xiao (mxiao-cb), Xavier Thierry (cb-xav)

### Purpose
The Event Query Router (EQR) was a solution to Carbon Black’s need to provide data quickly to internal stakeholders such as Threat Research, Threat Analysis Unit (TAU) and Analytics. Current solutions require adding business logic to predefined programs written in other languages like Python and Scala. This means that adding or changing destinations, fields require a developer to add the code, test, verify and deploy manually. The manual process does not provide Carbon Black’s internal stakeholders the rapid delivery they require to be successful. 

The solution, EQR, allows teams providing the manual service, relief in terms of having to provide ad hoc services to other stakeholders. It also provides these stakeholders the ability to write simple logic in a defined language by EQR to get the data they require quickly and delivered at the endpoint and in the format they need. The language is part of two foundational pieces that make up EQR: EQRLang and the Plugin System and does not require prior knowledge of other programming languages, build and large deployment processes.

EQR can be visualized at a high level as a simple interoperability and translation layer between AWS services as seen in the image below. However, the rules and plugin system allows the injection and outbound layers configurable to a user’s need.


### EQR Architecture
EQR stems around the idea of simple worker(s), that run an EQR rule against incoming data. This incoming data is assumed to be streaming such as records from a Kinesis stream. That data is the run through the elements of an EQR Rule, and the resulting information can be sent to a new destination.

Below is a visual of a typical EQR Application Workflow

![alt text](https://github.com/carbonblack/eqr/blob/master/docs/images/EQRWorkflow.png "EQR Application Workflow")


The first step when EQR starts up is pulls the necessary Rulesets, Rules, and Plugins from an S3 bucket supplied by the developer. The files are then downloaded stored, and Plugins are built at runtime. 

EQR will then take the rules are run them through the EQRLang processor where rules will be broken down into their Destination, Predicate, and Projection components. See EQRLang for further explanation. 

When EQR has successfully built the rules, the defined consuming rule will then begin to process. Assuming we are consuming from a Kinesis stream, each record that is pulled from the shard(s) is then sent to the EQRLang processor and run through the built rules. If a record matches the Predicate defined the rule, the Projection process begins which will translate the data as defined and send to the final destination.

### EQR Rulesets
Rulesets are a group of rules that an EQR worker will need to perform. The rulesets are defined in JSON and contain a limited amount of information, enough for EQR to download and setup rules. There are three requirements for a Ruleset.

1. Rulesets must have a 'RuleName' defined.
 
    *It is assumed and appropriate the filename is the RuleName.*

2. Rulesets must have a 'RecordGenerator' rule defined. 

    *This rule is responsible for pulling records from a source, this record will be sent to the rules.*
    
3. Rulesets must have at minimum one 'RecordRules' defined.

    *A JSON array must be provided. The rules contain the Predicate, Projection and Destination information.*

`{
    "RuleName" : "ExampleRuleset",
    "RecordGenerator" : "ConsumingRule.eqr",
    "RecordRules": [
	    { "Rule": "batchRule.eqr", "checkpoint": true },
	    { "Rule": "streamRule.eqr", "checkpoint": false }
    ]
}`

### EQR Rule
An EQR Rule is the actual definition of a rule. It contains information about where data will be pulled and/or sent from, the conditions to match a record on; and finally, how to alter or translate the data to be sent to the destination.

A simple EQR rule can look like the following.
```
CONSUME.MONGO_TEST_PLUGIN[]({},{})
```

The Rule above tells EQR that we are going to be consuming from a MongoDB, specifically a plugin that we've written called 'MONGO_TEST_PLUGIN'. The data that comes from this Plugin will be utilized as the record to send to other rules. For this example, lets assume the plugin mentioned above returns a JSON string.

Lets say a resulting record from the plugin looks like the following: 

```json
{
  "SomeKey": {
      "HasTowel": true,
      "Number": 42,
      "FirstName": "Arthur",
      "LastName": "Dent"
  },
  "OtherVals": {...}
}
```

Once a record is pulled from the consuming plugin it is sent to a record, that may look like this.

```
S3.BATCH[...parameters...](
 	{
 	    JSON(SomeKey) != null;
 	},
 	{
 	    SEND[
 		    SENDALL(*);
 	    ]
 	})
```

The Rule above does a few things. It defines that the destination is going to utilize the Batching plugin to S3 that comes as part of the EQR Language. The Predicate tells us, that whatever record is coming through on this rule has to have a JSON key of 'SomeKey' and it cannot be null. If a record comes through and has a JSON value with they key 'SomeKey' we will send ALL contents of the record to the destination.

Rules however, can be more complex. Depending on the data and translation someone wants an EQR rule can easily do the translation with a variety of plugins and functions that are easily extensible. 

Assuming we are still getting JSON data, we could specify a rule like the following.

```
EMIT.KINESIS[myKinesisStream,  ]({
	JSON(SomeKey) != null;
	JSON(SomeKey.HasTowel) == true;
	JSON(SomeKey.Number) <= 42;
	},
	{
		SEND[
			JSON(SomeKey.MyChildKey) AS new_keyname;
			JSON(SomeKey.Number) AS answer_to_life;
			SPRINTF(STR(Hello my name is - %v %v.), JSON(SomeKey.FirstName), JSON(SomeKey.LastName)) AS name
		]
	})
```

The rule would take the record coming from the Consume MongoDB plugin and match the Predicate. If 'SomeKey' exists as-well as 'HasTowel' is true and 'Number' is less than or equal to 42 we will send a JSON formatted result that may look like the following.

```json
{
  "new_keyname": "Test123",
  "answer_to_life": "42",
  "name": "Hello my name is - Arthur Dent."
}
```

EQR Provides a way to be both compact and verbose for translating data at real-time. It allows users to write smaller, simpler and easier rules for doing data translations. 

###EQRLang
The EQR Language is an extensible plugin system. The rules are built around the three requirements of Predicate, Projection, (Consumption/Destination). Operators, Functions and the way consuming and sending data to the final destination are exposed plugins that can be extended. 

EQR supports the current `operators` out-of-the-box. *Note* these are for Predicate lines only. 

| Operator | Returns      |
| :------: | :-----------:|
| `==`     | true / false |
| `!=`     | true / false |
| `<`      | true / false |
| `>`      | true / false |
| `<=`     | true / false |
| `>=`     | true / false |

In Addition to the operators listed above EQR supports Predicate `OR` keyword. Each semi-colon seperated line within a predicate is considered an `AND` statement to the following. You can therefore have something like the following:

```
EMIT.KINESIS[myKinesisStream,  ]({
	JSON(SomeKey.HasTowel) == true OR JSON(SomeKey.Number) == 42;
	},
	{
        	SEND[
               		SENDALL(*);
        	]
	})
```

EQR supports the following `functions` out-of-the-box. *Note* For Projection lines the keyword `AS`is available, this instructs EQR to use the name to right of `AS` for the name of key being sent downstream. If you leave off the `AS` keyword, EQR will by default use the keyname from the data that it is parsing.

| Function | Parameter(s) | Return Value |
| :------: | :----------: | :-----: |
| BASENAME | `(String)`   | Returns the base name of a filepath `(ex. C:\Test.txt -> Test.txt )`|
| CACHE | `(Key, Value)` | Caches a Key, Value pair. Key must be a `string` |
| CONCAT | `(...)` | Returns the concatenated string of all arguments |
| FETCH | `(Key)` | Returns the cached value stored under `Key`, otherwise `null` |
| GETDIR | `(String)` | Returns the directory string of a filepath `(ex. C:\Sub\Folder\File.zip -> C:\Sub\Folder)`|
| GETRAND | `(FLOAT32/FLOAT64/UINT32/UINT64)` | Returns a random number of the type specified |
| GETTIME | `(H/M/S/MS)` | Returns the current time value specified `H = Hour, M = Minute, S = Second, MS = Millisecond`|
| JSON | `(Key)` | Returns the JSON value of the Key specified or `null` if it does not exist. Child values can be obtained by giving the structured fields followed by periods `ex: Parent.Child.Child.Key` |
| SPRINTF | `(String, ...)` | Returns the string of the values provided. Operates exactly like sprintf |
| STR | `(Value)` | Returns the value casted as a `string` |


EQR Supports the current Consumers / Destination Plugins out-of-the-box.


| Name  | Parameters | Notes |
| :---: | :-------:  | :---: |
| CONSUME.KINESIS | `(KinesisStreamName, Region, ConsumerName, RoleToConsume, ConsumerARNvalue, CreateDynamoTable[True/False])` | Uses enhanced fanout, rules are per-shard (cross shard coming-soon) |
| EMIT.KINESIS | `(StreamName, Role[Optional])` | Emits data to Kinesis stream| 
| S3.BATCH | `(Environment, ShardID, S3Bucket, Region, FlushInterval[seconds], BufferSize[bytes])`|Batches data until the FlushInterval or Buffersize is hit|

### Getting Started
PRE-REQUISITE: Must have AWS CLI installed for below commands.

1. Create single S3 bucket to house all EQR plugins, rules, and rulesets
2. Create object folders inside the bucket you just created
```
`<bucket>/…/rulesets`
`<bucket>/…/rules`
`<bucket>/…/plugins`
`<bucket>/…/plugins/dest`
`<bucket>/…/plugins/operators`
```
3. Load plugins, rules, and rulesets to their respective folders
```
aws s3 cp exampleRules/exampleRuleset s3://eqr-getting-started-test/rulesets/exampleRuleset
aws s3 cp exampleRules/exampleConsume.eqr s3://eqr-getting-started-test/rules/exampleConsume.eqr
aws s3 cp exampleRules/exampleBatch.eqr s3://eqr-getting-started-test/rules/exampleBatch.eqr
for u in $(ls plugins/dest/) ; do aws s3 cp plugins/dest/$u s3://eqr-getting-started-test/plugins/dest/$u ; done
for u in $(ls plugins/operators/) ; do aws s3 cp plugins/operators/$u s3://eqr-getting-started-test/plugins/operators/$u ; done
```

4. Update `./config/example.json` to point to the correct S3 buckets and paths based on your setup.

5. Update dockerfile to use correct ruleset name and path to config file.
```
CMD /go/src/app/app exampleRuleset ./config/example.json
```

6. If using SignalFX, uncomment the SFX_TOKEN environment variable in Dockerfile and use your SFX_TOKEN (and enable metrics in main.go)

7. Run `make run-local`. This will build the EQR container and run it locally wherever this command is executed.

### Testing EQR Code Base
1. set following environment variables
```
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_REGION
```
2. By default the tests look for a dynamodb server at localhost:4567 and kinesis server at localhost:4568. Can install kinesalite and dynalite using npm.
```bash
kinesalite --port 4568 --createStreamMs 1 --deleteStreamMs 1 --updateStreamMs 1 --shardLimit 1000 &
dynalite --port 4567 --createTableMs 1 --deleteTableMs 1 --updateTableMs 1 &
```
3. run `make test`

### Contributing to EQR
To contribute to EQR:

1. fork the repository
2. modify the source
3. add tests where it makes sense
4. make sure local tests still pass
5. use clear, logical commit messages
6. send us a pull request describing the changes and stay involved
