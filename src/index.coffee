
# modules
async = require('async')
AWS = require('aws-sdk')
EventEmitter = require('events').EventEmitter
AWS.config.update({region: 'us-east-1'})


# Kinesis
class KinesisWrapper

  constructor: (options)->

    @kinesis = new AWS.Kinesis()

  stream: (stream_name, options={})->

    return new KinesisStream(@kinesis, stream_name, options)


# Stream Class
# apis: getShards, getRecords, putRecord
class KinesisStream extends EventEmitter

  constructor: (kinesis, stream_name, options={})->

    @options = options

    @kinesis = kinesis

    @stream_name = stream_name

    @shards = false

    @setup()

  setup: ()->

    # call aws-sdk api
    @kinesis.describeStream {
        StreamName: @stream_name
      }, (err, data)=>

        throw err if err

        console.log 'get stream info...'

        async.map data.StreamDescription.Shards, (shard, cb)=>
          @kinesis.getShardIterator {
              ShardId: shard.ShardId
              ShardIteratorType: @options.ShardIteratorType || 'LATEST'
              StreamName: data.StreamDescription.StreamName
            }, (err, data)=>
              cb(err, data.ShardIterator)
        , (err, shardIterators)=>

          console.log 'get shards...'

          @shards = shardIterators.map (i)=>
            return new KinesisShard(@kinesis, i)

          @emit('getShards', null, @shards)

  # get shards
  getShards: (cb)->

    if @shards
      return cb(null, @shards)

    listener = (err, shards)=>

      cb(null, shards)

      @removeListener('getShards', listener)

    @on 'getShards', listener

  # get records from all shards
  getRecords: (cb)->

    listener = (err, shards)=>

      shards.map (shard)=>
        shard.setup()
        shard.on 'getRecords', cb

      @emit 'startGetRecords', shards

    return listener(@shards) if @shards

    @on 'getShards', listener

  # put a record
  putRecord: (key, data, cb)->
    # encoding
    data = new Buffer(JSON.stringify(data)).toString('base64')

    # call aws-sdk api
    @kinesis.putRecord {
      Data: data
      PartitionKey: key
      StreamName: @stream_name
    }, (err, data)->
      cb(err, data) if cb


# Shard Class
# apis: getRecords
class KinesisShard extends EventEmitter
  constructor: (kinesis, iterator)->

    @kinesis = kinesis

    @iterator = iterator

    @initialized = false

  # setup
  setup: ()->

    return if @initialized

    @initialized = true

    # call aws-sdk api
    _getRecords = ()=>

      @kinesis.getRecords {
          ShardIterator: @iterator
        }, (err, data)=>

          throw err if err

          @iterator = data.NextShardIterator

          records = data.Records || []

          return setTimeout(_getRecords, 1000) if records.length == 0

          records = records.map (record)->

            val = JSON.parse(new Buffer(record.Data, 'base64').toString())

            return {key: record.PartitionKey, number: record.SequenceNumber, val: val}

          @emit 'getRecords', null, records

          _getRecords()

    # start
    _getRecords()

  # get records from a shard
  getRecords: (cb)->

    @on 'getRecords', cb

    @setup()


module.exports = new KinesisWrapper({})
