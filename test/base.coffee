assert = require('assert')
require('should')

kinesis = require('../src')

describe 'kinesis', ()->

  it 'put and get', (done)->

    stream = kinesis.stream('test1')

    # get
    stream.getRecords (err, records)->
      records[0].val.key.should.be.equal('a')
      done()

    # put
    stream.on 'startGetRecords', ()->
      stream.putRecord 'key', {key: 'a'}

  it 'get from each shard', (done)->

    stream = kinesis.stream('test2')

    # shards
    stream.getShards (err, shards)->
      shards.map (shard)->
        shard.getRecords (err, records)->
          if records.length > 0
            records[0].val.key.should.be.equal('b')
            done()
      stream.putRecord 'key', {key: 'b'}

