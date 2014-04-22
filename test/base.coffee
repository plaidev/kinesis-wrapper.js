assert = require('assert')
require('should')

kinesis = require('../')

describe 'kinesis', ()->

  it 'put and get', (done)->
    stream = kinesis.stream('teststream')
    # get
    stream.getRecords (err, records)->
      records[0].val.key.should.be.equal('a')
      done()

    # put
    setTimeout ()->
      stream.putRecord 'key', {key: 'a'}
    , 4000

  it 'get from each shard', (done)->
    stream = kinesis.stream('teststream')
    # shards
    stream.getShards (err, shards)->
      shards.map (shard)->
        shard.getRecords (err, records)->
          if records.length > 0
            records[0].val.key.should.be.equal('b')
            done()

    # put
    setTimeout ()->
      stream.putRecord 'key', {key: 'b'}
    , 4000

