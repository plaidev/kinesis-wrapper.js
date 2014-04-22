assert = require('assert')
require('should')

kinesis = require('../')

describe 'kinesis', ()->
  it 'put and get', (done)->
    stream = kinesis.stream('teststream')

    # get
    stream.getRecords (err, records)->
      console.log err, records
      records[0].val.key.should.be.equal('a')
      done()

    # put
    setTimeout ()->
      stream.putRecord 'key', {key: 'a'}
    , 4000
