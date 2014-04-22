
var kinesis = require('..');

var stream = kinesis.stream('hoge');

stream.putRecord('key', {date: new Date()});
