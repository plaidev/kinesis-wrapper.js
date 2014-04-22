var kinesis = require('..');

var stream = kinesis.stream('hoge');

stream.getRecords(function(err, records){
  console.log(new Date(), records[0].val.date);
});
