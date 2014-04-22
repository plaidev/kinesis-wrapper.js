var co = require('co');
var Gate = require('co-gate');

var kinesis = require('..');

var stream = kinesis.stream('hoge');

co(function *(){

  var gate = new Gate();

  stream.getRecords(gate.in());

  var records = yield gate.out();
  
  console.log(new Date(), records[0].val.date);

})();