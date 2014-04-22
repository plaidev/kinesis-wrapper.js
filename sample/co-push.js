var co = require('co');
var Gate = require('co-gate');

var kinesis = require('..');

var stream = kinesis.stream('hoge');

co(function *(){

  var gate = new Gate();

  stream.putRecord('key', {date: new Date()}, gate.in());

  var result = yield gate.out();

  console.log(result);

})();
