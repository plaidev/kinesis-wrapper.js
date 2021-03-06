var co = require('co');
var Gate = require('co-gate');
var cluster = require('cluster')

var kinesis = require('..');


var numWorkers = 2;
if(process.argv.length > 2)
  numWorkers = process.argv[2];
var numConcurrency = 2;
if(process.argv.length > 3)
  numConcurrency = process.argv[3];
var numIteration = 2;
if(process.argv.length > 4)
  numIteration = process.argv[4];


if (cluster.isMaster){

  for (var i = 0; i < numWorkers; i++){

    var worker = cluster.fork();

    console.log('worker' + i + ' forked');

  }

  cluster.on('death', function(worker){

    console.log('worker died:' + worker.pid);

  });

} else {

  var stream = kinesis.stream('hoge');

  co(function *(){

    var gate = new Gate();

    for (var i = 0; i < numIteration; i++){

      for (var j = 0; j < numConcurrency; j++){

        try{

          stream.putRecord('key', {date: new Date()}, gate.in());

          console.log(new Date());

        }

        catch (e) {

          console.log(e);

        }

      }

      var result = yield gate.out();

    }

    console.log(new Date() + ':' + JSON.stringify(result));

  })();
}


process.on('SIGINT', function() {

    if(cluster.isMaster) {

        console.log('master killed: pid=' + process.pid);

    } else {

        console.log('worker killed: pid=' + process.pid);

    }

    process.exit(0);

});