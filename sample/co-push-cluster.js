var co = require('co');
var Gate = require('co-gate');
var cluster = require('cluster')

var kinesis = require('..');


var numWorkers = 32;
var numIteration = 32;


if (cluster.isMaster){

  for (var i = 0; i < numWorkers; i++){

    var worker = cluster.fork();

    console.log('worker' + i + ' forked');

  }

  cluster.on('death', function(worker){

    console.log('worker died:' + worker.pid);

  });

}else{

  var stream = kinesis.stream('hoge');

  for (var i = 0; i < numIteration; i++){

    co(function *(){

      var gate = new Gate();

      stream.putRecord('key', {date: new Date()}, gate.in());

      var result = yield gate.out();

      console.log(new Date() + ':' + result);

    })();

  }

}


process.on('SIGINT', function() {

    if(cluster.isMaster) {

        console.log('master killed: pid=' + process.pid);

    } else {

        console.log('worker killed: pid=' + process.pid);

    }

    process.exit(0);

});
