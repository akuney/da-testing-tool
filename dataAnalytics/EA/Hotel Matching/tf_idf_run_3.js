var fs = require('fs')
var natural = require('natural')
var infile =   '/Users/eric.abis/Documents/Dumps/query_result_parsed.csv'
var wordsin =  '/Users/eric.abis/Documents/Dumps/words.csv'
var outfile = '/Users/eric.abis/Documents/Dumps/tf_results/results3.csv'
var async = require('async')
var obj
var totalDocs = 0


obj = JSON.parse(fs.readFileSync(infile))
var words = JSON.parse(fs.readFileSync(wordsin))

var hotel_ids_array = Object.keys(obj).filter(function(elem) {
	if (elem >= 657096 && elem < 867319) return elem
})


//get total count of documents
for (var i in obj) {++totalDocs }

function CreateScore (id) {
    var result = []
    var ary = obj[id]
    for (var i in obj) {
        var score = 0
        async.each(obj[i], function(elem, callback) {       
            //Each time there is a match add the IDF score
            if (ary.indexOf(elem) >= 0) {
                score = score + Math.log(totalDocs/words[elem])
            }
            callback();
            },
            function(err) {}
        );

        if (isNaN(score) === true) {
            //console.log("Nan")
        }
        else {
            score = Math.round(score * 10)/10 
            result.push([i, score])
        }
    }

    newresult = result.filter(function(elem) {
        return elem[1] >= 20
    });

    result = [];

    var finalresult = []

    async.sortBy(newresult, function(x, callback){
        callback(null, x[1]*-1);
    }, function(err,result){
        finalresult = result
    } );


    finalresult.splice(11, finalresult.length - 11)
    finalresult.splice(0, 1)

    fs.appendFileSync(outfile, id.toString() + ','  + finalresult.toString() + '\n')
}

async.eachSeries(hotel_ids_array, function( id, callback) {

  // Perform operation here.
    CreateScore(id)
    callback();

}, function(err){
    // if any of the file processing produced an error, err would equal that error
    if( err ) {
      // One of the iterations produced an error.
      // All processing will now stop.
      console.log(err);
    } else {
      console.log('All ids have been processed successfully');
    }
});
