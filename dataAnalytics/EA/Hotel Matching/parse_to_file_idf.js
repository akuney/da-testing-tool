var fs = require('fs')
var split = require('split')
var through = require('through')
var obj = {}
var words = {}
var reduce = require('/Users/eric.abis/Documents/Node JS/Hotel Mappings/my_reduce.js')
var street = require('/Users/eric.abis/Documents/Node JS/Hotel Mappings/street.js')

var counter = 0

var file =     '/Users/eric.abis/Documents/Dumps/query_result.csv'
var outfile =  '/Users/eric.abis/Documents/Dumps/query_result_parsed.csv'
var outwords = '/Users/eric.abis/Documents/Dumps/words.csv'

var readstream = fs.createReadStream(file)
var wstream = fs.createWriteStream(outfile)


function writefiles() {
	//output results to JSON
	fs.writeFile(outfile, JSON.stringify(obj), function (err) {
		if (err) throw err;
		console.log('It\'s saved!');
	})

	//output words to JSON
	fs.writeFile(outwords, JSON.stringify(words), function (err) {
		if (err) throw err;
		console.log('Words saved!');
	})
}

var tr = through(

	function write(data) {


		var textarry = data.split(",")

		//Clean up name column	
		var arry2 = textarry[1].split(" ").sort()
		var arry3 = arry2.map(function(elem) {
			return elem.replace("\"","" )
		})
		var arry4 = arry3.filter(function(elem) {
			return elem != ''
		})

		var arraystreet = street(arry4)
		var arrayreduced = reduce(arraystreet)

		arrayreduced.forEach(function(elem) {
			words[elem] = words[elem] + 1 || 1
		})

		++counter
		//Create Final Object
		obj[textarry[0]] = arrayreduced

		if (counter%1000 === 0) console.log(counter.toString())

		this.queue(data)

	}
	,

	function end () {
		this.queue( 
			writefiles()
		)
	}
)

readstream.pipe(split()).pipe(tr)

