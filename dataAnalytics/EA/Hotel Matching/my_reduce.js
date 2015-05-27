function reduce (array) {
	var previous = array[0]
	var newarr = []
	var counter = 0
	array.forEach(function(element, index, array) {
		if (index === 0) {
			previous = element;
			newarr[counter] = element;
			++counter
		}
		else {
			if (element === previous) {}
			else {
				newarr[counter] = element;
				++counter
				previous = element
			}
		}
	})
	return newarr
}

module.exports = reduce