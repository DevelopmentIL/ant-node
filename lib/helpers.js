
exports.equalBuffers = function(a, b) {
	if(a.length !== b.length)
		return false;

	for(var i = 0; i < a.length; i++) {
		if(a[i] !== b[i])
			return false;
	}

	return true;
};

exports.compareBuffers = function(a, b) {
	if(a.length !== b.length)
		return (a.length - b.length);

	for(var i = 0; i < a.length; i++) {
		if(a[i] !== b[i])
			return (a[i] - b[i]);
	}

	return 0;
};

exports.compareRInfo = function(a, b) {
	return a.address.localeCompare(b.address) || (a.port - b.port);
};