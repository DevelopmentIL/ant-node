var helpers = require('./helpers'),
	util = require('util'),
	bintrees = require('bintrees'),
	
	RBTree = bintrees.RBTree,
	Progress = require('./Progress'),
	messages = require('./messages'),
	helpers = require('./helpers'),
	
	CHUNK_SIZE = 500,
	THROUGHPUT_FACTOR = 2,
	TIMEOUT_POWER = 2,
	INIT_THROUGHPUT = 1,
	MAX_THROUGHPUT = 0xff,
	INIT_TIMEOUT = 800, // 800ms
	PEER_MAX_RETRIES = 5;
	
module.exports = exports = function(node, findQuery, size, options) {
	exports.super_.call(this);
	
	this._node = node;
	this.findQuery = findQuery;
	this.size = size;
	
	this._packetMaxSize = options.packetMaxSize || messages.PACKET_MAX_SIZE;
	this._initTimeout = options.initRequestTimeout || INIT_TIMEOUT;
	this._throughputFactor = options.throughputFactor || THROUGHPUT_FACTOR;
	this._timeoutPower = options.timeoutPower || TIMEOUT_POWER;
	this._initThroughput = options.initThroughput || INIT_THROUGHPUT;
	this._peerMaxRetries = options.peerMaxRetries || PEER_MAX_RETRIES;
	
	this.bandwidth(options.bandwidthLimit || null);
	this._init();
};
util.inherits(exports, Progress);
var proto = exports.prototype;



/*** static vars ***/
/*** static methods ***/
/*** public methods ***/

proto.bandwidth = function(limit) {
	if(limit === undefined) {
		return this._getBandwidth(Date.now(), this._bandwidth);
	}
	
	this._bandwidthLimit = limit || null;
	return this;
};

proto.peerBandwidth = function(rinfo) {
	var peer = this._peers.find(rinfo);
	if(!peer)
		return null;
	
	return this._getBandwidth(Date.now(), peer.bandwidth);
};

proto.addRecord = function(rinfo, recordId) {
	var peer = this._peers.find(rinfo);
	if(peer) {
		if(!this._activePeers.find(peer) && peer.errors <= this._peerMaxRetries) {
			this._activePeers.insert(peer);
	
			this._setRunImmediate();
			return true;
		}
		return false;
	}
	
	peer = {
		address: rinfo.address,
		port: rinfo.port,
		recordId: recordId,
		
		activeChunks: 0,
		lostChunks: 0,
		requestsCount: 0,
		errors: 0,
		
		throughput: this._initThroughput,
		timeout: this._initTimeout,
		bandwidth: {}
	};
	
	this._peers.insert(peer);
	this._activePeers.insert(peer);
	
	this._setRunImmediate();
	return true;
};

/*** protected methods ***/

proto._init = function() {
	var self = this;
	
	this._bandwidth = {};
	
	this._chunkSize = Math.min(this._packetMaxSize - messages.CHUNK_HEADER, this.size);
	this._chunksTotal = Math.ceil(this.size / this._chunkSize);
	this._chunkOffset = 0;
	
	this.stats = {
		total: 0,
		download: 0,
		lost: 0,
		errors: 0
	};
	
	this._activeChunks = new RBTree(compareChunk);
	this._lostChunks = new RBTree(compareChunk);
	
	this._peers = new RBTree(helpers.compareRInfo);
	this._activePeers = new RBTree(compareThroughput);
	
	this.findQuery.on('record', function(record, peer) {
		if(record.size !== self.size) {
			return;
		}
		
		self.addRecord(peer, record.recordId);
	});
	
	this._chunkListenerFn = function() {
		self._chunkListener.apply(self, arguments);
	};
	this._node.on('chunk', this._chunkListenerFn);
};

proto._pause = function() {
	this.findQuery.pause();
};

proto._resume = function() {
	this.findQuery.resume();
};

proto._stop = function() {
	this._node.removeListener('chunk', this._chunkListenerFn);
	this.findQuery.stop();
	
	var self = this;
	this._peers.each(function(peer) {
		self._node.sendEnd(peer, peer.recordId);
	});
};

proto._getPeer = function() {
	if(!this._activePeers.size)
		return null;
	
	var peer = this._activePeers.max();
	
	if(peer.throughput < peer.activeChunks)
		return null;
	
	return peer;
};

proto._getChunksSequence = function(peer) {
	var chunks = [], count = peer.throughput;
	
	if(this._lostChunks.size) {
		var chunk = this._lostChunks.min(), prevChunk;
		
		do {
			chunks.push(chunk);
			
			this._lostChunks.remove(chunk);
			prevChunk = chunk.chunk;
			
			chunk = this._lostChunks.min();
			
		} while(chunk && --count && chunk.chunk !== prevChunk + 1);
		
		return chunks;
	}
	
	if(this._chunkOffset >= this._chunksTotal)
		return chunks;
	
	var endOffset = this._chunkOffset + count;
	
	if(endOffset > this._chunksTotal)
		endOffset = this._chunksTotal;
	
	do {
		chunks.push({
			chunk: this._chunkOffset,
			offset: this._chunkOffset * this._chunkSize
		});
		
	} while(++this._chunkOffset < endOffset);
	
	return chunks;
};

proto._run = function() {
	if(this._runImmediate)
		return;
	
	var peer = this._getPeer();
	if(!peer)
		return;
	
	var chunks = this._getChunksSequence(peer);
	if(!chunks.length) {
		if(!this._activeChunks.size)
			this._complete();
		
		return;
	}
	
	var self = this,						
	request = {
		chunks: chunks,
		activeChunks: chunks.length,
		time: Date.now(),
		
		timer: setTimeout(function() {
			request.timer = null;
			self._requestTimeout(request, peer);
		}, peer.timeout)
	};
	
	chunks.forEach(function(chunk) {
		chunk.peer = peer;
		chunk.request = request;
		
		self._activeChunks.insert(chunk);
	});
	
	this.stats.total += chunks.length;
	peer.requestsCount += chunks.length;
	peer.activeChunks += chunks.length;
	
	this._activePeers.remove(peer);
	this._activePeers.insert(peer);
	
	this._node.sendRequest(peer, peer.recordId, 
			chunks[0].offset, this._chunkSize, chunks.length, 
			function() {
		self._setRunImmediate();
	});
};

proto._chunkListener = function(recordId, offset, buffer, rinfo) {
	var chunk = this._activeChunks.find({offset: offset});
	if(!chunk)
		return;

	var peer = chunk.peer;

	if(recordId !== peer.recordId)
		return;

	if(buffer.length !== this._chunkSize && 
			(chunk.offset + buffer.length) !== this.size)
		return;

	if(helpers.compareRInfo(peer, rinfo) !== 0)
		return;

	this._activeChunks.remove(chunk);
	
	this.stats.download++;
	this._setProgress(this.stats.download / this._chunksTotal);
	
	var now = new Date();
	this._updateBandwidth(now, this._bandwidth, buffer.length);
	this._updateBandwidth(now, peer.bandwidth, buffer.length);

	peer.activeChunks--;
	chunk.request.activeChunks--;

	if(!chunk.request.activeChunks && chunk.request.timer) {
		clearTimeout(chunk.request.timer);
		this._requestSuccess(chunk.request, peer);
	}

	this._activePeers.remove(peer);
	this._activePeers.insert(peer);
	
	this.emit('chunk', offset, buffer, rinfo);
	this._setRunImmediate();
};

proto._complete = function() {
	this.stop();
	
	var self = this;
	setImmediate(function() {
		self.emit('complete');
	});
};

proto._updateBandwidth = function(now, bandwidth, bufSize) {
	if(now - bandwidth.currTime < 1000) {
		bandwidth.curr += bufSize;
	} else {
		bandwidth.lastTime = bandwidth.currTime;
		bandwidth.currTime = now;
		
		bandwidth.last = bandwidth.curr;
		bandwidth.curr = bufSize;
	}
};

proto._getBandwidth = function(now, bandwidth) {
	if(now - bandwidth.lastTime < 2000)
		return bandwidth.last;
	else
		return 0;
};

proto._requestSuccess = function(request, peer) {
	var requestTime = Date.now() - request.time;
	
	peer.errors = 0;
	
	if(!this._bandwidthLimit || this.bandwidth() <= this._bandwidthLimit) {
		var timeoutFactor = Math.pow(this._throughputFactor, this._timeoutPower);
		
		if(requestTime < Math.ceil(peer.timeout / timeoutFactor)) {
			peer.timeout = Math.ceil(peer.timeout / this._throughputFactor);
			peer.throughput = Math.ceil(peer.throughput * this._throughputFactor);
		} else if(peer.throughput <= request.chunks.length) {
			peer.throughput++;
		}
		
		if(peer.throughput > MAX_THROUGHPUT)
			peer.throughput = MAX_THROUGHPUT;
	} else {
		peer.throughput = Math.ceil(peer.throughput / this._throughputFactor);
	}
};

proto._requestTimeout = function(request, peer) {
	var self = this,
	successChunks = request.chunks.length - request.activeChunks;
	
	request.chunks.forEach(function(chunk) {
		if(!self._activeChunks.remove(chunk))
			return;

		self._lostChunks.insert(chunk);
	});

	this.stats.lost += request.activeChunks;
	peer.lostChunks += request.activeChunks;
	peer.activeChunks -= request.activeChunks;
	
	if(successChunks === 0) {
		this.stats.errors++;
		peer.errors++;
		
		peer.timeout = Math.ceil(peer.timeout * this._throughputFactor);
		peer.throughput = Math.ceil(peer.throughput / this._throughputFactor);
	} else {
		peer.errors = 0;
		
		peer.throughput = Math.ceil(successChunks / this._throughputFactor);
	}

	this._activePeers.remove(peer);
	
	if(peer.errors < this._peerMaxRetries)
		this._activePeers.insert(peer);
	else if(peer.errors === this._peerMaxRetries)
		this.findQuery.retryPeer(peer);
	
	this._setRunImmediate();
};


/*** local functions ***/

function compareThroughput(a, b) {
	return ((a.throughput - a.activeChunks) - (b.throughput - b.activeChunks)) 
			|| helpers.compareRInfo(a, b);
}
	
function compareChunk(a, b) {
	return (a.offset - b.offset);
}