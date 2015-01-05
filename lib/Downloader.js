var helpers = require('./helpers'),
	util = require('util'),
	bintrees = require('bintrees'),
	
	RBTree = bintrees.RBTree,
	Progress = require('./Progress'),
	messages = require('./messages'),
	helpers = require('./helpers'),
	
	CHUNK_SIZE = 500,
	THROUGHPUT_FACTOR = 2,
	PERIOD_FACTOR = 2,
	REQUEST_TIMEOUT = 2000, // ms
	INIT_THROUGHPUT = 1,
	MAX_THROUGHPUT = messages.REQUEST_CHUNKS_MAX,
	INIT_PERIOD = 400, // ms
	LOSS_RATIO = 0.05,
	PEER_MAX_RETRIES = 5,
	BANDWIDTH_PERIOD = 1000, // ms
	KBPS_TO_BPS = 1024;
	
module.exports = exports = function(node, findQuery, size, options) {
	exports.super_.call(this);
	
	this._node = node;
	this.findQuery = findQuery;
	this.size = size;
	
	this._packetMaxSize = options.packetMaxSize || messages.PACKET_MAX_SIZE;
	this._throughputFactor = options.throughputFactor || THROUGHPUT_FACTOR;
	this._periodFactor = options.periodFactor || PERIOD_FACTOR;
	this._requestTimeout = options.requestTimeout || REQUEST_TIMEOUT;
	this._initPeriod = options.initRequestTimeout || INIT_PERIOD;
	this._initThroughput = options.initThroughput || INIT_THROUGHPUT;
	this._lossRatio = options.lossRatio || LOSS_RATIO;
	this._peerMaxRetries = options.peerMaxRetries || PEER_MAX_RETRIES;
	
//	this.bandwidth(options.bandwidthLimit || null);
	this._init();
};
util.inherits(exports, Progress);
var proto = exports.prototype;



/*** static vars ***/
/*** static methods ***/
/*** public methods ***/

proto.addRecord = function(recordId, rinfo) {
	if(this._peers.find(rinfo))
		return false;
	
	this._newPeer(rinfo, recordId);
	
	this._setRunImmediate();
	return true;
};

proto.activePeers = function() {
	return this._peers.size;
};

proto.peers = function() {
	var peers = new Array(this._peers.size),
	it = this._peers.iterator(), item;
	
	for(var i = 0; i < peers.length; i++) {
		item = it.next();
		
		peers[i] = {
			address: item.address,
			port: item.port
		};
	}
	
	return peers;
};

proto.bandwidth = function(limit) {
	if(limit === undefined) {
		return this._getBandwidth(Date.now(), this._bandwidth) / KBPS_TO_BPS;
	}
	
	this._bandwidthLimit = limit
					? limit * KBPS_TO_BPS
					: null;
	return this;
};

proto.peerBandwidth = function(rinfo) {
	var peer = this._peers.find(rinfo);
	if(!peer)
		return null;
	
	return this._getBandwidth(Date.now(), peer.bandwidth) / KBPS_TO_BPS;
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
	
	this._recordListenerBind = self._recordListener.bind(this);
	this.findQuery.on('record', this._recordListenerBind);
	
	this._chunkListenerBind = self._chunkListener.bind(this);
	this._node.on('chunk', this._chunkListenerBind);
};

proto._stop = function() {
	this.findQuery.stop();
	this._node.removeListener('chunk', this._chunkListenerBind);
	
	this._peers.each(this._removePeer.bind(this));
};

proto._pause = function() {
	this.findQuery.pause();
};

proto._resume = function() {
	this.findQuery.resume();
};

proto._recordListener = function(record, peer) {
	if(record.size !== this.size) {
		return;
	}

	this.addRecord(record.recordId, peer);
};

proto._complete = function() {
	if(!this.isRunning())
		return;
	
	this.stop();
	setImmediate(this.emit.bind(this, 'complete'));
};

proto._chunkListener = function(recordId, offset, buffer, rinfo) {
	var chunk = this._activeChunks.find({offset: offset});
	if(!chunk)
		return;
	
	var request = chunk.request,
	peer = request.peer;

	if(recordId !== peer.recordId)
		return;

	if(buffer.length !== this._chunkSize && 
			(chunk.offset + buffer.length) !== this.size)
		return;

	if(helpers.compareRInfo(peer, rinfo) !== 0)
		return;
	
	this.emit('chunk', offset, buffer, rinfo);
	this._activeChunks.remove(chunk);
	
	var now = new Date();
	this._updateBandwidth(now, this._bandwidth, buffer.length);
	this._updateBandwidth(now, peer.bandwidth, buffer.length);
	
	this.stats.download++;
	this._setProgress(this.stats.download / this._chunksTotal);
	
	if(++request.received === request.total)
		this._peerComplete(peer, request);
};

proto._updateBandwidth = function(now, bandwidth, bufSize) {
	if(now - bandwidth.currTime < BANDWIDTH_PERIOD) {
		bandwidth.curr += bufSize;
	} else {
		bandwidth.lastTime = bandwidth.currTime;
		bandwidth.currTime = now;
		
		bandwidth.last = bandwidth.curr;
		bandwidth.curr = bufSize;
	}
};

proto._getBandwidth = function(now, bandwidth) {
	if(now - bandwidth.lastTime < BANDWIDTH_PERIOD * 2)
		return bandwidth.last * (1000 / BANDWIDTH_PERIOD);
	else
		return 0;
};

proto._maxChunkSequence = function() {
	return (this._chunksTotal - this._chunkOffset);
};

proto._maxLostSequence = function() {
	if(!this._lostChunks.size)
		return 0;
		
	var it = this._lostChunks.iterator(), 
	curr = it.next(), start = curr.chunk, 
	pos = start;

	do {
		pos++;
		curr = it.next();

	} while(curr && curr.chunk === pos);

	return pos - start;
};

proto._newPeer = function(rinfo, recordId) {
	var peer = {
		address: rinfo.address,
		port: rinfo.port,
		recordId: recordId,
		
		lostChunks: 0,
		chunksCount: 0,
		errors: 0,
		
		throughput: this._initThroughput,
		activeChunks: 0,
		
		period: this._initPeriod,
		bandwidth: {}
	};
	
	this._peers.insert(peer);
	this._peerRun(peer);
};

proto._removePeer = function(peer) {
	if(peer.timer)
		clearTimeout(peer.timer);
	
	this._node.sendEnd(peer, peer.recordId);
	this._peers.remove(peer);
};

proto._peerRun = function(peer) {
	if(peer.timer)
		return;
	
	var request;
	
	if(peer.request) {
		request = peer.request;
		peer.request = null;
		
		if(request.received < request.total) {
			request.timer = setTimeout(
				this._requestLost.bind(this, request, peer), 
				this._requestTimeout);
		}
		
		if(request.received / request.total <= 1 - this._lossRatio) {
			this._requestSuccess(request, peer);
		} else {
			this._requestFailed(request, peer);
			
			if(peer.errors >= this._peerMaxRetries) {
				this._removePeer(peer);
				this.findQuery.retryPeer(peer);
				return;
			}
		}
	}
	
	request = peer.request = this._getRequest(peer);
	
	if(!request) {
		if(this._activeChunks.size === 0) {
			this._complete();
		} else {
			this._peerRunPeriod(peer);
		}
		
		return;
	}
	
	peer.chunksCount += request.total;
	this.stats.total += request.total;
	
	this._node.sendRequest(peer, peer.recordId, 
			request.start, this._chunkSize, request.total);
	
	this._peerRunPeriod(peer);
};

proto._peerRunPeriod = function(peer) {
	var self = this;
	
	peer.timer = setTimeout(function() {
		peer.timer = null;
		self._peerRun(peer);
	}, peer.period);
};

proto._peerComplete = function(peer, request) {
	if(request.timer)
		clearTimeout(request.timer);
	
	if(!peer.timer || peer.lostChunks || peer.request !== request)
		return;
	
	clearTimeout(peer.timer);
	
	var time = Math.max(Date.now() - request.time, 1), 
	period = Math.ceil(time * this._periodFactor);
	
	if(period < peer.period)
		peer.period = period;
	
	peer.timer = null;
	this._peerRun(peer);
};

proto._getRequest = function(peer) {
	var limit = peer.throughput,
	
	lost = this._maxLostSequence(),
	left = this._maxChunkSequence();
	
//	if(this._bandwidthLimit) {
//		this._getBandwidth(Date.now(), this._bandwidth)
//		 
//		// TODO bandwidth limit
//		limit = XXX;
//	}
	
	var request = {};
	if(lost < left && lost < limit) {
		request = this._requestChunkSequence(Math.min(limit, left));
	} else {
		request = this._requestLostSequence(Math.min(limit, lost));
	}
	
	if(!request.total)
		return null;
	
	request.peer = peer;
	request.received = 0;
	request.start = request.chunks[0].offset;
	request.time = Date.now();
	
	return request;
};

proto._requestChunkSequence = function(total) {
	var chunk, request = {total: total, chunks: []},
	endOffset = this._chunkOffset + total;
	
	do {
		chunk = {
			chunk: this._chunkOffset,
			offset: this._chunkOffset * this._chunkSize,
			request: request
		};
				
		this._activeChunks.insert(chunk);
		request.chunks.push(chunk);
		
	} while(++this._chunkOffset < endOffset);
	
	return request;
};

proto._requestLostSequence = function(total) {
	var chunk, request = {total: total, chunks: []};

	while(total--) {
		chunk = this._lostChunks.min();
		this._lostChunks.remove(chunk);
		
		chunk.request = request;
		
		this._activeChunks.insert(chunk);
		request.chunks.push(chunk);
	}

	return request;
};

proto._requestSuccess = function(request, peer) {
	peer.errors = 0;
	
	if(request.received < request.total) {
		peer.throughput++;
	} else {
		peer.throughput += Math.ceil(peer.throughput * this._throughputFactor);
	}

	if(peer.throughput > MAX_THROUGHPUT)
		peer.throughput = MAX_THROUGHPUT;
};

proto._requestFailed = function(request, peer) {
	if(request.received === 0) {
		peer.errors++;
		this.stats.errors++;
		
		peer.period = Math.ceil(peer.period * this._periodFactor);
	} else {
		if(peer.errors)
			peer.errors--;
	}
			
	peer.throughput = Math.ceil(peer.throughput / this._throughputFactor);
};

proto._requestLost = function(request, peer) {
	var self = this, lost = 0;
	
	request.timer = null;
	
	request.chunks.forEach(function(chunk) {
		if(!self._activeChunks.remove(chunk))
			return;

		self._lostChunks.insert(chunk);
		lost++;
	});
	
	peer.lostChunks += lost;
	peer.activeChunks -= lost;
	this.stats.lost += lost;
};


/*** local functions ***/
	
function compareChunk(a, b) {
	return (a.offset - b.offset);
}