var events = require('events'),
	util = require('util'),
	dgram = require('dgram'),
	stream = require('stream'),
	bintrees = require('bintrees'),
	
	RBTree = bintrees.RBTree,
	FindQuery = require('./FindQuery'),
	Downloader = require('./Downloader'),
	messages = require('./messages'),
	helpers = require('./helpers'),
	
//	MAX_LISTENERS = 100,
	PING_TIMEOUT = 2000,
	QUERY_TIMEOUT = 7000, // 7 seconds
	RECORD_TIMEOUT = 60 * 1000, // 1 minute
	
	EventTypes = {
		PING: 1,
		QUERY: 2
	};


module.exports = exports = function(options) {	
	exports.super_.call(this);
//	this.setMaxListeners(MAX_LISTENERS);
	
	this._options = options || {};
	
	this._socket = this._options.socket || dgram.createSocket('udp4');
	this._packetMaxSize = this._options.packetMaxSize || messages.PACKET_MAX_SIZE;
	this._pingTimeout = this._options.pingTimeout || PING_TIMEOUT;
	this._queryTimeout = this._options.queryTimeout || QUERY_TIMEOUT;
	this._recordTimeout = this._options.recordTimeout || RECORD_TIMEOUT;
	
	this._init();
};
util.inherits(exports, events.EventEmitter);
var proto = exports.prototype;


/*** static vars ***/

exports.VERSION = 0x0000;

exports.QUERY_MAX_LENGTH = messages.QUERY_MAX_LENGTH;


/*** static methods ***/


/*** public methods ***/

proto.ping = function(peer, callback) {
	var buffer = messages.ping({
		version: exports.VERSION
	});
	
	this._socket.send(buffer, 0, buffer.length, peer.port, peer.address);
	
	this._pushEvent({
		type: EventTypes.PING,
		address: peer.address,
		port: peer.port
	}, this._pingTimeout, callback);
	
	return this;
};

proto.find = function(query, options) {
	if(query.length > exports.QUERY_MAX_LENGTH)
		throw new Error('query length exceed QUERY_MAX_LENGTH');
	
	return new FindQuery(this, query, options || this._options);
};

proto.query = function(peer, query, callback) {
	var buffer = messages.query({
		version: exports.VERSION,
		query: query
	});
	
	this._socket.send(buffer, 0, buffer.length, peer.port, peer.address);
	
	this._pushEvent({
		type: EventTypes.QUERY,
		address: peer.address,
		port: peer.port,
		query: query
	}, this._queryTimeout, callback);
	
	return this;
};

proto.sendRecord = function(peer, query, record, callback) {
	record.__recordId = this._genRecordId();
	record.__lastUse = Date.now();
	
	var buffer = messages.record({
		query: query,
		size: record.size,
		recordId: record.__recordId,
		metadata: record.metadata
	});
	
	this._records.insert(record);
	this._maintenanceRecords();
	
	this._socket.send(buffer, 0, buffer.length, peer.port, peer.address, callback);
	return this;
};

proto.sendLookup = function(peer, query, peers, callback) {
	if(!peers.length) {
		var buffer = messages.lookup({
			query: query,
			peers: peers
		});
		
		this._socket.send(buffer, 0, buffer.length, peer.port, peer.address, callback);
		return this;
	}
	
	var maxLength = messages.lookupListLength(query, peer[0], this._packetMaxSize),
	pos = 0,
	
	self = this,
	run = function(err) {
		if(err) {
			if(callback)
				callback.apply(this, arguments);
			
			return;
		}
		
		if(pos >= peers.length) {
			if(callback)
				callback.apply(this, arguments);
			
			return;
		}
		
		var curr = pos;
		pos += maxLength;
		
		var buffer = messages.lookup({
			query: query,
			peers: peers.slice(curr, pos)
		});
		
		self._socket.send(buffer, 0, buffer.length, peer.port, peer.address, run);
	};
	
	run();
	return this;
};

proto.download = function(findQuery, size, options) {
	return new Downloader(this, findQuery, size, options || this._options);
};

proto.sendRequest = function(peer, recordId, start, chunkSize, chunks, callback) {
	var buffer = messages.request({
		recordId: recordId,
		start: start,
		chunkSize: chunkSize,
		chunks: chunks
	});
	
	this._socket.send(buffer, 0, buffer.length, peer.port, peer.address, callback);
	return this;
};

proto.sendStream = function(peer, request, stream, callback) {
	var self = this,
	offset = request.start,
	
	listerners = {
		error: function() {
			if(callback)
				callback.apply(this, arguments);
		},
		readable: function() {
			var chunk = stream.read(request.chunkSize);
			if(chunk !== null)
				return;

			var buffer = messages.chunk({
				recordId: request.record.__recordId,
				offset: offset,
				chunk: chunk
			});
			
			offset += chunk.length;

			self._socket.send(buffer, 0, buffer.length, peer.port, peer.address, function(err) {
				if(err) {
					if(callback) {
						for(var i in listerners)
							stream.removeListener(i, listerners[i]);

						callback.apply(this, arguments);
					}

					return;
				}

				listerners.readable();
			});
		},
		end: function() {
			if(callback)
				callback.apply(this, arguments);
		}
	};
	
	for(var i in listerners)
		stream.on(i, listerners[i]);
	
	listerners.readable();
	return this;
};

proto.sendBuffer = function(peer, request, buf, callback) {
	var pos = 0,
	
	self = this,
	run = function(err) {
		if(err || pos >= buf.length) {
			if(callback)
				callback.apply(this, arguments);
			
			return;
		}
		
		self._maintenanceRecords();
		
		var chunk = buf.slice(pos, pos + request.chunkSize),
		
		buffer = messages.chunk({
			recordId: request.record.__recordId,
			offset: request.start + pos,
			chunk: chunk
		});
		
		pos += chunk.length;

		self._socket.send(buffer, 0, buffer.length, peer.port, peer.address, run);
	};
	
	run();	
	return this;
};

proto.sendEnd = function(peer, recordId, callback) {
	var buffer = messages.end({
		recordId: recordId
	});
	
	this._socket.send(buffer, 0, buffer.length, peer.port, peer.address, callback);
	return this;
};


// socket methods
[
	'address', 'setBroadcast', 'setTTL', 
	'setMulticastTTL', 'setMulticastLoopback',
	'addMembership', 'dropMembership',
	'ref', 'unref'
].forEach(function(method) {
	proto[method] = function() {
		return this._socket[method].apply(this._socket, arguments);
	};
});

proto.bind = function() {
	this._socket.bind.apply(this._socket, arguments);
	
	return this;
};

proto.ref = function() {
	this._ref();
	this._socket.ref.apply(this._socket, arguments);
	
	return this;
};

proto.unref = function() {
	this._socket.unref.apply(this._socket, arguments);
	this._unref();
	
	return this;
};

proto.close = function() {
	this._unref();
	this._socket.close.apply(this._socket, arguments);
	
	return this;
};


/*** protected methods ***/

proto._init = function() {
	this._events = new RBTree(compareEvents);
	
	this._records = new RBTree(compareRecords);
	this._nextRecordId = Math.floor(Math.random() * messages.RECORD_ID_MAX);
	
	this._socketListeners = {
		error: this._socketError.bind(this),
		message: this._socketMessage.bind(this),
		listening: this._socketListening.bind(this),
		close: this._socketClose.bind(this)
	};
	
	for(var i in this._socketListeners) {
		this._socket.on(i, this._socketListeners[i]);
	}
};

proto._ref = function() {
	this._maintenanceRecords();
};

proto._unref = function() {
	if(this._mtrTimer) {
		clearTimeout(this._mtrTimer);
		this._mtrTimer = null;
	}
};

proto._socketError = function(err) {
	this.emit('error', err);
	this.close();
};

proto._socketMessage = function(buffer, rinfo) {
	// TODO comment this
	console.log('Node got new message from ' + 
			rinfo.address + ':' + rinfo.port + 
			'\n' + buffer.toString('hex') + '\n'
	);
	
	try {
		var msg = messages.parse(buffer);
	} catch(err) {
		// TODO comment this
		console.warn(err);
		return;
	}
	
	var res, Types = messages.Types;

	switch(msg.type) {
		case Types.PING:
			res = messages.hello({
				version: exports.VERSION
			});

			this._socket.send(res, 0, res.length, rinfo.port, rinfo.address);
			this.emit('ping', rinfo, msg);
			break;
			
		case Types.HELLO:
			this.emit('hello', rinfo, msg);
			
			var event = this._events.find({
				type: EventTypes.PING,
				address: rinfo.address,
				port: rinfo.port
			});
			if(!event)
				break;
			
			this._shiftEvent(event).call(this, null, rinfo, msg);
			break;

		case Types.QUERY:
			this.emit('query', msg.query, rinfo, msg);
			break;

		case Types.RECORD:
			this.emit('record', msg.query, msg, rinfo);
			
			var event = this._events.find({
				type: EventTypes.QUERY,
				address: rinfo.address,
				port: rinfo.port,
				query: msg.query
			});
			
			if(!event) {
				this.emit('newRecord', msg.query, msg, rinfo);
				break;
			}
			
			this._shiftEvent(event).call(this, null, msg, rinfo);
			break;

		case Types.LOOKUP:
			this.emit('lookup', msg.peers, msg.query, rinfo, msg);
			break;

		case Types.REQUEST:
			msg.record = this._records.find({__recordId: msg.recordId});
			if(!msg.record || msg.start >= msg.record.size)
				break;
			
			msg.end = msg.start + msg.chunks * msg.chunkSize;
			var diff = msg.end - msg.record.size;
			if(diff > 0) {
				if(diff > msg.chunkSize)
					break;
				
				msg.end = msg.record.size;
			}
			
			msg.record.__lastUse = Date.now();
			this.emit('request', msg, rinfo);
			break;

		case Types.CHUNK:
			this.emit('chunk', msg.recordId, msg.offset, msg.chunk, rinfo, msg);
			break;

		case Types.END:
			msg.record = this._records.find({__recordId: msg.recordId});
			if(!msg.record)
				break;
			
			this._records.remove(msg.record);
			this.emit('end', msg.record, rinfo, msg);
			break;

		default:
			this.emit('message', msg, rinfo);
	}
};

proto._socketListening = function() {
	this.emit('listening');
};

proto._socketClose = function() {
	this.emit('close');
};

proto._pushEvent = function(event, timeout, callback) {
	if(this._events.insert(event)) {
		event.fns = [];
	} else {
		event = this._events.find(event);
	}
	
	var self = this,
	fn = [
		callback,
		setTimeout(function() {
			fn[1] = null;
			self._shiftEvent(event).call(self, new Error('Timeout'));
		}, timeout)
	];
	
	event.fns.push(fn);
};

proto._shiftEvent = function(event) {
	var fn = event.fns.shift();
	
	if(!event.fns.length)
		this._events.remove(event);
	
	if(fn[1])
		clearTimeout(fn[1]);
	
	return fn[0];
};

proto._maintenanceRecords = function() {
	if(this._mtrTimer)
		return;
	
	var self = this;
	this._mtrTimer = setTimeout(function() {
		self._mtrTimer = null;
		
		var lastUse = Date.now() - self._recordTimeout,
		recs = [];
		
		self._records.each(function(record) {
			if(record.__lastUse < lastUse)
				return;
			
			recs.push(record);
		});
		
		recs.forEach(function(record) {
			self._records.remove(record);
			self.emit('end', record, null);
		});
	}, self._recordTimeout + 100);
};

proto._genRecordId = function() {
	if(this._records.size >= messages.RECORD_ID_MAX)
		throw new Error('All record ids are taken, please try later');
	
	var id;
	do {
		id = this._nextRecordId++;

		if(this._nextRecordId >= messages.RECORD_ID_MAX)
			this._nextRecordId = 1;
	} while(this._records.find({__recordId: id}));
	
	return id;
};

//proto._validRecordId = function(recordId) {
//	return (Number.isInteger(recordId) 
//			&& recordId > 0 && recordId < messages.RECORD_ID_MAX);
//};


/*** local functions ***/
	
function compareRecords(a, b) {
	return (a.__recordId - b.__recordId);
}
	
function compareEvents(a, b) {
	if(a.type !== b.type)
		return (a.type - b.type);
	
	var diff = helpers.compareRInfo(a, b);
	if(diff)
		return diff;
	
	if(a.type !== EventTypes.QUERY)
		return 0;
	
	return helpers.compareBuffers(a, b);
}