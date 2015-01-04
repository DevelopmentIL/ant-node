var ip = require('ip'),
	BufferOffset = require('buffer-offset'),
	
	HEADER_LENGTH = 1,
	IP4_LENGTH = 4,
	IP6_LENGTH = 16,
	PORT_LENGTH = 2,

	parsers = {};


var Types = 
exports.Types = {
	PING:			0x01,
	HELLO:			0x02,
	
	QUERY:			0x06,
	RECORD:			0x07,
	LOOKUP:			0x08,
	
	REQUEST:		0x0A,
	CHUNK:			0x0B,
	END:			0x0C
},
		
QUERY_MAX_LENGTH =
exports.QUERY_MAX_LENGTH = 0xff,

RECORD_ID_MAX = 
exports.RECORD_ID_MAX = 0xffff,

CHUNK_HEADER = 
exports.CHUNK_HEADER = 6,

PACKET_MAX_SIZE = 
exports.PACKET_MAX_SIZE = 512;

exports.parse = function(buffer) {
	var parser = BufferOffset.convert(buffer);
	
	if(parser.left() < HEADER_LENGTH)
		throw new Error('Message is too short');
	
	var msg = {
		type: parser.getUInt8()
	};
	
	if(parsers.hasOwnProperty(msg.type))
		parsers[msg.type](parser, msg);
	else
		msg.data = parser.get();
	
	return msg;
};


// PING message

exports.ping = function(data) {
	var writer = _writer(Types.PING, 2, data);
	
	writer.appendUInt16BE(data.version);
	
	return writer.slice();
};

parsers[Types.PING] = function(parser, msg) {
	if(parser.left() < 2)
		throw new Error('Message PING is too short');
	
	msg.version = parser.getUInt16BE();
	
	return msg;
};


// HELLO message

exports.hello = function(data) {
	var writer = _writer(Types.HELLO, 2, data);
	
	writer.appendUInt16BE(data.version);
	
	return writer.slice();
};

parsers[Types.HELLO] = function(parser, msg) {
	if(parser.left() < 2)
		throw new Error('Message HELLO is too short');
	
	msg.version = parser.getUInt16BE();
	
	return msg;
};


// QUERY message

exports.query = function(data) {
	if(data.query.length > QUERY_MAX_LENGTH)
		throw new Error('query length exceed QUERY_MAX_LENGTH');
	
	var writer = _writer(Types.QUERY, 3 + data.query.length, data);
	
	writer.appendUInt16BE(data.version);
	writer.appendUInt8(data.query.length);
	writer.append(data.query);
	
	return writer.slice();
};

parsers[Types.QUERY] = function(parser, msg) {
	if(parser.left() < 4)
		throw new Error('Message QUERY is too short');
	
	msg.version = parser.getUInt16BE();
	
	var queryLength = parser.getUInt8();
	if(parser.left() < queryLength)
		throw new Error('Message QUERY is too short');
	
	msg.query = parser.get(queryLength);
	
	return msg;
};


// RECORD message

exports.record = function(data) {
	if(data.query.length > QUERY_MAX_LENGTH)
		throw new Error('query length exceed QUERY_MAX_LENGTH');
	
	var length = 15 + data.query.length
			+ (data.metadata ? data.metadata.length : 0),
			
	writer = _writer(Types.RECORD, length, data);
	
	writer.appendUInt8(data.query.length);
	writer.append(data.query);
	writer.appendUInt32BE(data.size);
	writer.appendUInt16BE(data.recordId);
	writer.appendFill(0, 8);
	
	if(data.metadata)
		writer.append(data.metadata);
	
	return writer.slice();
};

parsers[Types.RECORD] = function(parser, msg) {
	if(parser.left() < 16)
		throw new Error('Message RECORD is too short');
	
	var queryLength = parser.getUInt8();
	if(!queryLength)
		throw new Error('Message RECORD query is empty');
	if(parser.left() < queryLength + 14)
		throw new Error('Message RECORD is too short');
	msg.query = parser.get(queryLength);
	
	msg.size = parser.getUInt32BE();
	msg.recordId = parser.getUInt16BE();
	parser.get(8);
	
	if(parser.left())
		msg.metadata = parser.get();
	
	return msg;
};


// LOOKUP message

exports.lookupListLength = function(query, peer, packetSize) {
	var length = 2;
	
	if(query) {
		if(query.length > QUERY_MAX_LENGTH)
			throw new Error('query length exceed QUERY_MAX_LENGTH');
		
		length += query.length;
	}
	
	var leftSize = packetSize - length,
	peerLength = _getPeerLength(peer);
	
	return Math.floor(leftSize / peerLength);
};

exports.lookup = function(data) {
	var length = 2;
	
	if(data.peers.length) {
		length += data.peers.length * _getPeerLength(data.peers[0]);
	}
	
	if(data.query) {
		if(data.query.length > QUERY_MAX_LENGTH)
			throw new Error('query length exceed QUERY_MAX_LENGTH');
		
		length += data.query.length;
	}
	
	var writer = _writer(Types.LOOKUP, length, data);
	
	if(data.query) {
		writer.appendUInt8(data.query.length);
		writer.append(data.query);
	} else {
		writer.appendUInt8(0);
	}
	
	writer.appendUInt8(data.peers.length);
	_writePeers(writer, data.peers);
	
	return writer.slice();
};

parsers[Types.LOOKUP] = function(parser, msg) {
	if(parser.left() < 2)
		throw new Error('Message LOOKUP is too short');
	
	var queryLength = parser.getUInt8();
	if(parser.left() < queryLength + 1)
		throw new Error('Message QUERY is too short');
	
	if(queryLength)
		msg.query = parser.get(queryLength);
	
	var count = parser.getUInt8();
	_parsePeers(parser, count, msg);
	
	return msg;
};


// REQUEST message

exports.request = function(data) {
	var writer = _writer(Types.REQUEST, 10, data);
	
	writer.appendUInt16BE(data.recordId);
	writer.appendUInt32BE(data.start);
	writer.appendUInt16BE(data.chunkSize);
	writer.appendUInt16BE(data.chunks);
	
	return writer.slice();
};

parsers[Types.REQUEST] = function(parser, msg) {
	if(parser.left() < 10)
		throw new Error('Message REQUEST is too short');
	
	msg.recordId = parser.getUInt16BE();
	msg.start = parser.getUInt32BE();
	msg.chunkSize = parser.getUInt16BE();
	msg.chunks = parser.getUInt16BE();
	
	return msg;
};


// CHUNK message

exports.chunk = function(data) {
	var writer = _writer(Types.CHUNK, CHUNK_HEADER + data.chunk.length, data);
	
	writer.appendUInt16BE(data.recordId);
	writer.appendUInt32BE(data.offset);
	writer.append(data.chunk);
	
	return writer.slice();
};

parsers[Types.CHUNK] = function(parser, msg) {
	if(parser.left() < CHUNK_HEADER)
		throw new Error('Message CHUNK is too short');
	
	msg.recordId = parser.getUInt16BE();
	msg.offset = parser.getUInt32BE();
	msg.chunk = parser.get();
	
	return msg;
};


// END message

exports.end = function(data) {
	var writer = _writer(Types.END, 2, data);
	
	writer.appendUInt16BE(data.recordId);
	
	return writer.slice();
};

parsers[Types.END] = function(parser, msg) {
	if(parser.left() < 2)
		throw new Error('Message END is too short');
	
	msg.recordId = parser.getUInt16BE();
	
	return msg;
};



/*** private helpers ***/

function _writer(type, length) {
	var buffer = new BufferOffset(HEADER_LENGTH + length);
	
	buffer.appendUInt8(type);
	
	return buffer;
}

function _getPeerLength(peer) {
	return (PORT_LENGTH + ip.toBuffer(peer.address).length);
}

function _writePeers (writer, peers) {
	peers.forEach(function(peer) {
		writer.append(ip.toBuffer(peer.address));
		writer.appendUInt16BE(peer.port);
	});
	
	return writer;
}

function _parsePeers(parser, count, msg) {
	msg.peers = [];
	
	if(parser.left() === count * (IP4_LENGTH + PORT_LENGTH)) {
		for(var i = 0; i < count; i++) {
			msg.peers.push({
				address: ip.toString(parser.get(IP4_LENGTH)),
				port: parser.getUInt16BE()
			});
		}
	} else if(parser.left() === count * (IP6_LENGTH + PORT_LENGTH)) {
		for(var i = 0; i < count; i++) {
			msg.peers.push({
				address: ip.toString(parser.get(IP6_LENGTH)),
				port: parser.getUInt16BE()
			});
		}
	} else {
		throw new Error('Peers array msg is invaild');
	}
	
	return msg;
};