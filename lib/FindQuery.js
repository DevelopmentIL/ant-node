var helpers = require('./helpers'),
	util = require('util'),
	bintrees = require('bintrees'),
	
	RBTree = bintrees.RBTree,
	Progress = require('./Progress'),
	helpers = require('./helpers'),

	PERIOD_TIME = 1000, // 1 second
	PERIOD_LIMIT = 50,
	QUERY_CACHE_TIME = 10 * 60000, // 10 minutes
			
	PeerStatus = {
		ADDED: 0,
		QUERY: 1,
		RECORD: 2,
		ERROR: 4
	};
	
module.exports = exports = function(node, query, options) {
	exports.super_.call(this);
	
	this.query = query;
	this._node = node;
	
	this._queryCacheTime = options.queryCacheTime || QUERY_CACHE_TIME;
	this._periodTime = options.queryPeriodTime || PERIOD_TIME;
	this._periodLimit = options.queryPeriodLimit || PERIOD_LIMIT;
	
	this._init();
};
util.inherits(exports, Progress);
var proto = exports.prototype;



/*** static vars ***/
/*** static methods ***/
/*** public methods ***/

proto.addPeer = function(rinfo, refPeer) {
	var peer = this._peers.find(rinfo),
	refPeerHash = refPeer ? [refPeer.address, ':', refPeer.port].join('') : null,
	now = Date.now();
	
	if(peer) {
		if(refPeerHash && !peer.refs.hasOwnProperty(refPeerHash)) {
			peer.refs[refPeerHash] = 1;
			peer.refCount++;
			
			if(peer.status === PeerStatus.ADDED) {
				this._leftPeers.remove(peer);
				this._leftPeers.insert(peer);
				return false;
			}
		} else if(peer.status === PeerStatus.ADDED)
			return false;
		
		if(peer.status === PeerStatus.RECORD)
			return false;
		
		if(now - peer.time < this._queryCacheTime)
			return false;
	} else {
		peer = {
			address: rinfo.address,
			port: rinfo.port,
			refs: {},
			refCount: 1 // same priority for everyone new
		};
		
		if(refPeerHash)
			peer.refs[refPeerHash] = 1;
		
		this._peers.insert(peer);
	}
	
	peer.time = now;
	peer.status = PeerStatus.ADDED;
	
	this._leftPeers.insert(peer);
	this.emit('peerAdded', peer);
	
	this._setRunImmediate();
	return true;
};

proto.retryPeer = function(rinfo) {
	var peer = this._peers.find(rinfo);
	if(!peer)
		return false;
	
	if(peer.status !== PeerStatus.RECORD)
		return false;
	
	peer.status = PeerStatus.ERROR;
	
	return this.addPeer(peer);
};

proto.pause = function() {
	if(this._runTimer) {
		clearTimeout(this._runTimer);
		this._runTimer = null;
	}
	
	return exports.super_.prototype.pause.apply(this, arguments);
};

/*** protected methods ***/

proto._init = function() {
	var self = this;
	
	this._runPeriodLimit = this._periodLimit;
	this._runTimer = null;
	
	this._peers = new RBTree(helpers.compareRInfo);
	this._leftPeers = new RBTree(compareFefCount);
	
	this._lookupListener = function(peers, query, refPeer) {
		if(!query || !helpers.equalBuffers(self.query, query))
			return;
		
		peers.forEach(function(peer) {
			self.addPeer(peer, refPeer);
		});
	};
	this._node.on('lookup', this._lookupListener);
};

proto._stop = function() {
	this._node.removeListener('lookup', this._lookupListener);
};

proto._run = function() {
	if(this._runTimer || this._runImmediate)
		return;
	
	var self = this,
	peer = this._leftPeers.max();
	
	if(!peer)
		return;
	
	this._runPeriodLimit--;
	
	this._leftPeers.remove(peer);
	this._setProgress(this._leftPeers.size / this._peers.size);
	
	peer.status = PeerStatus.QUERY;
	
	this._node.query(peer, this.query, function(err, record) {
		if(err) {
			peer.status = PeerStatus.ERROR;
			return;
		}

		peer.status = PeerStatus.RECORD;
		self.emit('record', record, peer);
	});
	
	if(this._runPeriodLimit <= 0) {
		this._runPeriodLimit = self._periodLimit;
		this._setPeriodTimer();
		return;
	}
	
	this._setRunImmediate();
};

proto._setPeriodTimer = function() {
	var self = this;
	
	this._runTimer = setTimeout(function() {
		self._runTimer = null;
		self._run();
	}, this._periodTime);
};


/*** local functions ***/
	
function compareFefCount(a, b) {
	return (a.refCount - b.refCount) || helpers.compareRInfo(a, b);
}