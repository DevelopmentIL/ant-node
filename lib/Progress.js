var events = require('events'),
	util = require('util');
	
module.exports = exports = function() {
	exports.super_.call(this);
	
	this._runImmediate = null;
	this.progress = 0;
};
util.inherits(exports, events.EventEmitter);
var proto = exports.prototype;



/*** static vars ***/
/*** static methods ***/
/*** public methods ***/

proto.isRunning = function() {
	return (this._runImmediate !== 'paused');
};

proto.pause = function() {
	if(!this.isRunning())
		return this;
	
	this._pause();
	
	if(this._runImmediate)
		clearImmediate(this._runImmediate);
	
	this._runImmediate = 'paused';
	return this;
};

proto.resume = function() {
	this._resume();
	
	this._runImmediate = null;
	this._setRunImmediate();
	
	return this;
};

proto.stop = function() {
	this.pause();
	this._stop();
	
	return this;
};

/*** protected methods ***/

proto._setProgress = function(progress) {
	this.progress = progress;
	this.emit('progress', progress);
};

proto._run = function() {};
proto._pause = function() {};
proto._resume = function() {};
proto._stop = function() {};

proto._setRunImmediate = function() {
	if(this._runImmediate)
		return;
	
	var self = this;
	this._runImmediate = setImmediate(function() {
		self._runImmediate = null;
		self._run();
	});
};
