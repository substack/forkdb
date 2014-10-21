var blob = require('content-addressable-blob-store');
var wrap = require('level-option-wrap');
var fwdb = require('fwdb');
var exchange = require('hash-exchange');

var defined = require('defined');
var has = require('has');
var isarray = require('isarray');
var stringify = require('json-stable-stringify');

var through = require('through2');
var Readable = require('readable-stream').Readable;
var readonly = require('read-only-stream');
var writeonly = require('write-only-stream');

var inherits = require('inherits');
var EventEmitter = require('events').EventEmitter;

var collect = require('./lib/collect.js');
var dropFirst = require('./lib/drop_first.js');

inherits(ForkDB, EventEmitter);
module.exports = ForkDB;

function ForkDB (db, opts) {
    var self = this;
    if (!(this instanceof ForkDB)) return new ForkDB(db, opts);
    if (!opts) opts = {};
    
    this._fwdb = fwdb(db);
    this.db = this._fwdb.db;
    this.store = defined(
        opts.store,
        blob({ dir: defined(opts.dir, './forkdb.blob') })
    );
    this._seen = {};
    this._queue = [];
    
    this._id = opts.id;
    if (this._id === undefined) {
        this._queue.push(function (cb) {
            self._getId(function (err, id) {
                if (err) return cb(err);
                self._id = id;
                cb(null);
            });
        });
    }
    this._queue.push(function (cb) {
        self._getSeq(function (err, seq) {
            if (err) return cb(err);
            self._seq = seq;
            cb(null);
        });
    });
    this._runQueue();
}

ForkDB.prototype._runQueue = function () {
    var self = this;
    if (self._running) return;
    self._running = true;
    (function next () {
        if (self._queue.length === 0) {
            self._running = false;
            return;
        }
        self._queue.shift()(function (err) {
            if (err) self.emit('error', err)
            else next()
        });
    })();
};

ForkDB.prototype._getId = function (cb) {
    var self = this;
    self.db.get('_id', function (err, value) {
        if (err && err.type === 'NotFoundError') {
            value = generateId();
            self.db.put('_id', value, function (err) {
                if (err) return cb(err)
                cb(null, value);
            });
        }
        else if (err) return cb(err)
        else cb(null, value)
    });
};

ForkDB.prototype._getSeq = function (cb) {
    var r = this.db.createReadStream({
        gt: [ 'seq', null ],
        lt: [ 'seq', undefined ],
        reverse: true,
        limit: 1
    });
    r.on('error', cb);
    r.pipe(through.obj(write, end));
    
    function write (row, enc, next) { cb(null, row.key[1]) }
    function end () { cb(null, 0) }
};

ForkDB.prototype._getSeen = function (id, cb) {
    var self = this;
    if (has(self._seen, id)) {
        return process.nextTick(function () {
            cb(null, self._seen[id]);
        });
    }
    self.db.get([ '_seen', id ], function (err, seq) {
        if (err && err.type !== 'NotFoundError') return cb(err);
        var n = self._seen[id] = seq || 0;
        cb(null, n);
    });
};

ForkDB.prototype.replicate = function (opts, cb) {
    var self = this;
    if (typeof opts === 'function') {
        cb = opts;
        opts = {};
    }
    if (!opts) opts = {};
    var mode = defined(opts.mode, 'sync');
    
    var otherId = null;
    var errors = [], exchanged = [];
    var pending = 1, fpending = 2;
    
    var ex = exchange(function (shash) {
        if (/^meta=/.test(shash)) return;
        if (mode === 'pull') {
            if (pending === 0) done();
            return;
        }
        
        var hash = shash.replace(/^[^:]+:/, '');
        pending ++;
        var r = self.store.createReadStream({ key: hash });
        r.on('end', function () {
            self._getSeen(otherId, function (err, seq) {
                if (err) return cb && cb(err)
                seq = Math.max(seq, Number(shash.split(':')[0]));
                self.db.put([ '_seen', otherId ], seq, function (err) {
                    if (err) return cb && cb(err)
                    self._seen[otherId] = seq;
                    if (-- pending === 0) done()
                });
            });
        });
        return r;
    });
    
    ex.on('available', function (hashes) {
        var h = hashes[0];
        if (/^meta=/.test(h)) {
            try { var meta = JSON.parse(h.replace(/^meta=/, '')) }
            catch (err) { return ex.end() }
            if (meta && meta.id !== undefined) {
                pending --;
                otherId = meta.id;
                provideFor(meta.id);
            }
            else if (meta && meta.seq) {
                self.db.put([ '_seen', otherId ], meta.seq, function (err) {
                    if (err) cb && cb(err)
                    else if (--fpending === 0) finish()
                });
            }
            else request(meta, hashes.slice(1))
        }
        else request({}, hashes)
    });
    ex.on('response', function (shash, stream) {
        var hash = shash.replace(/^[^:]+:/, '');
        var opts = { expected: hash }; // TODO: verify hash
        var df = dropFirst(function (err, meta) {
            df.pipe(self.createWriteStream(meta, opts, function (err) {
                if (err) errors.push(err);
                else exchanged.push(hash)
                if (-- pending === 0) done();
            }));
        });
        stream.pipe(df)
    });
    
    if (self._seq === undefined) {
        self._queue.push(function (fn) {
            ex.provide('meta=' + JSON.stringify({
                id: self._id,
                _r: Math.random()
            }));
            fn();
        });
    }
    else ex.provide('meta=' + JSON.stringify({
        id: self._id,
        _r: Math.random()
    }));
    return ex;
    
    function done () {
        ex.provide('meta=' + JSON.stringify({
            seq: self._seq,
            _r: Math.random()
        }));
        if (-- fpending === 0) finish();
    }
    function finish () {
        if (cb) cb(errors.length ? errors : null, exchanged);
    }
    
    function request (meta, hashes) {
        if (mode === 'push') return;
        
        if (!meta) meta = {};
        var p = hashes.length;
        var needed = [];
        hashes.forEach(function (h) {
            self.get(h.replace(/^[^:]+:/,''), function (err) {
                if (err) needed.push(h);
                if (-- p === 0) {
                    pending += needed.length;
                    ex.request(needed);
                }
            });
        });
    }
    
    function provideFor (id) {
        self._getSeen(id, function (err, seq) {
            if (err) cb && cb(err)
            else provideSince(seq);
        });
    }
    
    function provideSince (seq) {
        var hashes = [];
        var r = self.db.createReadStream({
            gte: [ 'seq', defined(seq, null) ],
            lt: [ 'seq', undefined ]
        });
        r.pipe(through.obj(write, flush));
        function write (row, enc, next) {
            hashes.push(row.key[1] + ':' + row.value);
            if (hashes.length >= 25) flush();
            next();
        }
        function flush () {
            if (hashes.length) ex.provide(hashes);
            hashes = [];
        }
    }
};

ForkDB.prototype.createWriteStream = function (meta, opts, cb) {
    var self = this;
    var input = through();
    self._queue.push(function (fn) {
        var w = self._createWriteStream(meta, opts, cb);
        w.on('error', function (err) { fn() });
        w.on('complete', function () { fn(null) });
        input.pipe(w);
    });
    self._runQueue();
    return writeonly(input);
};

ForkDB.prototype._createWriteStream = function (meta, opts, cb) {
    var self = this;
    if (typeof meta === 'function') {
        cb = meta;
        opts = {};
        meta = {};
    }
    if (!meta || typeof meta !== 'object') meta = {};
    if (typeof opts === 'function') {
        cb = opts;
        opts = {};
    }
    if (!opts) opts = {};
    var prebatch = defined(
        opts.prebatch,
        function (rows, key, fn) { fn(null, rows) }
    );
    var w = this.store.createWriteStream();
    w.write(stringify(meta) + '\n');
    if (cb) w.on('error', cb);
    
    w.once('finish', function () {
        var prev = getPrev(meta);
        var doc = { hash: w.key, key: meta.key, prev: prev };
        
        var key = defined(meta.key, 'undefined');
        self._fwdb._create(doc, function (err, rows) {
            if (err) return w.emit('error', err);
            if (prev.length === 0) {
                rows.push({
                    type: 'put',
                    key: [ 'tail', meta.key, w.key ],
                    value: 0
                });
            }
            var skey = [ 'seq', ++ self._seq ];
            rows.push({ type: 'put', key: [ 'meta', w.key ], value: meta });
            rows.push({ type: 'put', key: skey, value: w.key });
            prebatch(rows, w.key, commit);
        });
    });
    return w;
    
    function commit (err, rows) {
        if (err) return w.emit('error', err);
        if (!isarray(rows)) {
            return w.emit('error', new Error(
                'prebatch result is not an array'
            ));
        }
        self.db.batch(rows, function (err) {
            if (err) return w.emit('error', err);
            if (cb) cb(null, w.key);
            w.emit('complete', w.key);
        });
    }
};

ForkDB.prototype.heads = function (key, opts, cb) {
    return this._fwdb.heads(key, opts, cb);
};

ForkDB.prototype.keys = function (opts, cb) {
    return this._fwdb.keys(opts, cb);
};

ForkDB.prototype.tails = function (key, opts, cb) {
    if (typeof opts === 'function') {
        cb = opts;
        opts = {};
    }
    if (!opts) opts = {};
    var r = this._fwdb.db.createReadStream(wrap(opts, {
        gt: function (x) { return [ 'tail', key, null ] },
        lt: function (x) { return [ 'tail', key, undefined ] }
    }));
    var tr = through.obj(function (row, enc, next) {
        this.push({ hash: row.key[2] });
        next();
    });
    r.on('error', function (err) { tr.emit('error', err) });
    if (cb) tr.pipe(collect(cb));
    if (cb) tr.on('error', cb);
    return readonly(r.pipe(tr));
};

ForkDB.prototype.list = function (opts, cb) {
    if (typeof opts === 'function') {
        cb = opts;
        opts = {};
    }
    if (!opts) opts = {};
    var r = this._fwdb.db.createReadStream(wrap(opts, {
        gt: function (x) { return [ 'meta', defined(x, null) ] },
        lt: function (x) { return [ 'meta', defined(x, undefined) ] }
    }));
    var tr = through.obj(function (row, enc, next) {
        this.push({ meta: row.value, hash: row.key[1] });
        next();
    });
    r.on('error', function (err) { tr.emit('error', err) });
    if (cb) tr.pipe(collect(cb));
    if (cb) tr.on('error', cb);
    return readonly(r.pipe(tr));
};

ForkDB.prototype.createReadStream = function (hash) {
    var r = this.store.createReadStream({ key: hash });
    return readonly(r.pipe(dropFirst()));
};

ForkDB.prototype.get = function (hash, cb) {
    this._fwdb.db.get([ 'meta', hash ], function (err, meta) {
        if (err && cb) cb(err)
        else if (cb) cb(null, meta)
    });
};

ForkDB.prototype.links = function (hash, opts, cb) {
    return this._fwdb.links(hash, opts, cb);
};

ForkDB.prototype.history = function (hash) {
    var self = this;
    var r = new Readable({ objectMode: true });
    var next = hash;
    
    r._read = function () {
        if (!next) return r.push(null);
        self.get(next, onget);
    };
    return r;
    
    function onget (err, meta) {
        if (err) return r.emit('error', err)
        var hash = next;
        var prev = getPrev(meta);
        
        if (prev.length === 0) {
            next = null;
            r.push({ hash: hash, meta: meta });
        }
        else if (prev.length === 1) {
            next = hashOf(prev[0]);
            r.push({ hash: hash, meta: meta });
        }
        else {
            next = null;
            r.push({ hash: hash, meta: meta });
            prev.forEach(function (p) {
                r.emit('branch', self.history(hashOf(p)));
            });
        }
    }
};

ForkDB.prototype.future = function (hash) {
    var self = this;
    var r = new Readable({ objectMode: true });
    var next = hash;
    
    r._read = function () {
        if (!next) return r.push(null);
        
        var pending = 2, ref = {};
        self.get(next, function (err, meta) {
            if (err) return r.emit('error', err);
            ref.meta = meta;
            if (-- pending === 0) done();
        });
        
        self.links(next, function (err, crows) {
            if (err) return r.emit('error', err);
            ref.rows = crows;
            if (-- pending === 0) done();
        });
        
        function done () {
            var prev = next;
            if (ref.rows.length === 0) {
                next = null;
                r.push({ hash: prev, meta: ref.meta });
            }
            else if (ref.rows.length === 1) {
                next = hashOf(ref.rows[0]);
                r.push({ hash: prev, meta: ref.meta });
            }
            else {
                next = null;
                r.push({ hash: prev, meta: ref.meta });
                ref.rows.forEach(function (crow) {
                    r.emit('branch', self.future(hashOf(crow)));
                });
            }
        }
    };
    return r;
};

function getPrev (meta) {
    if (!meta) return [];
    if (!has(meta, 'prev')) return [];
    var prev = meta.prev;
    if (!isarray(prev)) prev = [ prev ];
    return prev.map(function (p) {
        if (p && typeof p === 'object' && p.hash) return p.hash;
        return p;
    }).filter(Boolean);
}

function hashOf (p) {
    return p && typeof p === 'object' ? p.hash : p;
}


function generateId () {
    var s = '';
    for (var i = 0; i < 4; i++) {
        s += Math.floor(Math.random() * Math.pow(16,8)).toString(16);
    }
    return s;
}
