#!/usr/bin/env node

var fs = require('fs');
var path = require('path');
var defined = require('defined');

var argv = require('subarg')(process.argv.slice(2), {
    alias: { d: 'dir', h: 'help' },
    boolean: [ 'live' ],
    default: { dir: defined(process.env.FORKDB_DIR, './forkdb') }
});
if (argv.help || argv._[0] === 'help') return showHelp(0);

var through = require('through2');
var isarray = require('isarray');
var stringify = require('json-stable-stringify');
var mkdirp = require('mkdirp');

var dbdir = argv.dbdir;
var blobdir = argv.blobdir;
if (dbdir === undefined) dbdir = path.join(argv.dir, 'db');
if (blobdir === undefined) blobdir = path.join(argv.dir, 'blob');

mkdirp.sync(dbdir);
mkdirp.sync(blobdir);

var db = require('level')(dbdir);
var fdb = require('../')(db, { dir: blobdir });
var showHistory = require('./lib/show_history.js');
var showFuture = require('./lib/show_future.js');

var cmd = argv._[0];

if (cmd === 'list') {
    var s = fdb.list(argv).pipe(ndjson());
    s.pipe(process.stdout);
    s.on('end', function () { db.close() });
}
else if (cmd === 'keys') {
    var s = fdb.keys(argv).pipe(keys());
    s.pipe(process.stdout);
    s.on('end', function () { db.close() });
}
else if (cmd === 'create') {
    var meta = {};
    if (argv.key) {
        meta.key = argv.key;
    }
    else meta.key = argv._[1];
    if (meta.key === undefined) {
        console.error('Missing KEY\nusage: forkdb create KEY');
        process.exit(1);
    }
    
    if (argv.prev) {
        meta.prev = (isarray(argv.prev) ? argv.prev : [ argv.prev ])
            .map(function (p) {
                if (p._) delete p._;
                return p;
            })
        ;
    }
    
    var w = fdb.createWriteStream(meta, function (err, id) {
        if (err) return error(err);
        console.log(id);
        db.close();
    });
    process.stdin.pipe(w);
}
else if (cmd === 'read') {
    if (argv._.length < 2) return showHelp(1);
    var s = fdb.createReadStream(argv._[1]);
    s.pipe(process.stdout);
    s.on('end', function () { db.close() });
}
else if (cmd === 'get') {
    if (argv._.length < 2) return showHelp(1);
    fdb.get(argv._[1], function (err, row) {
        if (err) return error(err)
        console.log(stringify({ space: 2 }));
        db.close();
    });
}
else if (cmd === 'heads') {
    var s = fdb.heads(argv._[1]).pipe(hashes());
    s.pipe(process.stdout);
    s.on('end', function () { db.close() });
}
else if (cmd === 'tails') {
    var s = fdb.tails(argv._[1]).pipe(hashes());
    s.pipe(process.stdout);
    s.on('end', function () { db.close() });
}
else if (cmd === 'links') {
    if (argv._.length < 2) return showHelp(1);
    var s = fdb.links(argv._[1]).pipe(ndjson());
    s.pipe(process.stdout);
    s.on('end', function () { db.close() });
}
else if (cmd === 'history') {
    if (argv._.length < 2) return showHelp(1);
    showHistory(fdb, argv._[1], function () { db.close() });
}
else if (cmd === 'future') {
    if (argv._.length < 2) return showHelp(1);
    showFuture(fdb, argv._[1], function () { db.close() });
}
else if (cmd === 'sync' || cmd === 'pull' || cmd === 'push') {
    var opts = { mode: cmd, live: argv.live };
    var rep = fdb.replicate(opts, function (errors, hashes) {
        if (errors) {
            errors.forEach(function (err) { console.error(err) });
        }
        if (!argv.live) {
            db.close();
            if (errors) process.exit(1)
            process.stdin.end();
        }
    });
    process.stdin.pipe(rep).pipe(process.stdout);
}
else if (cmd === 'concestor') {
    fdb.concestor(argv._.slice(1), function (err, cons) {
        if (err) error(err);
        else console.log(cons.join('\n'));
    });
}
else showHelp(1);

function showHelp (code) {
    var r = fs.createReadStream(path.join(__dirname, 'usage.txt'));
    r.pipe(process.stdout);
    r.on('end', function () {
        if (code) process.exit(code);
        else if (db) db.close()
    });
}

function error (err) {
    console.error(err);
    process.exit(1);
}

function ndjson () {
    return through.obj(function (row, enc, next) {
        this.push(stringify(row) + '\n');
        next();
    });
}

function hashes () {
    return through.obj(function (row, enc, next) {
        this.push(row.hash + '\n');
        next();
    });
}

function keys () {
    return through.obj(function (row, enc, next) {
        this.push(row.key + '\n');
        next();
    });
}
