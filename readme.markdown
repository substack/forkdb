# forkdb

forking content-addressed append-only historical key/value blob store over
leveldb with multi-master replication

Conflicts are unavoidable, particularly when latency is high. Instead of hiding
that fundamental fact or going into a conflict panic mode that demands an
immediate resolution, forkdb anticipates and welcomes conflicts.

Interfaces built on forkdb should be honest about the underlying data model and
embrace conflicts too.

For a lower-level version of just the link management for multi-master
replication, check out [fwdb](https://npmjs.org/package/fwdb), upon which this
library is based.

[![build status](https://secure.travis-ci.org/substack/forkdb.png)](http://travis-ci.org/substack/forkdb)

# example

Here we'll create a new document with the contents `beep boop` under the key
`"blorp"`.

```
$ echo beep boop | forkdb create blorp
9c0564511643d3bc841d769e27b1f4e669a75695f2a2f6206bca967f298390a0
```

This document is now the singular head of the blorp key:

```
$ forkdb heads blorp
9c0564511643d3bc841d769e27b1f4e669a75695f2a2f6206bca967f298390a0
```

But now, we'll make a new document that links back to the document we just
created and see that the head has updated to the new document's hash:

```
$ echo BEEP BOOP | forkdb create blorp \
  --prev=9c0564511643d3bc841d769e27b1f4e669a75695f2a2f6206bca967f298390a0
f5ff29843ef0658e2a1e14ed31198807ce8302936116545928756844be45fe41
```

```
$ forkdb heads blorp
f5ff29843ef0658e2a1e14ed31198807ce8302936116545928756844be45fe41
```

But suppose that while we were making our `BEEP BOOP` update, somebody else was
working on an edit to the same previous hash, 9c056451. In other words, a
conflict!

```
$ echo BeEp BoOp | forkdb create blorp \
  --prev=9c0564511643d3bc841d769e27b1f4e669a75695f2a2f6206bca967f298390a0
6c0c881fad7adb3fec52b75ab0de8670391ceb8847c8e4c3a2dce9a56244b328
```

This is no problem for forkdb. There are just 2 heads of the `blorp` key now,
which is completely fine:

```
$ forkdb heads blorp
6c0c881fad7adb3fec52b75ab0de8670391ceb8847c8e4c3a2dce9a56244b328
f5ff29843ef0658e2a1e14ed31198807ce8302936116545928756844be45fe41
```

A UI could show both (or more!) versions side by side or perhaps have a
branch where the files diverge.

However, we can also merge these 2 documents back into 1 by creating a new
document that points back and both heads:

```
$ echo BEEPITY BOOPITY | forkdb create blorp \
  --prev=6c0c881fad7adb3fec52b75ab0de8670391ceb8847c8e4c3a2dce9a56244b328 \
  --prev=f5ff29843ef0658e2a1e14ed31198807ce8302936116545928756844be45fe41
058647fc544f70a96d5d083ae7e3c373b441fc3d55b993407254fcce3c732f1e
```

and now we're back to a single head:

```
$ forkdb heads blorp
058647fc544f70a96d5d083ae7e3c373b441fc3d55b993407254fcce3c732f1e
```

However, all of the previous states of the blorp key were saved into the
history, which we can inspect by picking a key (in this case, the new head
e3bd9d14) and traversing back through the branches to end up at the tail:

```
$ forkdb history 058647fc544f70a96d5d083ae7e3c373b441fc3d55b993407254fcce3c732f1e
+- blorp :: 058647fc544f70a96d5d083ae7e3c373b441fc3d55b993407254fcce3c732f1e
 +- blorp :: 6c0c881fad7adb3fec52b75ab0de8670391ceb8847c8e4c3a2dce9a56244b328
 |- blorp :: 9c0564511643d3bc841d769e27b1f4e669a75695f2a2f6206bca967f298390a0
 +- blorp :: f5ff29843ef0658e2a1e14ed31198807ce8302936116545928756844be45fe41
 |- blorp :: 9c0564511643d3bc841d769e27b1f4e669a75695f2a2f6206bca967f298390a0
```

## replication

First, we'll populate two databases, `/tmp/a` and `/tmp/b` with some data:

```
$ echo beep boop | forkdb -d /tmp/a create msg
0673a2977261a9413b8a1abe8389b7c6ef327b319f60f814dece9617d43465c0
$ echo RAWR | forkdb -d /tmp/a create msg \
  --prev=0673a2977261a9413b8a1abe8389b7c6ef327b319f60f814dece9617d43465c0
071f8d4403f88ca431023ec12a277b28bcd68ab41c5043a5bf7e690b23ba7184
$ echo moo | forkdb -d /tmp/b create msg \
  --prev=0673a2977261a9413b8a1abe8389b7c6ef327b319f60f814dece9617d43465c0
e708cc6e5114ac184e0cf81aca203ddd6b02a599d9d85ac756b37b9b19cd4fae
```

Now we can use [dupsh](https://npmjs.org/package/dupsh) to pipe replication
endpoints for `/tmp/a` and `/tmp/b` together:

```
$ dupsh 'forkdb sync -d /tmp/a' 'forkdb sync -d /tmp/b'
```

dupsh is handy here because the `sync` command reads from stdin and writes to
stdout. You can sync two forkdbs over the network with any duplex transport.

For example, with netcat we can create a server on port 5000:

```
$ dupsh 'forkdb sync -d /tmp/a' 'nc -l 5000'
```

and then elsewhere we can connect to port 5000 for replication:

```
$ dupsh 'forkdb sync -d /tmp/b' 'nc localhost 5000'
```

No matter how you get the data to each database, everything is now in sync!

```
$ forkdb -d /tmp/a heads msg
071f8d4403f88ca431023ec12a277b28bcd68ab41c5043a5bf7e690b23ba7184
e708cc6e5114ac184e0cf81aca203ddd6b02a599d9d85ac756b37b9b19cd4fae
$ forkdb -d /tmp/b heads msg
071f8d4403f88ca431023ec12a277b28bcd68ab41c5043a5bf7e690b23ba7184
e708cc6e5114ac184e0cf81aca203ddd6b02a599d9d85ac756b37b9b19cd4fae
```

If we make a merge update on `/tmp/b`:

```
$ echo woop | forkdb -d /tmp/b create msg \
  --prev=071f8d4403f88ca431023ec12a277b28bcd68ab41c5043a5bf7e690b23ba7184 \
  --prev=e708cc6e5114ac184e0cf81aca203ddd6b02a599d9d85ac756b37b9b19cd4fae
7e38e3a49db243c39b86e8b17535745b8967b914b5aeaf442c8fac9f3e6a7b8b
```

and then merge again:

```
$ dupsh 'forkdb sync -d /tmp/a' 'forkdb sync -d /tmp/b'
```

now the data is merged on both databases:

```
$ forkdb -d /tmp/a heads msg
7e38e3a49db243c39b86e8b17535745b8967b914b5aeaf442c8fac9f3e6a7b8b
$ forkdb -d /tmp/b heads msg
7e38e3a49db243c39b86e8b17535745b8967b914b5aeaf442c8fac9f3e6a7b8b
```

Replication woo.

## api example

Create a forkdb instance by passing in a leveldown or levelup handle and a path
to where the blobs should go. Then you can use `createWriteStream(meta)` to
save some data:

``` js
var db = require('level')('/tmp/edit.db');
var fdb = require('forkdb')(db, { dir: '/tmp/edit.blob' });

var meta = JSON.parse(process.argv[2]);

var w = fdb.createWriteStream(meta, function (err, id) {
    if (err) console.error(err)
    else console.log(id)
});
process.stdin.pipe(w);
```

Now give the program some data on `stdin`:

```
$ echo beep boop | node create.js '{"key":"blorp"}'
9c0564511643d3bc841d769e27b1f4e669a75695f2a2f6206bca967f298390a0
```

# data model

The data model is append-only. Each document operates under a key and may
reference zero or more other documents by the hash of their content, which
always point backward in time. That is, to have a link to a document, the
document must first exist because the link is the hash of its content.

Each document can link back to zero, one, or many other documents from any other
key. You can make these links mean semantically whatever you wish, but given how
heads, tails, and forward indexes are generated from these backward links, this
is a helpful semantic model to use:

* `n = 0` - a new document with no history
* `n = 1` - an update to an existing document
* `n >= 2` - merge multiple documents together

For each of these conditions, the heads and tails are updated:

* `n = 0` - a new head and new tail are added
* `n = 1` - a head is removed, a new head is added
* `n >= 2` - `n` heads are removed, one head is added

Each update with a backward link also generates a forward link to enable fast
forward and backward traversals. Forward links, heads, and tails are all
generated purely for performance reasons since these can all be computed, albeit
slowly, from the backward links.

# methods

``` js
var forkdb = require('forkdb')
```

## var fdb = forkdb(db, opts)

Create a new forkdb instance `fdb` from a levelup or leveldown `db`.

Optionally set:

* `opts.id` - uniquely identify the current instance. This id is used to
  negotiate sequences for replication and MUST be unique to correctly identify
  this database.
* `opts.dir` - directory to use for blob storage, default: './forkdb.blob'
* `opts.store` - content-addressable [abstract-blob-store](https://npmjs.org/package/abstract-blob-store) to use instead of
[content-addressable-blob-store](https://npmjs.org/package/content-addressable-blob-store)

To run both the command-line tool and the api over the same data simultaneously,
use [level-party](https://npmjs.org/package/level-party) to create the `db`.

## var w = fdb.createWriteStream(meta, opts={}, cb)

Save the data written to the writable stream `w` into blob storage at
`meta.key`. To link back to previous documents, specify an array of objects with
`key` and `hash` properties as `meta.prev`. For example:

``` js
meta.key = 'blorp';
meta.prev = [
  { key: 'blorp', hash: 'fcbcbe4389433dd9652d279bb9044b8e570d7f033fab18189991354228a43e99' },
  { key: 'blorp', hash: 'c3122c908bf03bb8b36eaf3b46e27437e23827e6a341439974d5d38fb22fbdfc' }
];
```

`cb(err, key)` fires when an error occurs or when all the data has been written
successfully to blob storage and leveldb under the key `key`.

Optionally, you can set an `opts.prebatch(rows, key, fn)` function that gets
runs before `db.batch()` with the hash of the content, `key`. Your prebatch
function should call `fn(err, rows)` with the rows to insert.

## var r = fdb.createReadStream(hash)

Return a readable stream `r` with the blob content at `hash`.

## var r = fdb.heads(key)

Return a readable object stream `r` that outputs an object with `key` and `hash`
properties for every head of `key`.

If `key` is undefined, all heads from all the keys are output.

## var r = fdb.tails(key)

Return a readable object stream `r` that outputs an object with `key` and `hash`
properties for every tail of `key`.

If `key` is undefined, all tails from all the keys are output.

## var r = fdb.list(opts)

Return a readable object stream `r` that outputs each metadata object for every
document in the database.

Constrain the output stream by passing in `opts.gt`, `opts.lt`, or `opts.limit`.

## var r = fdb.keys(opts)

Return a readable object stream `r` that outputs a record for every key in the
database.

Constrain the output stream by passing in `opts.gt`, `opts.lt`, or `opts.limit`.

## fdb.get(hash, cb) 

Get the metadata for `hash` and call `cb(err, meta)` with the result.

## var r = fdb.links(hash)

Return a readable object stream `r` that outputs an object with `key` and `hash`
properties for every forward link of `hash`.

## var r = fdb.history(hash)

Return a readable object stream `r` that traverses backward starting from
`hash`, outputting a metadata object for each document in the history.

When the traversal comes to a branch, `r` ends and emits a `'branch'` event with
a `b` object for each branch. The branch object `b` has the same behavior as `r`
and operates recursively.

## var r = fdb.future(hash)

Return a readable object stream `r` that traverses forward starting from `hash`,
outputting a metadata object for each document in the future history.

When the traversal comes to a branch, `r` ends and emits a `'branch'` event with
a `b` object for each branch. The branch object `b` has the same behavior as `r`
and operates recursively.

## var d = fdb.replicate(opts={}, cb)

Return a duplex stream `d` to replicate with another forkdb.
Pipe the endpoints to each other, duplex stream style:

```
var d = fdb.replicate();
d.pipe(stream).pipe(d);
```

for some full-duplex `stream`, for example from a tcp connection.

Specify the replication strategy with `opts.mode`:

* `opts.mode === 'sync'` - multi-master replication (default strategy)
* `opts.mode === 'push'` - only send updates
* `opts.mode === 'pull'` - only receive updates

Note that if both endpoints try to push or both endpoints try to pull from each
other, nothing will happen.

forkdb saves the last sequence successfully replicated for each remote host to
avoid sending hashes for sequences that remote hosts already know about.

Set `opts.live` to `true` to keep the stream open for continuous streaming
replication as new documents are created.

# usage

```
usage: forkdb COMMAND OPTIONS

  Global options are:

    -d, --dir  directory to use for both db and blob storage
               If not specified, uses $FORKDB_DIR or ./forkdb
 
    --blobdir  directory to use for blob storage

    --dbdir    directory to use for db

forkdb create KEY {--prev=HASH ...}

  Create a new document with content from stdin under KEY.
  Set pointers to previous content with "--prev". To point back at multiple
  documents (a merge), use --prev more than once.
  
forkdb list {--lt=LT, --gt=GT, --limit=LIMIT}

  List all the document metadata in the database.
  Optionally set LT, GT, and LIMIT constraints on the output.

forkdb keys {--lt=LT, --gt=GT, --limit=LIMIT}

  List all the active keys in the database, one key per line.

forkdb read HASH

  Print the contents for HASH to stdout.

forkdb get HASH

  Print the metadata for HASH to stdout as json.

forkdb heads KEY

  Print the hash of every head for KEY on its own line.

forkdb tails KEY

  Print the hash of every tail for KEY on its own line.

forkdb links HASH

  Print newline-delimited json for the `key` and `hash` properties of each
  forward link back to HASH.

forkdb history HASH

  Print an ascii diagram to stdout tracing HASH back in time to its tails.

forkdb future

  Print an ascii diagram to stdout tracing HASH forward in time to its heads.

forkdb sync {OPTIONS} # multi-master replication
forkdb push {OPTIONS} # push updates
forkdb pull {OPTIONS} # pull updates

  Replicate with another forkdb using a replication strategy.
  stdin and stdout are used for incoming and outgoing traffic.
  Optionally:
  
    --live  Keep the connection open for additional updates after the initial
            replication phase.

forkdb help

  Show this message.

```

# install

With [npm](https://npmjs.org),
to get the `forkdb` command do:

```
npm install -g forkdb
```

and to get the library do:

```
npm install forkdb
```

# license

MIT
