---
title: Electric Moray: Joyent's Sharded Key/Value Store.
markdown2extras: tables, code-friendly
apisections: Buckets, Objects, Tokens
---

# Electric Moray

This is the reference documentation for Electric Moray, which provides an
abstraction over multiple [Moray][MS] shards so that objects can be
mostly evenly distributed throughout them based on their key.

Electric Moray does this by using pre-defined [key transformations][KT] and
a consistent hash (created and managed using [node-fash][NF]) to map objects
onto the service's backend shards.

Electric Moray provides the same interface as Moray, but with some
exceptions. This document will explain where the API diverges, and
what users may need to do differently. For basic Moray usage, see its
[API documentation](https://github.com/joyent/moray/blob/master/docs/index.md).

Clients connect to Electric Moray with the same [Moray client][MC] that they
would use to talk to a Moray server, just using an address that corresponds to
an Electric Moray instance instead.

# Buckets

When working with buckets, [CreateBucket][CB], [UpdateBucket][UB], and
[DeleteBucket][DB] will be run on every one of the backing shards. If the
command fails on any of the shards, then a `MultiError` will be returned
containing each of the failures. It is up to the consumer to run the command
repeatedly until each of the shards is in a consistent state. This means that
the call must return no error, indicating that it succeeded on all shards, or:

- CreateBucket needs to be repeated until it returns `BucketConflictError` for
  all shards so that the client knows it already exists on every node
- UpdateBucket needs to eventually return no errors (when not using versioned
  buckets) or the same `BucketVersionError` for all shards (when using versioned
  buckets)
- DeleteBucket needs to return `BucketNotFoundError` for all shards

For information on inputs to each of these RPCs, see the section of the Moray
documentation on [Buckets][BK].

## GetBucket

GetBucket behaves the same in Electric Moray as it does in Moray. It gets run on
a random shard with the expectation that the consumer has taken care to ensure
the bucket configuration is the same on each shard.

See the [GetBucket][GB] section of the Moray documentation for more.

## ListBuckets

ListBuckets is currently not an implemented RPC in Electric Moray. Attempts to
use it will hang, due to [MORAY-336][M336].

# Objects

PutObject, GetObject and DeleteObject all behave the same in Electric Moray,
and will be forwarded to one of the backing shards depending on the hashing of
the given key. Note that Electric Moray attempts to transform the key before
hashing it for some buckets:

- Keys for the `manta` bucket, which are Manta paths, get transformed into the
  directory name for the path to ensure that sibling objects end up on the
  same backing shard.
- Keys for the `manta_uploads` bucket, which are of the form
  `<Upload ID>:<Manta path>`, extract the directory name of the path component
  so that multipart upload information ends up on the same shard as the object
  being created.

If the bucket has no transformation defined, then the behaviour is undefined.

See the Moray documentation for [PutObject][PO], [GetObject][GO], and
[DeleteObject][DO].

## FindObjects

FindObjects behaves the same as the [RPCs described above](#objects), but
requires being told which shard it needs to operate on by exactly one of the
`hashkey` or `token` parameters in the `options` object.

See the [FindObjects][FO] section of the Moray documentation for more.

### Additional Options

| Field   | Type   | Description                                                                      |
| ------- | ------ | -------------------------------------------------------------------------------- |
| hashkey | string | a value to hash to choose a backing shard (e.g., a directory name for `'manta'`) |
| token   | string | a token returned from GetTokens referencing a shard                              |

## UpdateObjects

UpdateObjects is not supported by Electric Moray since the filter could require
updating objects on multiple shards.

## DeleteMany

DeleteMany is not supported by Electric Moray since the filter could require
deleting objects on multiple shards.

## ReindexObjects

ReindexObjects is currently not an implemented RPC in Electric Moray. Attempts
to use it will hang, due to [MORAY-336][M336].

## Batch

Batch is supported in Electric Moray, but in a limited fashion to ensure that
only objects living on the same shard are affected. This means that only the
operations `'put'` and `'delete'` can be used in the array of batch requests
given to Electric Moray. For each request in the array, their `'key'` fields
must transform to the same value.

See the [Batch][BA] section of the Moray documentation for more.

### API

    var req_id = mod_uuid.v4();
    var requests = [
        {
            operation: 'put',
            bucket: 'accounts',
            key: 'jsmith',
            options: { etag: null },
            value: {
                "email": "john.smith@example.com",
                "uuid": "e53ec812-a41a-6704-e5db-b18264995957"
            }
        },
        {
            operation: 'delete',
            bucket: 'accounts_pending',
            key: 'jsmith',
            options: { etag: 'E0805A8F' }
        }
    ];

    client.batch(requests, { req_id: req_id }, function (err, res) {
        mod_assert.ifError(err);
        console.log('etags: %j', res.etags);
    });

# Other

## sql

Every shard will be sent the same SQL and their results will be merged into a
single stream. As with regular Moray, this operation should only be used for
debugging.

See the [sql][SQ] section of the Moray documentation for more.

### Additional Options

| Field            | Type    | Description                                                                                                    |
| ---------------- | ------- | -------------------------------------------------------------------------------------------------------------- |
| readOnlyOverride | boolean | whether to override and ignore the safety guards that prevent using this RPC when the ring is marked read-only |

#### Additional Errors

* `ReadOnlyError`

### CLI

    $ sql 'select random();'
    {
      "random": 0.334264599718153
    }
    {
      "random": 0.730934096500278
    }

# Tokens

Electric Moray's API has a concept of "tokens", which are strings that represent
a shard. They are mostly useful for operators and developers looking to get
information from an Electric Moray since consumers should normally be unaware
of the individual backing Moray shards.

## GetTokens

GetTokens returns an array of tokens for each of the Electric Moray's shards.

### API

    client.getTokens(function (err, res) {
        assert.ifError(err);
        console.log('tokens: %j', res.tokens);
    });

### Inputs

| Field   | Type   | Description                                            |
| ------- | ------ | ------------------------------------------------------ |
| options | object | any optional parameters (req\_id)                      |

### CLI

    $ gettokens
    {
      "tokens": [
        "tcp://1.moray.emy-7.joyent.us:2020",
        "tcp://2.moray.emy-7.joyent.us:2020"
      ]
    }


<!-- Links to the Moray documentation -->
[BA]: https://github.com/joyent/moray/blob/master/docs/index.md#batch "Batch Documentation"
[BK]: https://github.com/joyent/moray/blob/master/docs/index.md#buckets "Moray Buckets Documentation"
[CB]: https://github.com/joyent/moray/blob/master/docs/index.md#createbucket "CreateBucket Documentation"
[DB]: https://github.com/joyent/moray/blob/master/docs/index.md#deletebucket "DeleteBucket Documentation"
[GB]: https://github.com/joyent/moray/blob/master/docs/index.md#getbucket "GetBucket Documentation"
[UB]: https://github.com/joyent/moray/blob/master/docs/index.md#updatebucket "UpdateBucket Documentation"
[PO]: https://github.com/joyent/moray/blob/master/docs/index.md#putobject "PutObject Documentation"
[GO]: https://github.com/joyent/moray/blob/master/docs/index.md#getobject "GetObject Documentation"
[DO]: https://github.com/joyent/moray/blob/master/docs/index.md#deleteobject "DeleteObject Documentation"
[FO]: https://github.com/joyent/moray/blob/master/docs/index.md#findobjects "FindObjects Documentation"
[SQ]: https://github.com/joyent/moray/blob/master/docs/index.md#sql "CreateBucket Documentation"

<!-- Links to files inside joyent/electric-moray -->
[KT]: https://github.com/joyent/electric-moray/blob/master/lib/schema/manta.js

<!-- Links to other repositories -->
[MS]: https://github.com/joyent/moray
[MC]: https://github.com/joyent/node-moray
[NF]: https://github.com/joyent/node-fash

<!-- Links to JIRA issues -->
[M336]: https://smartos.org/bugview/MORAY-336 "MORAY-336: Moray hangs on bad RPC method name"
