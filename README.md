# rxjs-amqp-bulk

An experiment in composing an at-least-once AMQP to Elasticsearch indexing pipeline entirely out of RxJS observables, using amqplib and RxJS 4 to scale out data processing and insert into Elasticsearch in bulk.

## Design

- Consumption is wrapped as an observable via `Observable.using`, so the AMQP channel is a disposable bound to the subscription lifetime; each delivery is emitted downstream as `{msg, channel}` (`lib/rx-amqp.js`).
- `channel.prefetch(n)` bounds in-flight unacked deliveries, so backpressure is held at the broker rather than in process memory.
- `bufferWithTimeOrCount(1000ms, 50)` coalesces deliveries into batches bounded by both latency and size.
- Each batch is enriched through chunked, concurrency-limited HTTP fetches (25 ids per call, 10 in flight), then written as a single Elasticsearch `_bulk` request.
- Messages are acked per batch only after the bulk write lands, or after they have been handed to the retry exchange. Bulk actions are `index` keyed on the document id, so redelivery overwrites rather than duplicates.
- Runtime failures republish to a retry exchange with a per-message TTL that doubles on each pass and is capped; the retry queue dead-letters back into the consume path, giving exponential backoff with no scheduler or in-process timer.
- Functional failures, records whose graph cannot be resolved, are parked on a separate error queue and never retried.
- An error in the pipeline itself triggers `retryWhen`, which disposes and rebuilds the channel and consumer; deliveries that were never acked are simply redelivered.
- Online and mass-reindex pipelines share one enrich-and-index stage; the reindex side is fed from a MongoDB cursor through `rx-node` (`lib/online.js`, `lib/reindex.js`).
- Tests run end to end against real RabbitMQ, Elasticsearch and MongoDB, covering ack counts, retry cycling, error-queue parking, broker connection loss mid-flight, throughput (1000 messages under 6s) and mass reindex (`test/es-sync.spec.js`).

## Running the tests

```
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml exec tests sh -c "npm install && npm test"
```

Service images are pinned and the dependency tree is shrinkwrapped, because both had drifted out from under this code: `elasticsearch:latest` now resolves to a major version that rejects the index mapping, `mongo:3` no longer binds all interfaces by default, and two dependencies published releases inside their semver range that Node 4 cannot parse.

## Status

2016 experiment, unmaintained, archived. Kept public as a reference sample of this messaging work. Last verified green in July 2026 against the pinned images above: 9 passing.
