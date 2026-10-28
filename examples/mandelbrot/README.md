# Mandelbrot demo

Renders the Mandelbrot set across a goactor cluster. A named coordinator splits the image into
tiles and hands them to workers placed round-robin. If a node dies, the coordinator re-issues its
tiles and replaces its workers. All of that is application code: the framework only reports the
failure.

## Running

Build once, with network access. Nothing later needs it, so do this before the presentation:

```
cd examples/mandelbrot
docker compose build
```

Open `viewer.html` in a browser to watch the render fill in; click the image to toggle full size.
May not be very responsive for large images
Every command below runs from `examples/mandelbrot`.

| # | Demo | Command |
| --- | --- | --- |
| 1 | One node, one worker | `WORKERS=1 docker compose --profile solo up solo` |
| 2 | One node, many workers | `docker compose --profile solo up solo` |
| 3 | Three nodes, same workers | `docker compose up node1 node2 node3` |
| 4 | Three nodes, one killed | as 3, then during the render: `docker compose kill node2` |

Stop a run with Ctrl-C, then `docker compose --profile solo down` before the next one. The output
is `out/mandelbrot.png`.

**Beware**: if you kill node 3 in demo 4 you will lose the render because the coordinator actor resides there.
If you let it restart the render will start from the beginning.

| Variable | Flag | Default |
| --- | --- | --- |
| `WORKERS` | `-workers` | 6 |
| `TILES` (a perfect square) | `-tiles` | 256 |
| `SIZE` | `-size` | 4096 |
| `MAX_ITER` | `-max-iter` | 10000 |
| `SNAPSHOT_INTERVAL` (0 for none) | `-snapshot-interval` | 1s |

`-tile-timeout` (1m) and `-join-timeout` (30s) have no variable. Every node must get the same
values: the first to reach the coordinator's owner wins.

## Reading the log

Only the coordinator's node prints log during execution, one line per event. Actors print as `node.epoch.seq`, so
`worker=2.4.2` lives on node 2.

```
INFO coordinator started actor=3.4.1 awaiting_nodes=3
INFO node ready node=1 ready=1 of=3
INFO render started size=4096 tiles=256 workers=6 max_iter=10000 nodes_ready=3
INFO worker spawned worker=2.4.1 node=2
INFO tile dispatched tile=120 worker=2.4.1
INFO tile completed tile=121 worker=3.4.2 completed=119 of=256
WARN worker lost worker=2.4.2 node=2 requeued_tile=122 err="goactor: no route"
INFO worker replaced worker=1.8.3 node=1
INFO render complete tiles=256 index_sum=32640 expected=32640 duplicates=0 elapsed=18.57s sha256=cf1e…
```

The `render complete` line is the demo's proof. `index_sum` against `expected` shows every tile
counted exactly once, and the same `sha256` on every run shows the same image, wherever it was
computed and whatever was killed.

## How a kill is handled

The render waits for a `Hello` from every configured node, or for `-join-timeout`, because `Spawn`
only places on nodes already alive and nodes do not wait for each other at startup.

Every 250 ms the coordinator pings each busy worker, which otherwise gets no sends to fail. A failed
`Send` means lost: its tile is requeued, and a replacement is spawned on a surviving node. The error
is usually `ErrNoRoute`, because a killed container's gRPC stream breaks before SWIM declares it
dead; `ErrNodeDown` follows.

`-tile-timeout` catches what `Send` cannot report: delivery is at-most-once, and a dead actor on a
live node drops messages silently. A false alarm costs one duplicate tile, dropped by index.

Only in-flight tiles are re-computed, so a kill shows as a few holes that fill in last.

## Recorded numbers

4096², 256 tiles, `MAX_ITER=10000`, snapshots off. Apple M1 Pro (8 cores, 6 performance), OrbStack
2.2.3 VM with 8 CPUs and 8 GB, Go 1.27.0, linux/arm64. Demos 2 and 3 are the mean of three runs.

| Demo | Wall-clock | |
| --- | --- | --- |
| 1. One node, 1 worker | 94.5 s | |
| 2. One node, 6 workers | 17.89 s | 5.3× demo 1 |
| 3. Three nodes, 6 workers | 17.96 s | **0.4 % overhead** against demo 2 |
| 4. As 3, node 2 killed at 6 s, snapshots on | 18.6 s | one run; 2 tiles requeued, same sha256 |

All runs produced the same sha256, `cf1ead1f…`.

Containers are not CPU-capped: demos 2 and 3 run the same workers on the same cores, so the
difference is what placement costs. A single node is always the faster setup on one host. The
number is low partly because the work is coarse: about 14 tiles per second over a Docker bridge, so
computation dominates. It shows that distribution costs little for this workload, not for every
workload.
