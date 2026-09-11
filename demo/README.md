# Demo presets

Environment overlays for the live demonstration (`docs/DEMO.md`). Each is
applied **on top of** `.env` by a root script, using Node's own `--env-file`
twice — later files win — so the same command works in PowerShell, bash and
CI without a shell-specific way of setting variables:

```
node --env-file=.env --env-file=demo/chaos.env packages/producer/dist/index.js
```

| Preset           | Script                            | What it changes                                                                  |
| ---------------- | --------------------------------- | -------------------------------------------------------------------------------- |
| `chaos.env`      | `npm run demo:producer:chaos`     | Mixed traffic: valid orders plus transient markers and poison pills, unbounded   |
| `transient.env`  | `npm run demo:producer:transient` | Ten records, half of them transient markers, no poison                           |
| `poison.env`     | `npm run demo:producer:poison`    | Ten records, a third of them poison pills, no transient markers                  |
| `consumer-b.env` | `npm run demo:consumer:b`         | A second consumer instance on port 3001, same group — triggers a rebalance       |
| `exhaust.env`    | `npm run demo:consumer:exhaust`   | The transient marker fails every tier and is dead-lettered (about six minutes)   |
| `restored.env`   | `npm run demo:consumer:restored`  | The "downstream is back": the marker succeeds first time — for replaying the DLQ |

Nothing here is read by the services on its own; `.env` remains the source of
truth and these only override it for one process.
