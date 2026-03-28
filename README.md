# 996 Coin Explorer Setup Guide

This guide explains how to run the explorer web app (`app.py`) with Gunicorn and how to keep the local SQLite address index up to date with `indexer.py` via cron.

## Overview

The project has two parts:

- `app.py`  
  Flask-based web explorer frontend and RPC-backed block / tx / address views.

- `indexer.py`  
  Local SQLite indexer for:
  - address history
  - rich list
  - supply page support
  - fast address lookups without `addressindex=1`

The index database is shared by both processes.

## Recommended directory layout

```bash
/home/<user>/996coin-explorer/
├── app.py
├── indexer.py
├── .env
├── explorer_index.db
├── requirements.txt
└── .venv/
```

## Python environment

Create a virtual environment and install dependencies:

```bash
cd /home/<user>/996coin-explorer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## `.env` example

Use an absolute path for the index database so `app.py` and `indexer.py` always read the same file.

```env
NNS_RPC_USER=<rpcusername>
NNS_RPC_PASSWORD=<rpcpassword>
NNS_RPC_HOST=127.0.0.1
NNS_RPC_PORT=41683

EXPLORER_INDEX_DB=/home/bpsexplorer/996coin-explorer/explorer_index.db

EXPLORER_RPC_TIMEOUT=20
EXPLORER_REORG_CHECK_DEPTH=20
EXPLORER_REWIND_EXTRA=5
EXPLORER_TIP_CONFIRMATION_BUFFER=1

EXPLORER_STORE_RAW_JSON=0

NNS_PUBKEY_ADDRESS_PREFIX=53
```

## Does the node still need `txindex=1`?

### Short answer

- **For the indexer itself:** usually **not strictly required anymore**
- **For the explorer in general:** still **recommended**

### Why

`indexer.py` now works mainly by:

1. reading blocks with `getblock <hash> 2`
2. indexing outputs directly from block data
3. resolving most prevouts from the local SQLite database

Because of that, the indexer no longer depends on `getrawtransaction` for every transaction.

### But `txindex=1` is still recommended because

- some fallback RPC paths still use `getrawtransaction`
- arbitrary transaction lookup in the web explorer may rely on it
- it makes debugging and edge cases much easier
- older Bitcoin-family forks can behave inconsistently without it

### Recommendation

Use:

```conf
txindex=1
```

If possible, keep it enabled. It is the safest setup.

## First indexing run

Build the SQLite index once before serving address pages to users:

```bash
cd /home/<user>/996coin-explorer
./.venv/bin/python indexer.py
```

If you need a full rebuild:

```bash
cd /home/<user>/996coin-explorer
rm -f explorer_index.db
./.venv/bin/python indexer.py
```

## Running the explorer with Gunicorn

For testing:

```bash
cd /home/<user>/996coin-explorer
./.venv/bin/gunicorn -w 2 -b 127.0.0.1:8080 app:app
```

### Recommended production command

```bash
cd /home/<user>/996coin-explorer
./.venv/bin/gunicorn \
  --workers 2 \
  --bind 127.0.0.1:8080 \
  --timeout 120 \
  --access-logfile - \
  --error-logfile - \
  app:app
```

## Recommended systemd service for Gunicorn

Create:

`/etc/systemd/system/996coin-explorer.service`

```ini
[Unit]
Description=996 Coin Explorer (Gunicorn)
After=network.target

[Service]
User=<user>
Group=<user>
WorkingDirectory=/home/<user>/996coin-explorer
EnvironmentFile=/home/<user>/996coin-explorer/.env
ExecStart=/home/<user>/996coin-explorer/.venv/bin/gunicorn --workers 2 --bind 127.0.0.1:8080 --timeout 120 app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable 996coin-explorer
sudo systemctl start 996coin-explorer
sudo systemctl status 996coin-explorer
```

## Keeping the index up to date with cron

A cron job every 3 minutes is usually enough for this explorer.

### Recommended crontab entry

```cron
*/3 * * * * cd /home/<user>/996coin-explorer && /usr/bin/flock -n /tmp/996_indexer.lock /home/<user>/996coin-explorer/.venv/bin/python indexer.py >> /home/<user>/996coin-explorer/indexer.log 2>&1
```

### Why `flock`?

`flock` prevents multiple indexer runs from overlapping.

Without it, a new cron run could start while the previous run is still writing to SQLite.

## How to edit cron

```bash
crontab -e
```

Paste the entry above and save.

## Recommended nginx reverse proxy

Example:

```nginx
server {
    listen 80;
    server_name explorer.example.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Then add TLS with Certbot if needed.

## Useful maintenance commands

### Check which DB file is used

Because the DB path is critical, always use an absolute path in `.env`:

```bash
grep EXPLORER_INDEX_DB /home/<user>/996coin-explorer/.env
```

### Check latest indexed block

```bash
sqlite3 /home/<user>/996coin-explorer/explorer_index.db "SELECT MAX(height) FROM blocks;"
```

### Check address entries

```bash
sqlite3 /home/<user>/996coin-explorer/explorer_index.db \
"SELECT block_height, txid, role, delta FROM address_txs WHERE address='YOUR_ADDRESS' ORDER BY block_height DESC LIMIT 20;"
```

### Watch the indexer log

```bash
tail -f /home/<user>/996coin-explorer/indexer.log
```

### Watch Gunicorn / service logs

```bash
journalctl -u 996coin-explorer -f
```

## Notes about reorg handling

The indexer now uses:

- a small stable-tip confirmation buffer
- limited rewind on reorg detection

This makes the explorer more stable on PoS chains, at the cost of being slightly behind the freshest tip.

That is usually the better trade-off for public explorer pages.

## Summary

Recommended production setup:

- keep `txindex=1` enabled if possible
- run `app.py` behind Gunicorn
- put Gunicorn behind nginx (or Apache / Caddy)
- run `indexer.py` every 1-3 minutes via cron
- use `flock`
- use an absolute `EXPLORER_INDEX_DB` path in `.env`
