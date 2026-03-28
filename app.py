import math
import os
import sqlite3
import requests
from flask import Flask, request, redirect, url_for, render_template_string, jsonify
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# --- 996-Coin RPC configuration ---
RPC_USER = os.environ.get("NNS_RPC_USER", "test")
RPC_PASSWORD = os.environ.get("NNS_RPC_PASSWORD", "test")
RPC_HOST = os.environ.get("NNS_RPC_HOST", "127.0.0.1")
RPC_PORT = os.environ.get("NNS_RPC_PORT", "41683")
EXPLORER_INDEX_DB = os.environ.get("EXPLORER_INDEX_DB", "explorer_index.db")


def rpc_request(method, params=None):
    """
    Send a JSON-RPC request to the coin daemon and return the 'result' field.
    Raises RuntimeError if the node reports an error.
    """
    url = f"http://{RPC_HOST}:{RPC_PORT}"
    payload = {
        "jsonrpc": "1.0",
        "id": "slm-rpc-explorer",
        "method": method,
        "params": params or [],
    }
    resp = requests.post(url, json=payload, auth=(RPC_USER, RPC_PASSWORD), timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(str(data["error"]))
    return data["result"]


def peer_addnode_target(peer):
    """
    Return a practical addnode target string for a peer.
    Prefer the explicit `addr` field from getpeerinfo.
    """
    addr = (peer.get("addr") or "").strip()
    if addr:
        return addr

    addrbind = (peer.get("addrbind") or "").strip()
    if addrbind:
        return addrbind

    addrlocal = (peer.get("addrlocal") or "").strip()
    if addrlocal:
        return addrlocal

    return ""


def format_local_time(ts):
    """
    Convert a Unix timestamp to server-local display time.
    """
    try:
        if ts is None:
            return "unknown"
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def tx_brief(txid):
    """
    Short display form for long hashes/txids while keeping the full value as tooltip.
    """
    if not isinstance(txid, str):
        return ""
    if len(txid) <= 24:
        return txid
    return f"{txid[:16]}…{txid[-8:]}"


# --- Helper: best-effort address-like detection ---
def looks_like_address(value):
    """
    Best-effort check for a wallet address-like search term.
    We keep this deliberately permissive because old Bitcoin-family forks may use
    different Base58 prefixes and lengths.
    """
    if not isinstance(value, str):
        return False

    q = value.strip()
    if not q:
        return False

    # Exclude obvious block hashes / txids and pure heights.
    if q.isdigit():
        return False
    if len(q) == 64:
        try:
            int(q, 16)
            return False
        except Exception:
            pass

    # Very permissive Base58-like heuristic.
    allowed = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    if not (26 <= len(q) <= 64):
        return False
    return all(ch in allowed for ch in q)


def looks_like_txid(value):
    """
    Strict txid / blockhash validator: exactly 64 hex characters.
    """
    if not isinstance(value, str):
        return False
    q = value.strip()
    if len(q) != 64:
        return False
    try:
        int(q, 16)
        return True
    except Exception:
        return False


def public_error_message(exc, fallback="Request failed."):
    """
    Return a user-facing error message without exposing raw internal/RPC details.
    During debug runs, keep the original exception text visible for development.
    """
    if app.debug:
        return str(exc)
    return fallback


BASE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>996-Coin RPC Explorer</title>
  <style>
    :root {
      --slm-bg-top: #255479;
      --slm-bg-bottom: #b9dbcb;
      --slm-card-bg: rgba(8, 25, 40, 0.86);
      --slm-border: rgba(255, 255, 255, 0.08);
      --slm-text-main: #f5f7fa;
      --slm-text-muted: #cfd8dc;
      --slm-accent: #f4d247;   /* 996-Coin yellow */
      --slm-accent-soft: #ffe58a;
      --slm-danger: #ff8a80;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      height: 100%;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI",
                   sans-serif;
      background: radial-gradient(circle at 0% 0%, #3a5479 0, var(--slm-bg-top) 30%),
                  linear-gradient(180deg, var(--slm-bg-top), var(--slm-bg-bottom));
      color: var(--slm-text-main);
      display: flex;
      justify-content: center;
      align-items: flex-start;
    }

    .shell {
      width: min(100%, 1500px);
      max-width: 1500px;
      padding: 2.5rem 1.25rem 3rem;
    }

    .card {
      background: var(--slm-card-bg);
      border-radius: 20px;
      box-shadow: 0 24px 60px rgba(0, 0, 0, 0.65);
      padding: 1.75rem 2rem 2rem;
      border: 1px solid var(--slm-border);
      backdrop-filter: blur(18px);
    }

    a {
      color: var(--slm-accent-soft);
      text-decoration: none;
    }

    a:hover {
      color: var(--slm-accent);
    }

    h1, h2, h3 {
      margin-top: 0;
      letter-spacing: 0.02em;
    }

    h1 a {
      color: var(--slm-text-main);
    }

    header {
      margin-bottom: 1.5rem;
      border-bottom: 1px solid var(--slm-border);
      padding-bottom: 0.9rem;
    }

    nav.meta {
      margin-top: 0.35rem;
      color: var(--slm-text-muted);
      font-size: 0.9rem;
    }

    pre {
      background: rgba(0, 0, 0, 0.45);
      border-radius: 10px;
      padding: 0.75rem 0.9rem;
      overflow-x: auto;
      border: 1px solid rgba(255, 255, 255, 0.06);
      font-size: 0.85rem;
    }

    table {
      border-collapse: collapse;
      width: 100%;
      margin-top: 0.75rem;
      table-layout: fixed;
      background: rgba(0, 0, 0, 0.2);
      border-radius: 12px;
      overflow: hidden;
      border: 1px solid rgba(255, 255, 255, 0.05);
    }

    th, td {
      border-bottom: 1px solid rgba(255, 255, 255, 0.03);
      padding: 0.45rem 0.55rem;
      font-size: 0.9rem;
      vertical-align: top;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    
    .index-cell {
      width: 3.25rem;
      white-space: nowrap;
    }

    .small-cell {
      width: 1%;
      white-space: nowrap;
    }

    .hash-link {
      display: inline-block;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      line-height: 1.3;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    
    .addr-cell {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.92rem;
      line-height: 1.3;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .hash-cell {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.9rem;
      line-height: 1.3;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .amount-cell {
      white-space: nowrap;
    }

    th {
      background: rgba(255, 255, 255, 0.04);
      text-align: left;
      font-weight: 600;
      color: var(--slm-text-muted);
    }

    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.85rem;
      white-space: nowrap;
    }

    tr:nth-child(even) td {
      background: rgba(255, 255, 255, 0.02);
    }

    .error {
      color: var(--slm-danger);
      margin-bottom: 1rem;
      padding: 0.5rem 0.75rem;
      border-radius: 8px;
      background: rgba(183, 28, 28, 0.18);
      border: 1px solid rgba(255, 138, 128, 0.4);
      font-size: 0.9rem;
    }

    .meta {
      color: var(--slm-text-muted);
      font-size: 0.9rem;
    }

    form {
      margin: 0.75rem 0 0.1rem;
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      align-items: center;
      font-size: 0.9rem;
    }

    label {
      margin-right: 0.35rem;
    }

    input[type="text"],
    input[type="number"] {
      padding: 0.3rem 0.55rem;
      min-width: 220px;
      border-radius: 999px;
      border: 1px solid rgba(255, 255, 255, 0.16);
      background: rgba(10, 33, 52, 0.9);
      color: var(--slm-text-main);
      outline: none;
      font-size: 0.9rem;
    }

    input[type="text"]:focus,
    input[type="number"]:focus {
      border-color: var(--slm-accent);
      box-shadow: 0 0 0 1px rgba(244, 210, 71, 0.35);
    }

    input[type="submit"] {
      padding: 0.35rem 0.9rem;
      border-radius: 999px;
      border: none;
      background: linear-gradient(135deg, var(--slm-accent), var(--slm-accent-soft));
      color: #2b2b2b;
      font-weight: 600;
      cursor: pointer;
      font-size: 0.9rem;
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.35);
    }

    input[type="submit"]:hover {
      filter: brightness(1.05);
    }

    ul {
      padding-left: 1.2rem;
    }

    @media (max-width: 720px) {
      .card {
        padding: 1.25rem 1.3rem 1.5rem;
      }
      form {
        align-items: stretch;
      }
      input[type="text"],
      input[type="number"] {
        flex: 1 1 100%;
      }
      table {
        display: block;
        overflow-x: auto;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="card">
      <header>
        <h1><a href="{{ url_for('index') }}">996-Coin RPC Explorer</a></h1>
        <nav class="meta">
          Chain tip: {{ tip_height if tip_height is not none else "unknown" }}
          &nbsp;|&nbsp;
          <a href="{{ url_for('view_peers') }}">Peers</a>
          &nbsp;|&nbsp;
          <a href="{{ url_for('view_stats') }}">Supply &amp; Richlist</a>
        </nav>

        <form action="{{ url_for('search') }}" method="get">
          <label>Search (block height, block hash, txid or address):</label>
          <input type="text" name="q" placeholder="e.g. block height, block hash, txid or address" required>
          <input type="submit" value="Go">
        </form>
      </header>

      {% if error %}
        <div class="error">Error: {{ error }}</div>
      {% endif %}

      {{ content|safe }}
    </div>
  </div>
</body>
</html>
"""


def extract_vout_addresses(vout):
    """
    Best-effort extraction of destination/source addresses from a decoded vout.
    """
    spk = vout.get("scriptPubKey") or {}

    addresses = spk.get("addresses")
    if isinstance(addresses, list) and addresses:
        return addresses

    address = spk.get("address")
    if isinstance(address, str) and address.strip():
        return [address.strip()]

    asm = spk.get("asm")
    if spk.get("type") == "pubkey" and isinstance(asm, str) and asm.strip():
        return [f"pubkey:{asm.split()[0]}"]

    return []


def summarize_vout(vout):
    """
    Convert a decoded vout into a compact explorer-friendly summary.
    """
    spk = vout.get("scriptPubKey") or {}
    addresses = extract_vout_addresses(vout)
    return {
        "n": vout.get("n"),
        "value": vout.get("value"),
        "type": spk.get("type", "unknown"),
        "addresses": addresses,
        "address_display": ", ".join(addresses) if addresses else "unknown",
    }


def resolve_input_details(vin):
    """
    Resolve the previous output referenced by a vin entry so the explorer can
    show the apparent source address and input amount.
    """
    if "coinbase" in vin:
        return {
            "kind": "coinbase",
            "txid": None,
            "vout": None,
            "value": None,
            "type": "coinbase",
            "addresses": ["coinbase"],
            "address_display": "coinbase",
            "error": None,
        }

    prev_txid = vin.get("txid")
    prev_vout_index = vin.get("vout")

    if not prev_txid or prev_vout_index is None:
        return {
            "kind": "unknown",
            "txid": prev_txid,
            "vout": prev_vout_index,
            "value": None,
            "type": "unknown",
            "addresses": [],
            "address_display": "unknown",
            "error": "missing prevout reference",
        }

    try:
        prev_tx = rpc_request("getrawtransaction", [prev_txid, 1])
        prev_outputs = prev_tx.get("vout") or []
        prev_vout = None
        for candidate in prev_outputs:
            if candidate.get("n") == prev_vout_index:
                prev_vout = candidate
                break

        if prev_vout is None:
            return {
                "kind": "prevout",
                "txid": prev_txid,
                "vout": prev_vout_index,
                "value": None,
                "type": "unknown",
                "addresses": [],
                "address_display": "unknown",
                "error": "referenced output not found",
            }

        summary = summarize_vout(prev_vout)
        summary.update({
            "kind": "prevout",
            "txid": prev_txid,
            "vout": prev_vout_index,
            "error": None,
        })
        return summary
    except Exception as exc:
        return {
            "kind": "prevout",
            "txid": prev_txid,
            "vout": prev_vout_index,
            "value": None,
            "type": "unknown",
            "addresses": [],
            "address_display": "unknown",
            "error": str(exc),
        }


def enrich_transaction(tx):
    """
    Add explorer-friendly input/output summaries to a decoded transaction.
    """
    if not isinstance(tx, dict):
        return tx

    tx = dict(tx)

    raw_vin = tx.get("vin") or []
    raw_vout = tx.get("vout") or []

    input_rows = []
    output_rows = []
    input_total = 0.0
    output_total = 0.0
    input_total_known = True

    for idx, vin in enumerate(raw_vin):
        details = resolve_input_details(vin)
        value = details.get("value")
        if isinstance(value, (int, float)):
            input_total += float(value)
        else:
            input_total_known = False

        row = {
            "index": idx,
            "txid": details.get("txid"),
            "vout": details.get("vout"),
            "value": value,
            "type": details.get("type", "unknown"),
            "addresses": details.get("addresses", []),
            "address_display": details.get("address_display", "unknown"),
            "error": details.get("error"),
            "is_coinbase": details.get("kind") == "coinbase",
        }
        input_rows.append(row)

    for vout in raw_vout:
        summary = summarize_vout(vout)
        summary = enrich_vout_with_links(summary)
        value = summary.get("value")
        if isinstance(value, (int, float)):
            output_total += float(value)
        output_rows.append(summary)

    tx["explorer_inputs"] = input_rows
    tx["explorer_outputs"] = output_rows
    tx["explorer_type"] = classify_transaction(tx)
    tx["explorer_input_total"] = round(input_total, 8) if input_total_known else None
    tx["explorer_output_total"] = round(output_total, 8)

    if input_total_known:
        delta = round(output_total - input_total, 8)
        if tx["explorer_type"] == "coinstake":
            tx["explorer_reward"] = delta
            tx["explorer_fee"] = None
        else:
            tx["explorer_reward"] = None
            tx["explorer_fee"] = round(input_total - output_total, 8)
    else:
        tx["explorer_reward"] = None
        tx["explorer_fee"] = None

    tx["explorer_addresses"] = extract_tx_addresses(tx)

    return tx


def enrich_vout_with_links(vout):
    """
    Add a best-effort explorer link target for an output address.
    """
    vout = dict(vout)
    addresses = vout.get("addresses") or []
    vout["primary_address"] = addresses[0] if addresses else None
    return vout


def extract_tx_addresses(tx):
    """
    Collect all unique addresses that appear in the resolved inputs/outputs of a tx.
    """
    seen = set()
    ordered = []

    for row in (tx.get("explorer_inputs") or []):
        for addr in row.get("addresses") or []:
            if addr and addr not in seen and not str(addr).startswith("pubkey:") and addr != "coinbase":
                seen.add(addr)
                ordered.append(addr)

    for row in (tx.get("explorer_outputs") or []):
        for addr in row.get("addresses") or []:
            if addr and addr not in seen and not str(addr).startswith("pubkey:"):
                seen.add(addr)
                ordered.append(addr)

    return ordered


def is_probable_coinstake(tx):
    """
    Heuristic for classic PoS coinstake transactions:
    - not coinbase
    - has at least one input
    - first output is an empty/nonstandard zero-value marker
    """
    if not isinstance(tx, dict):
        return False

    vin = tx.get("vin") or []
    vout = tx.get("vout") or []
    if not vin or not vout:
        return False

    first_vin = vin[0] or {}
    if "coinbase" in first_vin:
        return False

    first_vout = vout[0] or {}
    first_value = first_vout.get("value")
    spk = first_vout.get("scriptPubKey") or {}
    first_type = spk.get("type")
    first_hex = (spk.get("hex") or "").strip()
    first_asm = (spk.get("asm") or "").strip()

    return (
        first_value == 0.0
        and first_type == "nonstandard"
        and first_hex == ""
        and first_asm == ""
    )


def classify_transaction(tx):
    """
    Return a simple explorer classification for a decoded transaction.
    """
    if not isinstance(tx, dict):
        return "unknown"

    vin = tx.get("vin") or []
    if vin and isinstance(vin[0], dict) and "coinbase" in vin[0]:
        return "coinbase"

    if is_probable_coinstake(tx):
        return "coinstake"

    return "regular"


# --- Explorer index database integration ---

def index_db_connect():
    """
    Open the local explorer index database.
    """
    conn = sqlite3.connect(EXPLORER_INDEX_DB)
    conn.row_factory = sqlite3.Row
    return conn


def index_get_address_summary(address, tip_height=None, limit=50, offset=0):
    """
    Read address history and totals from the local explorer index.
    Returns None if the required index table does not exist yet.
    """
    conn = index_db_connect()
    try:
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='address_txs'"
        ).fetchone()
        if not table_exists:
            return None

        totals = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END), 0) AS received_total,
                COALESCE(SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END), 0) AS sent_total,
                COALESCE(SUM(delta), 0) AS balance_delta,
                COUNT(*) AS entry_count,
                COUNT(DISTINCT txid) AS tx_count
            FROM address_txs
            WHERE address = ?
            """,
            (address,),
        ).fetchone()

        rows = conn.execute(
            """
            SELECT
                txid,
                block_height,
                block_time,
                tx_type,
                role,
                delta,
                other_address
            FROM address_txs
            WHERE address = ?
            ORDER BY
                CASE WHEN block_height IS NULL THEN 1 ELSE 0 END,
                block_height DESC,
                block_time DESC,
                txid DESC,
                id DESC
            LIMIT ? OFFSET ?
            """,
            (address, int(limit), int(offset)),
        ).fetchall()

        tx_rows = []
        for row in rows:
            block_height = row["block_height"]
            confirmations = None
            if isinstance(tip_height, int) and isinstance(block_height, int):
                confirmations = max(0, tip_height - block_height + 1)

            delta = float(row["delta"] or 0)
            received = round(delta, 8) if delta > 0 else 0.0
            sent = round(-delta, 8) if delta < 0 else 0.0

            tx_rows.append({
                "txid": row["txid"],
                "block_height": block_height,
                "time": format_local_time(row["block_time"]),
                "time_unix": row["block_time"],
                "confirmations": confirmations,
                "tx_type": row["tx_type"] or "unknown",
                "role": row["role"] or "unknown",
                "other_address": row["other_address"] or "",
                "received": round(received, 8),
                "sent": round(sent, 8),
                "delta": round(delta, 8),
            })

        return {
            "received_total": round(float(totals["received_total"] or 0), 8),
            "sent_total": round(float(totals["sent_total"] or 0), 8),
            "balance_delta": round(float(totals["balance_delta"] or 0), 8),
            "entry_count": int(totals["entry_count"] or 0),
            "tx_count": int(totals["tx_count"] or 0),
            "tx_rows": tx_rows,
        }
    finally:
        conn.close()


# --- Supply stats and richlist ---

def index_get_supply_stats():
    """
    Return explorer-wide supply stats.
    Use the node-reported moneysupply as the single displayed supply value to avoid
    confusing users with locally reconstructed UTXO-sum differences.
    """
    blockchaininfo = rpc_request("getblockchaininfo")
    return {
        "total_supply": int(blockchaininfo.get("moneysupply") or 0),
    }


def index_get_top_wallets(limit=100, offset=0):
    """
    Return the richest addresses from the indexed UTXO set.
    Uses only currently unspent outputs with a single resolved address.
    """
    conn = index_db_connect()
    try:
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tx_outputs'"
        ).fetchone()
        if not table_exists:
            return None

        rows = conn.execute(
            """
            SELECT
                address,
                ROUND(SUM(value), 8) AS balance,
                COUNT(*) AS utxo_count
            FROM tx_outputs
            WHERE spent_by_txid IS NULL
              AND address IS NOT NULL
              AND TRIM(address) <> ''
              AND address NOT LIKE 'pubkey:%'
            GROUP BY address
            HAVING SUM(value) > 0
            ORDER BY SUM(value) DESC, address ASC
            LIMIT ? OFFSET ?
            """,
            (int(limit), int(offset)),
        ).fetchall()

        total_wallets_row = conn.execute(
            """
            SELECT COUNT(*) AS wallet_count
            FROM (
                SELECT address
                FROM tx_outputs
                WHERE spent_by_txid IS NULL
                  AND address IS NOT NULL
                  AND TRIM(address) <> ''
                  AND address NOT LIKE 'pubkey:%'
                GROUP BY address
                HAVING SUM(value) > 0
            ) t
            """
        ).fetchone()

        wallets = []
        for row in rows:
            wallets.append({
                "address": row["address"],
                "balance": round(float(row["balance"] or 0), 8),
                "utxo_count": int(row["utxo_count"] or 0),
            })

        return {
            "wallets": wallets,
            "wallet_count": int(total_wallets_row["wallet_count"] or 0),
        }
    finally:
        conn.close()


def render_page(content_html, error=None, tip_height=None, **ctx):
    """
    Render the inner content template first (with its own context),
    then embed the resulting HTML into the base layout.
    """
    # First render the per-page content (block/tx/index template)
    inner_html = render_template_string(content_html, tx_brief=tx_brief, **ctx)

    # Then render the base template and inject the already-rendered HTML
    return render_template_string(
        BASE_TEMPLATE,
        content=inner_html,
        error=error,
        tip_height=tip_height,
        tx_brief=tx_brief,
    )


# --- Supply & Richlist page ---

@app.route("/stats")
def view_stats():
    """
    Explorer-wide supply stats and richlist from the local SQLite index.
    """
    error = None
    tip_height = None
    supply = None
    wallets = []
    wallet_count = 0
    per_page = 100
    page = request.args.get("page", default=1, type=int)
    if page is None or page < 1:
        page = 1
    offset = (page - 1) * per_page

    try:
        tip_height = rpc_request("getblockcount")
        supply = index_get_supply_stats()
        richlist = index_get_top_wallets(limit=per_page, offset=offset)

        if supply is None or richlist is None:
            error = (
                "Explorer stats index not found. Please build it first with indexer.py "
                f"(database: {EXPLORER_INDEX_DB})."
            )
        else:
            wallets = richlist["wallets"]
            wallet_count = richlist["wallet_count"]
    except Exception as e:
        error = public_error_message(e, "Failed to load supply statistics.")

    total_pages = max(1, math.ceil(wallet_count / per_page)) if wallet_count else 1
    if page > total_pages:
        page = total_pages
    has_prev = page > 1
    has_next = page < total_pages
    prev_page = page - 1 if has_prev else None
    next_page = page + 1 if has_next else None
    rank_start = offset + 1

    content = """
    <h2>Supply &amp; Richlist</h2>

    {% if supply %}
      <p>
        <strong>Total supply:</strong> {{ supply.total_supply }}
        &nbsp;|&nbsp;
        <strong>Wallets with balance:</strong> {{ wallet_count }}
      </p>
    {% endif %}

    {% if wallets %}
      <p class="meta">
        Top wallets page {{ page }} of {{ total_pages }}
        &nbsp;|&nbsp;
        Showing up to {{ per_page }} wallets per page
      </p>

      <table>
        <tr>
          <th style="width: 8%;">Rank</th>
          <th style="width: 62%;">Address</th>
          <th style="width: 18%;">Balance</th>
          <th style="width: 12%;">UTXOs</th>
        </tr>
        {% for wallet in wallets %}
        <tr>
          <td class="small-cell">{{ rank_start + loop.index0 }}</td>
          <td class="addr-cell">
            <a href="{{ url_for('view_address', address=wallet.address) }}">{{ wallet.address }}</a>
          </td>
          <td class="amount-cell">{{ wallet.balance }}</td>
          <td class="small-cell">{{ wallet.utxo_count }}</td>
        </tr>
        {% endfor %}
      </table>

      <div style="margin-top: 1rem; display: flex; gap: 0.75rem; flex-wrap: wrap;">
        {% if has_prev %}
          <a href="{{ url_for('view_stats', page=prev_page) }}">&larr; Higher balances</a>
        {% endif %}
        {% if has_next %}
          <a href="{{ url_for('view_stats', page=next_page) }}">Lower balances &rarr;</a>
        {% endif %}
      </div>
    {% else %}
      <p>No indexed wallet balances found.</p>
      <p class="meta">Run <code>indexer.py</code> first to populate the local explorer index database.</p>
    {% endif %}
    """

    return render_page(
        content,
        error=error,
        tip_height=tip_height,
        supply=supply,
        wallets=wallets,
        wallet_count=wallet_count,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        has_prev=has_prev,
        has_next=has_next,
        prev_page=prev_page,
        next_page=next_page,
        rank_start=rank_start,
    )


@app.route("/")
def index():
    """
    Home page: show recent blocks with pagination.
    """
    error = None
    blocks = []
    tip_height = None
    per_page = 25
    page = request.args.get("page", default=1, type=int)
    if page is None or page < 1:
        page = 1

    total_pages = 1
    has_prev = False
    has_next = False
    prev_page = None
    next_page = None

    try:
        tip_height = rpc_request("getblockcount")
        total_blocks = tip_height + 1
        total_pages = max(1, math.ceil(total_blocks / per_page))
        if page > total_pages:
            page = total_pages

        newest_height = tip_height - ((page - 1) * per_page)
        oldest_height = max(0, newest_height - per_page + 1)

        for h in range(newest_height, oldest_height - 1, -1):
            bhash = rpc_request("getblockhash", [h])
            blk = rpc_request("getblock", [bhash])
            blocks.append({
                "height": h,
                "hash": bhash,
                "time": blk.get("time"),
                "time_local": format_local_time(blk.get("time")),
                "tx_count": len(blk.get("tx", [])),
            })

        has_prev = page > 1
        has_next = page < total_pages
        prev_page = page - 1 if has_prev else None
        next_page = page + 1 if has_next else None
    except Exception as e:
        error = public_error_message(e, "Failed to load latest blocks.")

    content = """
    <h2>Latest blocks</h2>
    {% if blocks %}
    <p class="meta">
      Page {{ page }} of {{ total_pages }}
      &nbsp;|&nbsp;
      Showing up to {{ per_page }} blocks per page
    </p>

    <table>
      <tr>
        <th style="width: 8%;">Height</th>
        <th style="width: 52%;">Hash</th>
        <th style="width: 28%;">Time</th>
        <th style="width: 12%;">Tx count</th>
    </tr>
      {% for b in blocks %}
      <tr>
        <td class="small-cell"><a href="{{ url_for('view_block_height', height=b.height) }}">{{ b.height }}</a></td>
        <td class="hash-cell"><a class="hash-link" href="{{ url_for('view_block_hash', blockhash=b.hash) }}" title="{{ b.hash }}">{{ b.hash }}</a></td>
        <td class="small-cell" title="{{ b.time }}">{{ b.time_local }}</td>
        <td class="small-cell">{{ b.tx_count }}</td>
      </tr>
      {% endfor %}
    </table>

    <div style="margin-top: 1rem; display: flex; gap: 0.75rem; flex-wrap: wrap;">
      {% if has_prev %}
        <a href="{{ url_for('index', page=prev_page) }}">&larr; Newer blocks</a>
      {% endif %}
      {% if has_next %}
        <a href="{{ url_for('index', page=next_page) }}">Older blocks &rarr;</a>
      {% endif %}
    </div>
    {% else %}
      <p>No block data available.</p>
    {% endif %}
    """
    return render_page(
        content,
        error=error,
        tip_height=tip_height,
        blocks=blocks,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        has_prev=has_prev,
        has_next=has_next,
        prev_page=prev_page,
        next_page=next_page,
    )


# --- Peers page and API ---


@app.route("/peers")
def view_peers():
    """
    Show currently connected peers from getpeerinfo.
    Useful for manually collecting addnode targets.
    """
    error = None
    tip_height = None
    peers = []
    try:
        tip_height = rpc_request("getblockcount")
        raw_peers = rpc_request("getpeerinfo")

        for peer in raw_peers:
            synced_blocks = peer.get("synced_blocks")
            synced_headers = peer.get("synced_headers")

            if isinstance(synced_blocks, int) and synced_blocks >= 0:
                sync_display = str(synced_blocks)
                sync_sort = synced_blocks
            elif isinstance(synced_headers, int) and synced_headers >= 0:
                sync_display = f"headers only ({synced_headers})"
                sync_sort = synced_headers
            else:
                sync_display = "unknown"
                sync_sort = -1

            peers.append({
                "id": peer.get("id"),
                "addr": peer_addnode_target(peer) or peer.get("addr", ""),
                "version": peer.get("version", ""),
                "inbound": peer.get("inbound", False),
                "synced_headers": synced_headers,
                "synced_blocks": synced_blocks,
                "startingheight": peer.get("startingheight"),
                "sync_display": sync_display,
                "sync_sort": sync_sort,
            })

        peers.sort(key=lambda p: (-p["sync_sort"], p["addr"]))
    except Exception as e:
        error = public_error_message(e, "Failed to load peers.")

    content = """
    <h2>Connected peers</h2>
    {% if peers %}
    <table>
      <tr>
        <th>ID</th>
        <th>Peer</th>
        <th>Inbound</th>
        <th>Version</th>
        <th>Sync</th>
        <th>Starting height</th>
      </tr>
      {% for p in peers %}
      <tr>
        <td>{{ p.id }}</td>
        <td><code>{{ p.addr }}</code></td>
        <td>{{ "yes" if p.inbound else "no" }}</td>
        <td>{{ p.version }}</td>
        <td>{{ p.sync_display }}</td>
        <td>{{ p.startingheight if p.startingheight is not none else "?" }}</td>
      </tr>
      {% endfor %}
    </table>

    <h3>Quick addnode list</h3>
    <pre>{% for p in peers %}{% if p.addr %}addnode={{ p.addr }}
{% endif %}{% endfor %}</pre>
    {% else %}
      <p>No peers available.</p>
    {% endif %}
    """
    return render_page(content, error=error, tip_height=tip_height, peers=peers)


@app.route("/api/peers")
def api_peers():
    """
    Return connected peers as JSON.
    """
    peers = rpc_request("getpeerinfo")
    return jsonify(peers)


@app.route("/search")
def search():
    """
    Simple search:
    - integer -> block height
    - 64-hex -> try block hash, then txid
    - address-like string -> address page
    - anything else -> rejected as invalid search input
    """
    q = request.args.get("q", "").strip()
    if not q:
        return redirect(url_for("index"))

    # Check if the query is a block height.
    if q.isdigit():
        return redirect(url_for("view_block_height", height=int(q)))

    # Strict block hash / txid handling: only real 64-hex strings are accepted.
    if looks_like_txid(q):
        try:
            rpc_request("getblock", [q])
            return redirect(url_for("view_block_hash", blockhash=q))
        except Exception:
            return redirect(url_for("view_tx", txid=q))

    # Best-effort address search.
    if looks_like_address(q):
        return redirect(url_for("view_address", address=q))

    error = "Invalid search term. Please enter a block height, block hash, txid, or wallet address."
    content = """
    <h2>Invalid search</h2>
    <p>{{ error_text }}</p>
    <p class="meta">Supported inputs: block height, 64-character block hash / txid, or wallet address.</p>
    """
    return render_page(content, error=error, tip_height=None, error_text=error)


@app.route("/address/<address>")
def view_address(address):
    """
    Show recent transactions involving a specific address.
    Uses the local explorer SQLite index instead of addressindex/searchrawtransactions.
    """
    error = None
    tip_height = None
    tx_rows = []
    received_total = 0.0
    sent_total = 0.0
    balance_delta = 0.0
    tx_count = 0
    per_page = 50
    page = request.args.get("page", default=1, type=int)
    if page is None or page < 1:
        page = 1
    offset = (page - 1) * per_page

    try:
        tip_height = rpc_request("getblockcount")
        summary = index_get_address_summary(address, tip_height=tip_height, limit=per_page, offset=offset)
        if summary is None:
            error = (
                "Explorer address index not found. Please build it first with indexer.py "
                f"(database: {EXPLORER_INDEX_DB})."
            )
        else:
            tx_rows = summary["tx_rows"]
            received_total = summary["received_total"]
            sent_total = summary["sent_total"]
            balance_delta = summary["balance_delta"]
            tx_count = summary["tx_count"]
    except Exception as e:
        error = public_error_message(e, "Failed to load address activity.")

    has_prev = page > 1
    has_next = len(tx_rows) == per_page
    prev_page = page - 1 if has_prev else None
    next_page = page + 1 if has_next else None

    content = """
    <h2>Address {{ address }}</h2>

    {% if tx_count %}
      <p>
        <strong>Net balance delta:</strong> {{ balance_delta }}
        &nbsp;|&nbsp;
        <strong>Total received:</strong> {{ received_total }}
        &nbsp;|&nbsp;
        <strong>Total sent:</strong> {{ sent_total }}
        &nbsp;|&nbsp;
        <strong>Indexed transactions:</strong> {{ tx_count }}
      </p>

      <p class="meta">
        Page {{ page }}
        &nbsp;|&nbsp;
        Showing up to {{ per_page }} address entries per page from the local explorer index
      </p>

      <table>
        <tr>
          <th style="width: 16%;">Time</th>
          <th style="width: 26%;">Txid</th>
          <th style="width: 10%;">Height</th>
          <th style="width: 10%;">Confirmations</th>
          <th style="width: 10%;">Type</th>
          <th style="width: 10%;">Role</th>
          <th style="width: 9%;">Delta</th>
          <th style="width: 9%;">Other</th>
        </tr>
        {% for row in tx_rows %}
        <tr>
          <td class="small-cell" title="{{ row.time_unix if row.time_unix is not none else '' }}">{{ row.time }}</td>
          <td class="hash-cell">
            <a class="hash-link" href="{{ url_for('view_tx', txid=row.txid) }}" title="{{ row.txid }}">{{ tx_brief(row.txid) }}</a>
          </td>
          <td class="small-cell">{{ row.block_height if row.block_height is not none else "unconfirmed" }}</td>
          <td class="small-cell">{{ row.confirmations if row.confirmations is not none else "unknown" }}</td>
          <td>{{ row.tx_type }}</td>
          <td>{{ row.role }}</td>
          <td class="amount-cell">{{ row.delta }}</td>
          <td class="addr-cell">
            {% if row.other_address %}
              <a href="{{ url_for('view_address', address=row.other_address) }}">{{ row.other_address }}</a>
            {% else %}
              -
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </table>

      <div style="margin-top: 1rem; display: flex; gap: 0.75rem; flex-wrap: wrap;">
        {% if has_prev %}
          <a href="{{ url_for('view_address', address=address, page=prev_page) }}">&larr; Newer entries</a>
        {% endif %}
        {% if has_next %}
          <a href="{{ url_for('view_address', address=address, page=next_page) }}">Older entries &rarr;</a>
        {% endif %}
      </div>
    {% else %}
      <p>No indexed address activity found.</p>
      <p class="meta">Run <code>indexer.py</code> first to populate the local address index database.</p>
    {% endif %}
    """

    return render_page(
        content,
        error=error,
        tip_height=tip_height,
        address=address,
        tx_rows=tx_rows,
        received_total=received_total,
        sent_total=sent_total,
        balance_delta=balance_delta,
        tx_count=tx_count,
        page=page,
        per_page=per_page,
        has_prev=has_prev,
        has_next=has_next,
        prev_page=prev_page,
        next_page=next_page,
    )


@app.route("/block/height/<int:height>")
def view_block_height(height):
    """
    Show a block by its height.
    """
    error = None
    tip_height = None
    block = None
    try:
        tip_height = rpc_request("getblockcount")
        bhash = rpc_request("getblockhash", [height])
        block = rpc_request("getblock", [bhash])
        # Make sure the hash field is present in the dict
        if isinstance(block, dict):
            block = dict(block)
            block.setdefault("hash", bhash)
            block["time_local"] = format_local_time(block.get("time"))
    except Exception as e:
        error = public_error_message(e, "Failed to load block by height.")

    content = """
    <h2>Block {{ height }}</h2>
    {% if block %}
      <p><strong>Hash:</strong>
        <a href="{{ url_for('view_block_hash', blockhash=block.hash) }}">{{ block.hash }}</a>
      </p>
      {% if block.previousblockhash %}
        <p><strong>Parent:</strong>
          <a href="{{ url_for('view_block_hash', blockhash=block.previousblockhash) }}">
            {{ block.previousblockhash }}
          </a>
        </p>
      {% endif %}
      <p><strong>Time:</strong> {{ block.time_local }} <span class="meta" title="{{ block.time }}">(unix: {{ block.time }})</span></p>
      <p><strong>Difficulty:</strong> {{ block.difficulty }}</p>
      <p><strong>Transactions:</strong> {{ block.tx|length }}</p>

      <h3>Transactions ({{ block.tx|length }})</h3>
      <table>
        <tr>
          <th style="width: 8%;">#</th>
          <th style="width: 92%;">Txid</th>
        </tr>
        {% for txid in block.tx %}
        <tr>
          <td class="index-cell">{{ loop.index0 }}</td>
          <td class="hash-cell"><a class="hash-link" href="{{ url_for('view_tx', txid=txid) }}" title="{{ txid }}">{{ txid }}</a></td>
        </tr>
        {% endfor %}
      </table>

      <h3>Raw block (JSON)</h3>
      <pre>{{ block|tojson(indent=2) }}</pre>
    {% else %}
      <p>Block could not be loaded.</p>
    {% endif %}
    """
    return render_page(content, error=error, tip_height=tip_height, block=block, height=height)


@app.route("/block/hash/<blockhash>")
def view_block_hash(blockhash):
    """
    Show a block by its hash.
    """
    error = None
    tip_height = None
    block = None
    height = None
    try:
        tip_height = rpc_request("getblockcount")
        block = rpc_request("getblock", [blockhash])
        if isinstance(block, dict):
            height = block.get("height")
            block["time_local"] = format_local_time(block.get("time"))
    except Exception as e:
        error = public_error_message(e, "Failed to load block by hash.")

    content = """
    <h2>Block {{ height if height is not none else "unknown" }}</h2>
    {% if block %}
      <p><strong>Hash:</strong> {{ blockhash }}</p>
      {% if block.previousblockhash %}
        <p><strong>Parent:</strong>
          <a href="{{ url_for('view_block_hash', blockhash=block.previousblockhash) }}">
            {{ block.previousblockhash }}
          </a>
        </p>
      {% endif %}
      <p><strong>Time:</strong> {{ block.time_local }} <span class="meta" title="{{ block.time }}">(unix: {{ block.time }})</span></p>
      <p><strong>Difficulty:</strong> {{ block.difficulty }}</p>
      <p><strong>Transactions:</strong> {{ block.tx|length }}</p>

      <h3>Transactions ({{ block.tx|length }})</h3>
      <table>
        <tr>
          <th style="width: 8%;">#</th>
          <th style="width: 92%;">Txid</th>
        </tr>
        {% for txid in block.tx %}
        <tr>
          <td class="index-cell">{{ loop.index0 }}</td>
          <td class="hash-cell"><a class="hash-link" href="{{ url_for('view_tx', txid=txid) }}" title="{{ txid }}">{{ txid }}</a></td>
        </tr>
        {% endfor %}
      </table>

      <h3>Raw block (JSON)</h3>
      <pre>{{ block|tojson(indent=2) }}</pre>
    {% else %}
      <p>Block could not be loaded.</p>
    {% endif %}
    """
    return render_page(
        content,
        error=error,
        tip_height=tip_height,
        block=block,
        blockhash=blockhash,
        height=height,
    )


@app.route("/tx/<txid>")
def view_tx(txid):
    """
    Show a transaction by its txid, using getrawtransaction(txid, 1).
    """
    error = None
    tip_height = None
    tx = None
    try:
        tip_height = rpc_request("getblockcount")
        tx = rpc_request("getrawtransaction", [txid, 1])
        tx = enrich_transaction(tx)
    except Exception as e:
        error = public_error_message(e, "Failed to load transaction.")

    content = """
    <h2>Transaction {{ txid }}</h2>
    {% if tx %}
      <p><strong>Blockhash:</strong>
        {% if tx.blockhash %}
          <a href="{{ url_for('view_block_hash', blockhash=tx.blockhash) }}">{{ tx.blockhash }}</a>
        {% else %}
          (unconfirmed)
        {% endif %}
      </p>
      <p><strong>Confirmations:</strong>
        {{ tx.confirmations if tx.confirmations is defined else "unknown" }}
      </p>
      <p><strong>Time:</strong>
        {{ tx.time if tx.time is defined else "unknown" }}
      </p>
      <p><strong>Transaction type:</strong>
        {{ tx.explorer_type if tx.explorer_type is defined else "unknown" }}
      </p>

      <h3>Inputs (senders)</h3>
      {% if tx.explorer_inputs %}
      <table>
        <tr>
          <th style="width: 4%;">#</th>
          <th style="width: 41%;">From</th>
          <th style="width: 10%;">Amount</th>
          <th style="width: 29%;">Source</th>
          <th style="width: 8%;">Type</th>
          <th style="width: 8%;">Status</th>
        </tr>
      {% for vin in tx.explorer_inputs %}
        <tr>
        <td class="index-cell">{{ vin.index }}</td>
        <td class="addr-cell">
            {% if vin.is_coinbase %}
            coinbase
            {% elif vin.addresses %}
            {% for addr in vin.addresses %}
                {% if addr.startswith('pubkey:') %}
                {{ addr }}
                {% else %}
                <a href="{{ url_for('view_address', address=addr) }}">{{ addr }}</a>
                {% endif %}
                {% if not loop.last %}<br>{% endif %}
            {% endfor %}
            {% else %}
            <span class="hash-link">{{ vin.address_display }}</span>
            {% endif %}
        </td>
        <td class="amount-cell">
            {% if vin.value is not none %}
            {{ vin.value }}
            {% else %}
            unknown
            {% endif %}
        </td>
        <td class="hash-cell">
            {% if vin.txid %}
            <a class="hash-link" href="{{ url_for('view_tx', txid=vin.txid) }}" title="{{ vin.txid }}">{{ tx_brief(vin.txid) }}</a>
            {% if vin.vout is not none %}
                :{{ vin.vout }}
            {% endif %}
            {% else %}
            -
            {% endif %}
        </td>
        <td>{{ vin.type }}</td>
        <td>{{ vin.error if vin.error else "ok" }}</td>
        </tr>
        {% endfor %}  
      </table>
      {% else %}
        <p>No inputs available.</p>
      {% endif %}

      <p class="meta">
        <strong>Total input:</strong>
        {{ tx.explorer_input_total if tx.explorer_input_total is not none else "unknown" }}
        &nbsp;|&nbsp;
        <strong>Total output:</strong> {{ tx.explorer_output_total }}
            {% if tx.explorer_type == "coinstake" %}
            &nbsp;|&nbsp;
            <strong>Reward:</strong> {{ tx.explorer_reward if tx.explorer_reward is not none else "unknown" }}
        {% else %}
            &nbsp;|&nbsp;
            <strong>Fee:</strong> {{ tx.explorer_fee if tx.explorer_fee is not none else "unknown" }}
        {% endif %}
       </p>

      <h3>Outputs (recipients)</h3>
      {% if tx.explorer_outputs %}
      <table>
        <tr>
          <th style="width: 4%;">n</th>
          <th style="width: 66%;">To</th>
          <th style="width: 14%;">Amount</th>
          <th style="width: 16%;">Type</th>
        </tr>
        {% for vout in tx.explorer_outputs %}
        <tr>
        <td class="index-cell">{{ vout.n }}</td>
        <td class="addr-cell">
            {% if vout.addresses %}
            {% for addr in vout.addresses %}
                {% if addr.startswith('pubkey:') %}
                {{ addr }}
                {% else %}
                <a href="{{ url_for('view_address', address=addr) }}">{{ addr }}</a>
                {% endif %}
                {% if not loop.last %}<br>{% endif %}
            {% endfor %}
            {% else %}
            {{ vout.address_display }}
            {% endif %}
        </td>
        <td class="amount-cell">{{ vout.value }}</td>
        <td>
            {{ vout.type }}
            {% if tx.explorer_type == "coinstake" and vout.n == 0 and vout.type == "nonstandard" and vout.value == 0.0 %}
            <br><span class="meta">coinstake marker</span>
            {% endif %}
        </td>
        </tr>
        {% endfor %}
      </table>
      {% else %}
        <p>No outputs available.</p>
      {% endif %}

      {% if tx.explorer_addresses %}
      <h3>Addresses in this transaction</h3>
      <ul>
        {% for addr in tx.explorer_addresses %}
          <li><a href="{{ url_for('view_address', address=addr) }}">{{ addr }}</a></li>
        {% endfor %}
      </ul>
      {% endif %}

      <h3>Vin</h3>
      <pre>{{ tx.vin|tojson(indent=2) }}</pre>

      <h3>Vout</h3>
      <pre>{{ tx.vout|tojson(indent=2) }}</pre>

      <h3>Raw transaction (JSON)</h3>
      <pre>{{ tx|tojson(indent=2) }}</pre>
    {% else %}
      <p>Transaction could not be loaded.</p>
    {% endif %}
    """

    return render_page(
        content,
        error=error,
        tip_height=tip_height,
        tx=tx,
        txid=txid,
    )


if __name__ == "__main__":
    # Run the development server, reachable in the local network.
    app.run(host="0.0.0.0", port=8080, debug=False)
