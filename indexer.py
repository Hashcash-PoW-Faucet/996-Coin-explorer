#!/usr/bin/env python3
import hashlib
import os
import sqlite3
import time
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

RPC_USER = os.environ.get("NNS_RPC_USER", "test")
RPC_PASSWORD = os.environ.get("NNS_RPC_PASSWORD", "test")
RPC_HOST = os.environ.get("NNS_RPC_HOST", "127.0.0.1")
RPC_PORT = os.environ.get("NNS_RPC_PORT", "41683")
EXPLORER_INDEX_DB = os.environ.get("EXPLORER_INDEX_DB", "explorer_index.db")


REORG_CHECK_DEPTH = int(os.environ.get("EXPLORER_REORG_CHECK_DEPTH", "20"))
REORG_REWIND_EXTRA = int(os.environ.get("EXPLORER_REWIND_EXTRA", "5"))
TIP_CONFIRMATION_BUFFER = int(os.environ.get("EXPLORER_TIP_CONFIRMATION_BUFFER", "2"))

RPC_TIMEOUT = int(os.environ.get("EXPLORER_RPC_TIMEOUT", "20"))
STORE_RAW_JSON = os.environ.get("EXPLORER_STORE_RAW_JSON", "0").strip().lower() in {"1", "true", "yes", "on"}

PUBKEY_ADDRESS_PREFIX = int(os.environ.get("NNS_PUBKEY_ADDRESS_PREFIX", "53"))
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def rpc_request(method: str, params: Optional[list] = None):
    url = f"http://{RPC_HOST}:{RPC_PORT}"
    payload = {
        "jsonrpc": "1.0",
        "id": "nns-indexer",
        "method": method,
        "params": params or [],
    }
    resp = requests.post(
        url,
        json=payload,
        auth=(RPC_USER, RPC_PASSWORD),
        timeout=RPC_TIMEOUT,
    )

    # Old Bitcoin-family daemons often return HTTP 500 for JSON-RPC errors.
    # Therefore parse the JSON body first so we can see the real RPC message.
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise RuntimeError(f"RPC response was not valid JSON for method {method}")

    if data.get("error"):
        raise RuntimeError(f"RPC error in {method}: {data['error']}")

    resp.raise_for_status()
    return data["result"]


def sha256_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def ripemd160_bytes(data: bytes) -> bytes:
    h = hashlib.new("ripemd160")
    h.update(data)
    return h.digest()


def hash160(data: bytes) -> bytes:
    return ripemd160_bytes(sha256_bytes(data))


def b58encode(data: bytes) -> str:
    zeros = 0
    while zeros < len(data) and data[zeros] == 0:
        zeros += 1

    value = int.from_bytes(data, byteorder="big", signed=False)
    chars = []
    while value > 0:
        value, mod = divmod(value, 58)
        chars.append(BASE58_ALPHABET[mod])

    encoded = "".join(reversed(chars)) if chars else ""
    return ("1" * zeros) + (encoded or "") or "1"


def b58check_encode(payload: bytes) -> str:
    checksum = sha256_bytes(sha256_bytes(payload))[:4]
    return b58encode(payload + checksum)


def compressed_pubkey_to_address(pubkey_hex: str) -> Optional[str]:
    """
    Convert a compressed secp256k1 public key hex string directly into a Base58Check
    wallet address using the chain's PUBKEY_ADDRESS prefix.
    No EC math is needed here because the public key is already provided.
    """
    if not isinstance(pubkey_hex, str):
        return None

    pubkey_hex = pubkey_hex.strip()
    if len(pubkey_hex) != 66:
        return None
    if not (pubkey_hex.startswith("02") or pubkey_hex.startswith("03")):
        return None

    try:
        pubkey_bytes = bytes.fromhex(pubkey_hex)
    except Exception:
        return None

    payload = bytes([PUBKEY_ADDRESS_PREFIX]) + hash160(pubkey_bytes)
    return b58check_encode(payload)


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(EXPLORER_INDEX_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS blocks (
            height INTEGER PRIMARY KEY,
            hash TEXT NOT NULL UNIQUE,
            time INTEGER,
            tx_count INTEGER NOT NULL DEFAULT 0,
            indexed_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            txid TEXT PRIMARY KEY,
            block_height INTEGER,
            block_hash TEXT,
            block_time INTEGER,
            tx_type TEXT,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS tx_outputs (
            txid TEXT NOT NULL,
            n INTEGER NOT NULL,
            value REAL,
            script_type TEXT,
            address TEXT,
            addresses_json TEXT,
            spent_by_txid TEXT,
            spent_by_vin INTEGER,
            PRIMARY KEY (txid, n)
        );

        CREATE TABLE IF NOT EXISTS tx_inputs (
            txid TEXT NOT NULL,
            vin_n INTEGER NOT NULL,
            prev_txid TEXT,
            prev_vout INTEGER,
            value REAL,
            script_type TEXT,
            address TEXT,
            addresses_json TEXT,
            PRIMARY KEY (txid, vin_n)
        );

        CREATE TABLE IF NOT EXISTS address_txs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            txid TEXT NOT NULL,
            block_height INTEGER,
            block_hash TEXT,
            block_time INTEGER,
            tx_type TEXT,
            role TEXT NOT NULL,
            delta REAL NOT NULL,
            other_address TEXT,
            vin_index INTEGER,
            vout INTEGER
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_address_txs_unique
        ON address_txs(address, txid, role, COALESCE(vin_index, -1), COALESCE(vout, -1));

        CREATE INDEX IF NOT EXISTS idx_address_txs_address_time
        ON address_txs(address, block_height DESC, block_time DESC, txid DESC);

        CREATE INDEX IF NOT EXISTS idx_address_txs_txid
        ON address_txs(txid);

        CREATE INDEX IF NOT EXISTS idx_tx_outputs_address
        ON tx_outputs(address);

        CREATE INDEX IF NOT EXISTS idx_tx_outputs_spent
        ON tx_outputs(spent_by_txid);

        CREATE INDEX IF NOT EXISTS idx_tx_inputs_address
        ON tx_inputs(address);
        """
    )
    conn.commit()


def get_meta(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    return row["value"]


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO meta(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def extract_vout_addresses(vout: dict) -> List[str]:
    spk = vout.get("scriptPubKey") or {}

    addresses = spk.get("addresses")
    if isinstance(addresses, list) and addresses:
        return [str(a).strip() for a in addresses if str(a).strip()]

    address = spk.get("address")
    if isinstance(address, str) and address.strip():
        return [address.strip()]

    asm = spk.get("asm")
    if spk.get("type") == "pubkey" and isinstance(asm, str) and asm.strip():
        pubkey_hex = asm.split()[0].strip()
        derived_address = compressed_pubkey_to_address(pubkey_hex)
        if derived_address:
            return [derived_address]
        return [f"pubkey:{pubkey_hex}"]

    return []


def classify_transaction(tx: dict) -> str:
    vin = tx.get("vin") or []
    if vin and isinstance(vin[0], dict) and "coinbase" in vin[0]:
        return "coinbase"

    vout = tx.get("vout") or []
    if vin and vout:
        first_vout = vout[0] or {}
        first_value = first_vout.get("value")
        spk = first_vout.get("scriptPubKey") or {}
        if (
            "coinbase" not in (vin[0] or {})
            and first_value == 0.0
            and spk.get("type") == "nonstandard"
            and (spk.get("hex") or "").strip() == ""
            and (spk.get("asm") or "").strip() == ""
        ):
            return "coinstake"

    return "regular"


def jsonish(value) -> str:
    import json
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def get_tx_cached(txid: str, tx_cache: Dict[str, dict]) -> dict:
    if txid not in tx_cache:
        try:
            tx_cache[txid] = rpc_request("getrawtransaction", [txid, 1])
        except Exception as e:
            raise RuntimeError(
                f"Failed to load transaction {txid} via getrawtransaction. "
                f"Make sure txindex=1 is fully built (or that the tx is not a special case such as the genesis coinbase). "
                f"Original error: {e}"
            )
    return tx_cache[txid]


def get_prevout_from_db(conn: sqlite3.Connection, prev_txid: str, prev_vout_n: int) -> Tuple[Optional[str], List[str], Optional[float], str]:
    """
    Resolve a prevout from the local SQLite index first.
    This is much faster than going back to RPC once earlier blocks are already indexed.
    """
    row = conn.execute(
        """
        SELECT value, script_type, address, addresses_json
        FROM tx_outputs
        WHERE txid = ? AND n = ?
        """,
        (prev_txid, prev_vout_n),
    ).fetchone()

    if not row:
        return None, [], None, "unknown"

    value = row["value"]
    script_type = row["script_type"] or "unknown"
    single_addr = row["address"]

    addresses: List[str] = []
    raw_addresses = row["addresses_json"]
    if raw_addresses:
        try:
            import json
            parsed = json.loads(raw_addresses)
            if isinstance(parsed, list):
                addresses = [str(a).strip() for a in parsed if str(a).strip()]
        except Exception:
            addresses = []

    if single_addr and not addresses:
        addresses = [single_addr]

    # Backward-compatibility path for older index databases that stored pubkey:... instead
    # of the derived wallet address. New indexing runs should already store the real address.
    if len(addresses) == 1 and isinstance(addresses[0], str) and addresses[0].startswith("pubkey:"):
        derived_address = compressed_pubkey_to_address(addresses[0].split(":", 1)[1])
        if derived_address:
            addresses = [derived_address]

    if len(addresses) == 1:
        return addresses[0], addresses, value, script_type

    return None, addresses, value, script_type


def resolve_prevout(conn: sqlite3.Connection, vin: dict, tx_cache: Dict[str, dict]) -> Tuple[Optional[str], List[str], Optional[float], str]:
    if "coinbase" in vin:
        return "coinbase", ["coinbase"], None, "coinbase"

    prev_txid = vin.get("txid")
    prev_vout_n = vin.get("vout")
    if not prev_txid or prev_vout_n is None:
        return None, [], None, "unknown"

    # Fast path: prefer the local SQLite index once previous blocks are already indexed.
    db_single_addr, db_addresses, db_value, db_script_type = get_prevout_from_db(conn, prev_txid, prev_vout_n)
    if db_addresses or db_single_addr is not None or db_value is not None:
        return db_single_addr, db_addresses, db_value, db_script_type

    # Slow fallback: ask the node via RPC if the prevout is not yet present locally.
    try:
        prev_tx = get_tx_cached(prev_txid, tx_cache)
    except Exception:
        # The genesis coinbase is a common special case in old Bitcoin-family nodes:
        # it may exist in block 0 but still not be retrievable via getrawtransaction.
        # For indexing purposes we treat such unresolved prevouts as unknown instead of aborting the whole run.
        return None, [], None, "unknown"

    prev_outputs = prev_tx.get("vout") or []

    prevout = None
    for candidate in prev_outputs:
        if candidate.get("n") == prev_vout_n:
            prevout = candidate
            break

    if prevout is None:
        return None, [], None, "unknown"

    addresses = extract_vout_addresses(prevout)
    value = prevout.get("value")
    spk = prevout.get("scriptPubKey") or {}
    script_type = spk.get("type", "unknown")

    if len(addresses) == 1:
        return addresses[0], addresses, value, script_type

    return None, addresses, value, script_type


def get_single_output_address(vout: dict) -> Tuple[Optional[str], List[str], str]:
    addresses = extract_vout_addresses(vout)
    spk = vout.get("scriptPubKey") or {}
    script_type = spk.get("type", "unknown")
    if len(addresses) == 1:
        return addresses[0], addresses, script_type
    return None, addresses, script_type


def delete_from_height(conn: sqlite3.Connection, start_height: int) -> None:
    rows = conn.execute(
        "SELECT txid FROM transactions WHERE block_height >= ?",
        (start_height,),
    ).fetchall()
    txids = [row["txid"] for row in rows]

    conn.execute("DELETE FROM address_txs WHERE block_height >= ?", (start_height,))
    conn.execute("DELETE FROM tx_inputs WHERE txid IN (SELECT txid FROM transactions WHERE block_height >= ?)", (start_height,))
    conn.execute("DELETE FROM tx_outputs WHERE txid IN (SELECT txid FROM transactions WHERE block_height >= ?)", (start_height,))
    conn.execute("DELETE FROM transactions WHERE block_height >= ?", (start_height,))
    conn.execute("DELETE FROM blocks WHERE height >= ?", (start_height,))

    if txids:
        placeholders = ",".join("?" for _ in txids)
        conn.execute(
            f"""
            UPDATE tx_outputs
            SET spent_by_txid = NULL, spent_by_vin = NULL
            WHERE spent_by_txid IN ({placeholders})
            """,
            txids,
        )

    max_height_row = conn.execute("SELECT MAX(height) AS h FROM blocks").fetchone()
    last_height = max_height_row["h"] if max_height_row and max_height_row["h"] is not None else -1
    set_meta(conn, "last_indexed_height", str(last_height))
    conn.commit()


def check_reorg(conn: sqlite3.Connection, stable_tip_height: int) -> int:
    """
    Check the already indexed stable range for reorgs.
    We only compare blocks up to stable_tip_height so that very fresh tip blocks do not
    constantly churn the explorer index.
    """
    last_indexed = int(get_meta(conn, "last_indexed_height", "-1") or "-1")
    if last_indexed < 0:
        return 0

    # If the chain tip moved backwards (or we now intentionally index only up to a lower stable tip),
    # drop any indexed tail above the current stable tip.
    if last_indexed > stable_tip_height:
        rewind_height = max(0, stable_tip_height + 1)
        print(f"[reorg] indexed height {last_indexed} is above stable tip {stable_tip_height}, trimming tail from {rewind_height}")
        delete_from_height(conn, rewind_height)
        return rewind_height

    compare_to = min(last_indexed, stable_tip_height)
    start = max(0, compare_to - REORG_CHECK_DEPTH + 1)

    for height in range(start, compare_to + 1):
        row = conn.execute("SELECT hash FROM blocks WHERE height = ?", (height,)).fetchone()
        if not row:
            continue
        node_hash = rpc_request("getblockhash", [height])
        if node_hash != row["hash"]:
            rewind_height = max(0, height - REORG_REWIND_EXTRA)
            print(
                f"[reorg] mismatch at height {height}, rebuilding from {rewind_height} "
                f"(extra rewind={REORG_REWIND_EXTRA})"
            )
            delete_from_height(conn, rewind_height)
            return rewind_height

    return last_indexed + 1


def index_block(conn: sqlite3.Connection, height: int, tx_cache: Dict[str, dict]) -> None:
    block_hash = rpc_request("getblockhash", [height])
    block = rpc_request("getblock", [block_hash, 2])

    block_time = block.get("time")
    block_txs = block.get("tx") or []
    txids = []

    # With verbosity=2 the block already contains decoded transactions.
    # This avoids an immediate getrawtransaction() call for every tx in the block,
    # which is especially helpful for edge cases like the genesis block.
    for tx in block_txs:
        if isinstance(tx, dict) and tx.get("txid"):
            txid = tx["txid"]
            txids.append(txid)
            tx_cache[txid] = tx
        else:
            txids.append(tx)

    conn.execute(
        """
        INSERT OR REPLACE INTO blocks(height, hash, time, tx_count, indexed_at)
        VALUES(?, ?, ?, ?, ?)
        """,
        (height, block_hash, block_time, len(txids), int(time.time())),
    )

    for txid in txids:
        tx = get_tx_cached(txid, tx_cache)
        tx_type = classify_transaction(tx)

        raw_json_value = jsonish(tx) if STORE_RAW_JSON else None

        conn.execute(
            """
            INSERT OR REPLACE INTO transactions(txid, block_height, block_hash, block_time, tx_type, raw_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (txid, height, block_hash, block_time, tx_type, raw_json_value),
        )

        outputs = tx.get("vout") or []
        input_addresses: List[str] = []
        output_addresses: List[str] = []

        for vout in outputs:
            n = vout.get("n")
            value = vout.get("value")
            single_addr, addresses, script_type = get_single_output_address(vout)

            conn.execute(
                """
                INSERT OR REPLACE INTO tx_outputs(txid, n, value, script_type, address, addresses_json, spent_by_txid, spent_by_vin)
                VALUES(?, ?, ?, ?, ?, ?, COALESCE((SELECT spent_by_txid FROM tx_outputs WHERE txid = ? AND n = ?), NULL),
                       COALESCE((SELECT spent_by_vin FROM tx_outputs WHERE txid = ? AND n = ?), NULL))
                """,
                (
                    txid,
                    n,
                    value,
                    script_type,
                    single_addr,
                    jsonish(addresses),
                    txid,
                    n,
                    txid,
                    n,
                ),
            )

            if single_addr:
                output_addresses.append(single_addr)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO address_txs(
                        address, txid, block_height, block_hash, block_time, tx_type,
                        role, delta, other_address, vin_index, vout
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        single_addr,
                        txid,
                        height,
                        block_hash,
                        block_time,
                        tx_type,
                        "output",
                        float(value or 0),
                        None,
                        None,
                        n,
                    ),
                )

        vins = tx.get("vin") or []
        for vin_index, vin in enumerate(vins):
            prev_txid = vin.get("txid")
            prev_vout = vin.get("vout")
            single_addr, addresses, value, script_type = resolve_prevout(conn, vin, tx_cache)

            conn.execute(
                """
                INSERT OR REPLACE INTO tx_inputs(txid, vin_n, prev_txid, prev_vout, value, script_type, address, addresses_json)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    txid,
                    vin_index,
                    prev_txid,
                    prev_vout,
                    value,
                    script_type,
                    single_addr,
                    jsonish(addresses),
                ),
            )

            if prev_txid is not None and prev_vout is not None:
                conn.execute(
                    """
                    UPDATE tx_outputs
                    SET spent_by_txid = ?, spent_by_vin = ?
                    WHERE txid = ? AND n = ?
                    """,
                    (txid, vin_index, prev_txid, prev_vout),
                )

            if single_addr and single_addr != "coinbase":
                input_addresses.append(single_addr)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO address_txs(
                        address, txid, block_height, block_hash, block_time, tx_type,
                        role, delta, other_address, vin_index, vout
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        single_addr,
                        txid,
                        height,
                        block_hash,
                        block_time,
                        tx_type,
                        "input",
                        -float(value or 0),
                        None,
                        vin_index,
                        None,
                    ),
                )

        inferred_other = None
        if len(output_addresses) == 1 and len(input_addresses) == 1:
            inferred_other = output_addresses[0] if output_addresses[0] != input_addresses[0] else None

        if inferred_other:
            conn.execute(
                """
                UPDATE address_txs
                SET other_address = ?
                WHERE txid = ? AND role = 'input' AND other_address IS NULL
                """,
                (inferred_other, txid),
            )

        if len(input_addresses) == 1:
            sender = input_addresses[0]
            conn.execute(
                """
                UPDATE address_txs
                SET other_address = ?
                WHERE txid = ? AND role = 'output' AND other_address IS NULL AND address != ?
                """,
                (sender, txid, sender),
            )

    set_meta(conn, "last_indexed_height", str(height))
    conn.commit()


def run_indexer() -> None:
    conn = db_connect()
    try:
        init_db(conn)

        if STORE_RAW_JSON:
            print("[indexer] raw transaction JSON storage is ENABLED (EXPLORER_STORE_RAW_JSON=1)")
        else:
            print("[indexer] raw transaction JSON storage is DISABLED (default)")

        tip_height = rpc_request("getblockcount")
        stable_tip_height = max(-1, tip_height - TIP_CONFIRMATION_BUFFER)
        print(
            f"[indexer] chain tip={tip_height}, stable tip={stable_tip_height} "
            f"(confirmation buffer={TIP_CONFIRMATION_BUFFER})"
        )

        if stable_tip_height < 0:
            print("[indexer] chain too short for stable indexing yet")
            return

        start_height = check_reorg(conn, stable_tip_height)

        if start_height > stable_tip_height:
            print(f"[indexer] up to date at stable height {stable_tip_height}")
            return

        tx_cache: Dict[str, dict] = {}

        for height in range(start_height, stable_tip_height + 1):
            print(f"[indexer] indexing block {height}/{tip_height}")
            index_block(conn, height, tx_cache)

            # Keep only a modest in-memory cache. The local SQLite index is now the
            # primary fast path for prevout lookups, so we do not need a huge RPC cache.
            if len(tx_cache) > 500:
                tx_cache.clear()

        print(f"[indexer] finished at stable height {stable_tip_height} (chain tip {tip_height})")
    finally:
        conn.close()


if __name__ == "__main__":
    run_indexer()
