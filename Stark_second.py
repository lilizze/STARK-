import asyncio
import json
import os
import urllib.request
import logging
import time
from datetime import datetime
from typing import Optional

import aiosqlite
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from starknet_py.contract import Contract
from starknet_py.net.account.account import Account
from starknet_py.net.client_errors import ClientError, ContractNotFoundError
from starknet_py.net.client_models import TransactionExecutionStatus
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.net.models.chains import StarknetChainId
from starknet_py.net.signer.key_pair import KeyPair
from starknet_py.net.models.transaction import InvokeV3

# --- КОНФИГУРАЦИЯ ---
DEVNET_URL = "http://127.0.0.1:5050"
RPC_URL = DEVNET_URL + "/rpc"
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "0x01efe22c44b3101e88462499382f653fc8d3992771891e10af472bb9d31eea3a")
READ_FUNCTION = "get_balance"
WRITE_FUNCTION = "increase_balance"
WRITE_ARGS = [1]
DB_PATH = "tasks.db"

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    handlers=[logging.FileHandler("runner.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT UNIQUE,
                status TEXT,
                tx_hash TEXT,
                error_message TEXT,
                updated_at TEXT
            )
        """)
        await db.commit()

async def is_task_done_today() -> bool:
    today = datetime.now().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT status FROM task_runs WHERE run_date = ?", (today,)) as cursor:
            row = await cursor.fetchone()
            return row is not None and row[0] == "SUCCESS"

async def update_task_status(status: str, tx_hash: Optional[str] = None, error: Optional[str] = None):
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO task_runs (run_date, status, tx_hash, error_message, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(run_date) DO UPDATE SET
                status=excluded.status,
                tx_hash=excluded.tx_hash,
                error_message=excluded.error_message,
                updated_at=excluded.updated_at
        """, (today, status, tx_hash, error, now))
        await db.commit()


def get_devnet_account_data() -> tuple[int, int]:
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "devnet_getPredeployedAccounts", "params": [],
    }).encode()
    req = urllib.request.Request(RPC_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        accounts = json.loads(r.read())["result"]
    first = accounts[0]
    return int(first["address"], 16), int(first["private_key"], 16)

async def execute_starknet_tx():
    client = FullNodeClient(node_url=RPC_URL)
    address, private_key = get_devnet_account_data()
    account = Account(
        address=address, client=client,
        key_pair=KeyPair.from_private_key(private_key),
        chain=StarknetChainId.SEPOLIA
    )
    contract = await Contract.from_address(address=CONTRACT_ADDRESS, provider=account)
    
    before = await contract.functions[READ_FUNCTION].call()
    logger.info(f"Balance before: {before}")

    invoke_result = await contract.functions[WRITE_FUNCTION].invoke_v3(
        *WRITE_ARGS,
        auto_estimate=True,
    )
    
    tx_hash = hex(invoke_result.hash)
    logger.info(f"Tx sent: {tx_hash}")
    
    await invoke_result.wait_for_acceptance()
    
    receipt = await client.get_transaction_receipt(invoke_result.hash)
    
    if receipt.execution_status != TransactionExecutionStatus.SUCCEEDED:
        raise Exception(f"Tx reverted: {receipt.execution_status}")
        
    logger.info(f"Tx fee paid: {receipt.actual_fee}")
    
    after = await contract.functions[READ_FUNCTION].call()
    logger.info(f"Balance after: {after}")
    
    return tx_hash


async def daily_job_with_retries(max_retries=3):
    if await is_task_done_today():
        logger.info("Task already completed today. Skipping (Idempotency).")
        return

    logger.info("Starting daily task...")
    await update_task_status("IN_PROGRESS")

    for attempt in range(1, max_retries + 1):
        try:
            tx_hash = await execute_starknet_tx()
            await update_task_status("SUCCESS", tx_hash=tx_hash)
            logger.info("Task completed successfully.")
            return
            
        except ClientError as e:
            error_msg = str(e)
            logger.warning(f"Attempt {attempt}/{max_retries} failed (ClientError): {error_msg}")
            if "429" in error_msg:
                logger.warning("Rate limited by RPC. Sleeping for 10 seconds...")
                await asyncio.sleep(10)
            else:
                await asyncio.sleep(2 ** attempt) 
                
        except Exception as e:
            logger.error(f"Attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                await update_task_status("FAILED", error=str(e))
                logger.critical("All retries exhausted. Task failed.")
            else:
                await asyncio.sleep(2 ** attempt)


# --- 4. ЗАПУСК ---
async def main():
    await init_db()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_job_with_retries, CronTrigger(hour=12, minute=0))

    scheduler.start()
    logger.info("Scheduler started. Waiting for tasks...")
    
    try:
        await asyncio.Event().wait() 
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    asyncio.run(main())