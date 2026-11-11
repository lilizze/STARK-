import asyncio
import json
import os
import urllib.request

from starknet_py.contract import Contract
from starknet_py.net.account.account import Account
from starknet_py.net.client_errors import ClientError, ContractNotFoundError
from starknet_py.net.client_models import TransactionExecutionStatus
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.net.models.chains import StarknetChainId
from starknet_py.net.signer.key_pair import KeyPair


DEVNET_URL = "http://127.0.0.1:5050"
RPC_URL = DEVNET_URL + "/rpc"

CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "0x0375a18592a1ded9dbaedf0837e7378978330a785a52b8432f89a76b6369f5cc")

READ_FUNCTION = "get_balance"        
WRITE_FUNCTION = "increase_balance"  
WRITE_ARGS = [1]                     


def get_devnet_account_data() -> tuple[int, int]:
    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "devnet_getPredeployedAccounts",
        "params": [],
    }).encode()
    req = urllib.request.Request(
        RPC_URL, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        accounts = json.loads(r.read())["result"]
    first = accounts[0]
    return int(first["address"], 16), int(first["private_key"], 16)


async def main():
    if "ВСТАВЬ" in CONTRACT_ADDRESS:
        raise SystemExit("Укажи реальный CONTRACT_ADDRESS в настройках или в переменной окружения.")

    client = FullNodeClient(node_url=RPC_URL)
    print("Номер блока:", await client.get_block_number())

    address, private_key = get_devnet_account_data()
    account = Account(
        address=address,
        client=client,
        key_pair=KeyPair.from_private_key(private_key),
        chain=StarknetChainId.SEPOLIA,  
    )
    print("Аккаунт:", hex(address))


    contract = await Contract.from_address(address=CONTRACT_ADDRESS, provider=account)
    print("\nФункции контракта (из ABI):")
    for name in contract.functions:
        print(" -", name)

    for name in (READ_FUNCTION, WRITE_FUNCTION):
        if name not in contract.functions:
            raise SystemExit(f"В ABI нет функции '{name}'. Поправь READ_FUNCTION/WRITE_FUNCTION.")


    before = await contract.functions[READ_FUNCTION].call()
    print(f"\n{READ_FUNCTION}() до записи:", before)


    invoke_result = await contract.functions[WRITE_FUNCTION].invoke_v3(
        *WRITE_ARGS,
        auto_estimate=True,
    )
    print("Хеш транзакции:", hex(invoke_result.hash))
    await invoke_result.wait_for_acceptance()


    receipt = await client.get_transaction_receipt(invoke_result.hash)
    print("\nReceipt:")
    print(" статус исполнения:", receipt.execution_status)
    print(" статус финализации:", receipt.finality_status)
    print(" комиссия:", receipt.actual_fee)

    if receipt.execution_status != TransactionExecutionStatus.SUCCEEDED:
        raise SystemExit(f"Транзакция откатилась: {receipt.execution_status}")

    after = await contract.functions[READ_FUNCTION].call()
    print(f"\n{READ_FUNCTION}() после записи:", after)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ContractNotFoundError:
        print("Контракт не найден: проверь адрес и не перезапускался ли devnet "
              "(после перезапуска контракт нужно деплоить заново).")
    except ClientError as e:
        print("Ошибка узла:", e)
    except OSError as e:
        print("Нет соединения с devnet (запущен ли он на порту 5050?):", e)
