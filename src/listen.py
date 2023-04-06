import asyncio
from datetime import datetime
import json
import os

from azure.storage.blob.aio import BlobServiceClient
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import websockets

product_ids = ["BTC-USD", "ETH-USD", "ADA-USD"]
batch_size = 2_000

message_count = [0]
batch_count = [0]
dropped_count = [0]

subscribe_message = {
    "type": "subscribe",
    "product_ids": product_ids,
    "channels": ["level2"],
}

pd_columns = ["ts", "event", "product_id", "side", "price", "amount"]
# pd_dtypes = [
#     ("ts", "datetime64[ns]"),
#     ("event", "str"),
#     ("product_id", "str"),
#     ("side", "str"),
#     ("price", "float64"),
#     ("amount", "float64"),
# ]
# use dict
pd_dtypes = {
    # "ts": "datetime64[ns]",
    "event": "str",
    "product_id": "str",
    "side": "str",
    "price": "float64",
    "amount": "float64",
}

pa_schema = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns", tz="UTC")),
        pa.field("event", pa.string()),
        pa.field("product_id", pa.string()),
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
    ]
)


async def status_message(count):
    while True:
        message_count = count[0]
        await asyncio.sleep(1)
        messages_recieved = count[0] - message_count
        batch_size = batch_count[0]
        # dropped = dropped_count[0]
        # print(f"messages/s: {messages_recieved}, batch_size: {batch_size}, dropped: {dropped}")
        print(f"messages/s: {messages_recieved}, batch_size: {batch_size}")
        count[0] = 0


def get_fname():
    now = datetime.now()
    ts = now.strftime("%y-%m-%d_%H-%M-%S.%f")

    return f"cb_stream_{ts}.parquet"


def write_batch(batch, fname, path_data):
    print(f"Writing batch to {fname}")
    try:
        # print(batch[0:5])
        step = 0
        # df = pd.DataFrame.from_records(batch, columns=pd_columns)
        
        df = pd.DataFrame(batch, columns=pd_columns).astype(pd_dtypes)
        # df['ts'] = df['ts'].dt.tz_localize(None) 
        df['ts'] = pd.to_datetime(df['ts'], utc=True).dt.tz_localize(None) 
        print(f"df: {df.head()}, types: {df.dtypes}")
        step = 1
        # table = pa.Table.from_pylist(batch, schema=pa_schema)
        table = pa.Table.from_pandas(df, schema=pa_schema)
        # print(f"table: {table.head()}")
        step = 2
        abs_path = os.path.abspath(path_data)
        pq.write_table(table, f"{abs_path}/{fname}")
    except Exception as e:
        print(f"Error writing batch: {e}, step: {step}")


async def upload_batch(container_client, fname, path_data):
    try:
        blob_client = container_client.get_blob_client(fname)

        # Upload the file
        with open(f"{path_data}/{fname}", "rb") as data:
            await blob_client.upload_blob(data, overwrite=True)
    except Exception as e:
        print(f"Error uploading {fname}: {e}")


def process_message(message):
    output = []
    try:
        time = message["time"]
        event = message["type"]
        product_id = message["product_id"]
        side = message["changes"][0][0]
        price = message["changes"][0][1]
        amount = message["changes"][0][2]

        return [time, event, product_id, side, price, amount]

    except Exception as e:
        print(f"Error formatting message: {e}")


async def handle_messages(url, container_client, path_data):
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        asyncio.create_task(status_message(message_count))

        batch = []

        try:
            while True:
                messagestr = await websocket.recv()
                message = json.loads(messagestr)
                message_count[0] += 1
                # print(message)

                fmted_message = process_message(message)
                # print(fmted_message)

                if fmted_message is not None:
                    batch.append(fmted_message)
                    batch_count[0] += 1
                else:
                    dropped_count[0] += 1

                if batch_count[0] >= batch_size:
                    fname = get_fname()
                    write_batch(batch, fname, path_data)
                    await upload_batch(container_client, fname, path_data)
                    batch = []
                    batch_count[0] = 0

        except Exception as e:
            print("Error receiving message: {e}")


if __name__ == "__main__":
    load_dotenv()

    azure_url = os.environ.get("AZURE_URL")
    azure_access_key = os.getenv("AZURE_ACCESS_KEY")
    azure_conn_string = os.getenv("AZURE_CONN_STRING")
    ws_url = os.environ.get("WS_URL")
    blob_container = os.environ.get("BLOB_CONTAINER")
    path_data = os.environ.get("PATH_DATA")

    path_data = os.path.abspath(path_data)
    # credential = DefaultAzureCredential()
    credential = azure_access_key
    blob_service_client = BlobServiceClient(azure_url, credential=credential)
    container_client = blob_service_client.get_container_client(blob_container)
    # blob = BlobClient.from_connection_string(conn_str=azure_conn_string, container_name=blob_container, blob_name="my_blob")
    # BlobClient.from_blob_url(blob_url="https://myaccount.blob.core.windows.net/mycontainer/myblob")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(handle_messages(ws_url, container_client, path_data))
