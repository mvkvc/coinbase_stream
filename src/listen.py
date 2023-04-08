import asyncio
from datetime import datetime
import json
import os
import warnings

from azure.storage.blob.aio import BlobServiceClient
import click
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sortedcollections import SortedDict
import websockets

message_count = [0]
batch_count = [0]


async def status_message(count):
    while True:
        message_count = count[0]
        await asyncio.sleep(1)
        messages_received = count[0] - message_count
        batch_size = batch_count[0]
        print(f"messages/s: {messages_received}, batch_size: {batch_size}")
        count[0] = 0


def get_fname():
    now = datetime.now()
    ts = now.strftime("%y-%m-%d_%H-%M-%S.%f")

    return f"coinbase_stream_{ts}.parquet"


def write_batch(batch, fname, path_data, pa_schema, pd_columns, pd_dtypes):
    print(f"Writing batch to {fname}")

    df = pd.DataFrame(batch, columns=pd_columns)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_localize(None)
    df = df.astype(pd_dtypes)

    table = pa.Table.from_pandas(df, schema=pa_schema)
    abs_path = os.path.abspath(path_data)
    pq.write_table(table, f"{abs_path}/{fname}")


async def upload_batch(container_client, fname, path_data):
    try:
        blob_client = container_client.get_blob_client(fname)

        with open(f"{path_data}/{fname}", "rb") as data:
            await blob_client.upload_blob(data, overwrite=True)

        print(f"Uploaded {fname}")
    except Exception as e:
        print(f"Error uploading {fname}: {e}")


def process_message(message, order_book, price_levels):
    ts = message["time"]
    product_id = message["product_id"]
    side = message["changes"][0][0]
    price = float(message["changes"][0][1])
    amount = float(message["changes"][0][2])

    order_book = update_book(order_book, product_id, side, price, amount)
    top_n = top_n_order_book(order_book, product_id, price_levels)

    return order_book, [ts, product_id] + top_n


def top_n_order_book(ob_dict, id, n):
    ask_prices = list(ob_dict[id]["asks"].keys())[:n]
    ask_volumes = list(ob_dict[id]["asks"].values())[:n]
    bid_prices = list(ob_dict[id]["bids"].keys())[-n:]
    bid_volumes = list(ob_dict[id]["bids"].values())[-n:]

    return ask_prices + ask_volumes + bid_prices + bid_volumes


def update_book(ob_dict, id, side, price, amount):
    if side == "sell":
        side = "asks"
    elif side == "buy":
        side = "bids"

    if amount == "0":
        if price in ob_dict[id][side]:
            del ob_dict[id][side][price]
    else:
        ob_dict[id][side][price] = amount

    return ob_dict


def create_book(ob_dict, message):
    id = message["product_id"]
    asks = message["asks"]
    bids = message["bids"]

    ob_dict[id] = {"asks": SortedDict(), "bids": SortedDict()}
    for ask in asks:
        ob_dict[id]["asks"][float(ask[0])] = float(ask[1])
    for bid in bids:
        ob_dict[id]["bids"][float(bid[0])] = float(bid[1])

    return ob_dict


async def handle_messages(
    url,
    container_client,
    path_data,
    subscribe_message,
    batch_size,
    price_levels,
    pa_schema,
    pd_columns,
    pd_dtypes,
    upload,
):
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        status_task = asyncio.create_task(status_message(message_count))

        batch = []
        order_book = {}

        try:
            while True:
                messagestr = await websocket.recv()
                message = json.loads(messagestr)

                if "changes" in message.keys():
                    message_count[0] += 1
                    order_book, fmted_message = process_message(
                        message, order_book, price_levels
                    )

                    batch.append(fmted_message)
                    batch_count[0] += 1

                    if batch_count[0] >= batch_size:
                        fname = get_fname()
                        write_batch(
                            batch, fname, path_data, pa_schema, pd_columns, pd_dtypes
                        )

                        if upload:
                            await upload_batch(container_client, fname, path_data)

                        batch = []
                        batch_count[0] = 0
                elif "asks" in message.keys():
                    order_book = create_book(order_book, message)
                    print(f"Book created for: {message['product_id']}")
                else:
                    pass
        except Exception as e:
            print(f"Error in handling message: {e}")


@click.command()
@click.option("--ws-url", show_default=True, default="wss://ws-feed.pro.coinbase.com")
@click.option("--product-ids", show_default=True, default="BTC-USD,ETH-USD,ADA-USD")
@click.option("--path-data", show_default=True, default="data")
@click.option("--batch-size", show_default=True, default=10_000)
@click.option("--price-levels", show_default=True, default=10)
@click.option("--upload", is_flag=True, show_default=True, default=False)
def main(ws_url, product_ids, path_data, batch_size, price_levels, upload):
    """
    Connects to the Coinbase websocket API to receive real-time market data, and writes the top N price level prices and amounts to Parquet files.

    Optionally, the Parquet file can be uploaded to Azure Blob Storage using the `--upload` flag which also deletes the file after upload. The following environment variables must be set in the local .env file: AZURE_URL, AZURE_ACCESS_KEY, and AZURE_BLOB_CONTAINER.
    """

    product_ids = product_ids.split(",")

    subscribe_message = {
        "type": "subscribe",
        "product_ids": product_ids,
        "channels": ["level2"],
    }

    pd_columns = (
        ["ts", "product_id"]
        + [f"ask_price{i}" for i in range(1, price_levels + 1)]
        + [f"ask_volume{i}" for i in range(1, price_levels + 1)]
        + [f"bid_price{i}" for i in range(1, price_levels + 1)]
        + [f"bid_volume{i}" for i in range(1, price_levels + 1)]
    )
    pd_dtypes = {
        "ts": "datetime64[ns]",
        "product_id": "str",
    }
    pd_dtypes.update({f"ask_price{i}": "float64" for i in range(1, price_levels + 1)})
    pd_dtypes.update({f"ask_volume{i}": "float64" for i in range(1, price_levels + 1)})
    pd_dtypes.update({f"bid_price{i}": "float64" for i in range(1, price_levels + 1)})
    pd_dtypes.update({f"bid_volume{i}": "float64" for i in range(1, price_levels + 1)})

    pa_fields = (
        [
            pa.field("ts", pa.timestamp("ns", tz="UTC")),
            pa.field("product_id", pa.string()),
        ]
        + [pa.field(f"ask_price{i}", pa.float64()) for i in range(1, price_levels + 1)]
        + [pa.field(f"ask_volume{i}", pa.float64()) for i in range(1, price_levels + 1)]
        + [pa.field(f"bid_price{i}", pa.float64()) for i in range(1, price_levels + 1)]
        + [pa.field(f"bid_volume{i}", pa.float64()) for i in range(1, price_levels + 1)]
    )
    pa_schema = pa.schema(pa_fields)

    load_dotenv()

    for env_var in ["AZURE_URL", "AZURE_ACCESS_KEY", "AZURE_BLOB_CONTAINER"]:
        if env_var not in os.environ:
            raise ValueError(f"Missing environment variable: {env_var}")

    azure_url = os.environ.get("AZURE_URL")
    azure_access_key = os.getenv("AZURE_ACCESS_KEY")
    blob_container = os.environ.get("AZURE_BLOB_CONTAINER")

    path_data = os.path.abspath(path_data)
    blob_service_client = BlobServiceClient(azure_url, credential=azure_access_key)
    container_client = blob_service_client.get_container_client(blob_container)

    warnings.filterwarnings("ignore", category=DeprecationWarning)

    try:
        asyncio.run(
            handle_messages(
                ws_url,
                container_client,
                path_data,
                subscribe_message,
                batch_size,
                price_levels,
                pa_schema,
                pd_columns,
                pd_dtypes,
                upload,
            )
        )
    except Exception as e:
        print(f"Error in main: {e}")


if __name__ == "__main__":
    main()
