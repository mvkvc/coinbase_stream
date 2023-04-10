import asyncio
import atexit
from datetime import datetime
from itertools import islice
import json
import os

from azure.storage.blob.aio import BlobServiceClient
import click
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sortedcollections import SortedDict
import websockets

# TODO: Add logging
# TODO: Remove global variables if possible

message_count = [0]
batch_count = [0]


def send_slack_message(client, channel, text):
    try:
        response = client.chat_postMessage(channel=channel, text=text)
        print(f"Slack message sent: {response['message']['text']}")
    except SlackApiError as e:
        print("Error sending Slack message:", e)


async def status_message(message_count, batch_count):
    while True:
        messages_initial = message_count[0]
        await asyncio.sleep(1)
        messages_received = message_count[0] - messages_initial
        batch_size = batch_count[0]
        print(f"messages/s: {messages_received}, batch_size: {batch_size}")
        message_count[0] = 0


def get_fname():
    now = datetime.now()
    ts = now.strftime("%y-%m-%d_%H-%M-%S.%f")

    return f"coinbase_stream_{ts}.parquet"


def write_batch(batch, fname, path_data, pa_schema, pd_columns):
    for index, value in enumerate(batch):
        batch[index] = pa.array(value)

    table = pa.Table.from_arrays(batch, schema=pa_schema)

    abs_path = os.path.abspath(path_data)
    print(f"Writing batch to {fname}")
    pq.write_table(table, f"{abs_path}/{fname}")


async def upload_batch(container_client, fname, path_data):
    blob_client = container_client.get_blob_client(fname)

    with open(f"{path_data}/{fname}", "rb") as data:
        print(f"Uploading {fname}")
        await blob_client.upload_blob(data, overwrite=True)


def process_message(message, order_book, side_map, price_levels):
    ts = message["time"]
    product_id = message["product_id"]
    side = message["changes"][0][0]
    price = float(message["changes"][0][1])
    amount = float(message["changes"][0][2])

    book = book_update(order_book, side_map, product_id, side, price, amount)
    top_n = book_top_n(order_book, product_id, price_levels)

    return book, [ts, product_id] + top_n


def book_top_n(ob_dict, id, n):
    asks = ob_dict[id]["asks"]
    bids = ob_dict[id]["bids"]

    ask_prices, ask_volumes = zip(*islice(asks.items(), n))
    bid_prices, bid_volumes = zip(*islice(reversed(bids.items()), n))

    return list(ask_prices) + list(ask_volumes) + list(bid_prices) + list(bid_volumes)


def book_update(book_dict, side_map, id, side, price, amount):
    side = side_map.get(side)

    if amount == 0:
        book_dict[id][side].pop(price, None)
    else:
        book_dict[id][side][price] = amount

    return book_dict


def create_book(book_dict, message):
    id = message["product_id"]
    asks = message["asks"]
    bids = message["bids"]

    book_dict[id] = {
        "asks": SortedDict({float(ask[0]): float(ask[1]) for ask in asks}),
        "bids": SortedDict({float(bid[0]): float(bid[1]) for bid in bids}),
    }

    return book_dict


def reset_batch(price_levels):
    return [[] for _ in range(price_levels * 4 + 2)]


async def handle_messages(
    url,
    container_client,
    path_data,
    subscribe_message,
    batch_size,
    price_levels,
    pa_schema,
    pd_columns,
    delete,
):
    side_map = {"buy": "bids", "sell": "asks"}
    order_book = {}
    batch = reset_batch(price_levels)

    async with websockets.connect(url, ping_timeout=None) as websocket:
        asyncio.create_task(status_message(message_count, batch_count))
        await websocket.send(json.dumps(subscribe_message))

        try:
            while True:
                messagestr = await websocket.recv()
                message = json.loads(messagestr)

                if "changes" in message.keys():
                    message_count[0] += 1
                    order_book, fmted_message = process_message(
                        message, order_book, side_map, price_levels
                    )

                    for b, m in zip(batch, fmted_message):
                        b.append(m)

                    # batch.append(fmted_message)
                    batch_count[0] += 1

                    if batch_count[0] >= batch_size:
                        fname = get_fname()
                        write_batch(batch, fname, path_data, pa_schema, pd_columns)

                        if container_client:
                            await upload_batch(container_client, fname, path_data)
                            if delete:
                                os.remove(f"{path_data}/{fname}")

                        batch = reset_batch(price_levels)
                        batch_count[0] = 0
                elif "asks" in message.keys():
                    print(f"Creating book for: {message['product_id']}")
                    order_book = create_book(order_book, message)
        except Exception as e:
            print(f"Error handling message: {e}")


@click.command()
@click.option(
    "--ws-url",
    show_default=True,
    default="wss://ws-feed.pro.coinbase.com",
    help="Websocket URL",
)
@click.option(
    "--product-ids",
    show_default=True,
    default="BTC-USD,ETH-USD,ADA-USD",
    help="Comma-separated list of product IDs",
)
@click.option(
    "--path-data",
    show_default=True,
    default="data",
    help="Relative path to write Parquet files",
)
@click.option(
    "--batch-size",
    show_default=True,
    default=100_000,
    help="Number of messages to write to Parquet file before uploading",
)
@click.option(
    "--price-levels",
    show_default=True,
    default=20,
    help="Number of price levels to write to Parquet file",
)
@click.option(
    "--upload",
    is_flag=True,
    show_default=True,
    default=False,
    help="Upload Parquet files to Azure Blob Storage",
)
@click.option(
    "--delete",
    is_flag=True,
    show_default=True,
    default=False,
    help="Delete Parquet files after uploading to Azure Blob Storage",
)
@click.option(
    "--slack",
    is_flag=True,
    show_default=True,
    default=False,
    help="Send message to Slack when script exits",
)
def main(
    ws_url, product_ids, path_data, batch_size, price_levels, upload, slack, delete
):
    """
    Connects to the Coinbase websocket API to receive real-time market data, and writes the top N price level prices and amounts to Parquet files.

    The `--upload` flag enables uploading the Parquet file to Azure Blob Storage. The following environment variables must be set in the local `.env` file: AZURE_URL, AZURE_ACCESS_KEY, and AZURE_BLOB_CONTAINER.

    The `--slack` flag will send a message to a provided Slack channel when the script is exiting. The following environment variables must be set in the local `.env` file: SLACK_TOKEN, SLACK_CHANNEL.
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

    container_client = None
    if upload:
        for env_var in ["AZURE_URL", "AZURE_ACCESS_KEY", "AZURE_BLOB_CONTAINER"]:
            if env_var not in os.environ:
                raise ValueError(f"Missing environment variable: {env_var}")

        azure_url = os.environ.get("AZURE_URL")
        azure_access_key = os.getenv("AZURE_ACCESS_KEY")
        blob_container = os.environ.get("AZURE_BLOB_CONTAINER")

        path_data = os.path.abspath(path_data)
        blob_service_client = BlobServiceClient(azure_url, credential=azure_access_key)
        container_client = blob_service_client.get_container_client(blob_container)

    if slack:
        for env_var in ["SLACK_TOKEN", "SLACK_CHANNEL"]:
            if env_var not in os.environ:
                raise ValueError(f"Missing environment variable: {env_var}")

        slack_token = os.environ.get("SLACK_TOKEN")
        slack_channel = os.environ.get("SLACK_CHANNEL")

        client = WebClient(token=slack_token)

        def send_exit_message():
            send_slack_message(
                client, slack_channel, "Exiting Coinbase websocket script"
            )

        atexit.register(send_exit_message)

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
            delete,
        )
    )


if __name__ == "__main__":
    main()
