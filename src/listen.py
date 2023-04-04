import asyncio
import websockets
import json
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import os

load_dotenv()

event_hubs_connection_string = os.environ.get("EVENT_HUBS_CONNECTION_STRING")
event_hub_name = os.environ.get("EVENT_HUB_NAME")
ws_url = os.environ.get("WS_URL")

product_ids = ["BTC-USD", "ETH-USD", "ADA-USD"]
message_count = 0

subscribe_message = {
    "type": "subscribe",
    "product_ids": product_ids,
    "channels": [
        "level2",
        "heartbeat",
        {
            "name": "ticker",
            "product_ids": product_ids
        }
    ]
}

async def messages_per_second(count):
    while True:
        print(f"Messages per second: {count}")
        messages = count[0]
        await asyncio.sleep(1)
        messages_recieved = count[0] - messages
        print(f"Messages received in the last second: {messages_received}")

async def send_message_to_eventhub(message):
    producer = EventHubProducerClient.from_connection_string(event_hubs_connection_string, event_hub_name)
    event_data = EventData(json.dumps(message))
    async with producer:
        await producer.send_batch([event_data])

async def handle_messages():
    async with websockets.connect("wss://ws-feed.pro.coinbase.com") as websocket:
        await websocket.send(json.dumps(subscribe_message))

        try:
            while True:
                message = await websocket.recv()
                print("Received message:")
                print(message)
                # Send message to Azure Event Hub
                # asyncio.create_task(send_message_to_eventhub(message))
        except:
            print("Error receiving message")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handle_messages())
