#! /bin/bash

python src/listen.py --product-ids BTC-USD,ETH-USD,MATIC-USD --batch-size 100_000 --upload --slack
