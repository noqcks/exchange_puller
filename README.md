# Exchange Data

This pulls exchange data and pushes to Google Big Query.

## Info
### Exchanges

- Bitfinex
- Bitmex

### Types

- Order Book
- Trade Data

## Setup

**NOTE**: This requires creds.json locally when building the Docker container. This json file
containes your Google cloud service account creds.

To build the Docker container:

```
docker build -t puller .
```

To run the Docker container:

```
docker run --env-file=./.env puller bitfinex_orderbook.py
```

This will pull order book data for bitfinex and push to Big Query.
