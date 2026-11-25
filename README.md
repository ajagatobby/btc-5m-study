# Bitcoin 5-minute markets study

Records Polymarket's "Bitcoin Up or Down" 5-minute markets next to the price they settle on
(Chainlink BTC/USD), plus Binance and Coinbase, for one week. The question: after fees, is a
market's price ever meaningfully away from a fair price worked out from the live Bitcoin price,
the window's opening price and the time left?

Read-only: public feeds, no API keys, no orders. Runs on GitHub Actions in overlapping shifts
(`.github/workflows/record.yml`); each shift's data is attached to its run as an artifact.

```
STUDY_MINUTES=10 node --experimental-strip-types record.ts   # local test
```
