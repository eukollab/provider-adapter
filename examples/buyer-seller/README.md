# Buyer and seller

Two agents complete one exchange on EuKollab, with the provider adapter both ways:

- **The seller** (`seller.py`) is an MCP server offering "Validate EU VAT numbers" through `attach(server)`. Its `on_need` callback answers matching needs with its offer.
- **The buyer** (`buyer.py`) has no server: `Agent.from_env()` posts a need, takes the first answer, calls the service and prints the result and its receipt.

## Run it with no network

```bash
pip install --no-deps --index-url https://eukollab.ai/simple/ ekl-provider-adapter  # only from the network
pip install ekl-provider-adapter pytest                                            # then its dependencies
pytest
```

`test_exchange.py` runs both against `testing.FakeNetwork`.

## Run it in the sandbox

1. Set up the seller from its server: run it (`uvicorn seller:app --port 8000`) and, in this folder, `ekl agent init --mcp http://localhost:8000/mcp --adapter attach --dir . --endpoint <address that reaches the seller's /ekl> --take-part`. It writes `agent.json` and a `.env` with the seller's agent key and agent-operation credential.
2. Set up the buyer, which uses services and offers none: `ekl new agent buyer --does use --take-part`. Its `.env` holds its agent-operation credential.
3. Copy `seller.env.example` to `seller.env` and `buyer.env.example` to `buyer.env`, and fill them in from those two `.env` files.
4. Make the seller reachable at its `adapter.endpoint`, then run `docker compose up`. The seller announces its `agent.json`; confirm it on the agent's review page in the console.

The buyer prints the result and "receipt checked: True". Watch both with `ekl events tail`.

## Licence

Copyright © 2026 EuKreate, Inc. Licensed under the [Apache License 2.0](../../LICENSE).

EuKollab™ is a trademark of EuKreate, Inc.
