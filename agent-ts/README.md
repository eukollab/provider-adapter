# @eukollab/agent

EuKollab for apps written in TypeScript or JavaScript. The connector (`ekl agent run` starts it) runs beside your app
and does the network's work; this client wraps its loopback API and, while you design and test, answers from the
stand-ins `ekl try --record` saved in `eukollab/fixtures/`.

```ts
import { createAgent } from "@eukollab/agent";

const agent = createAgent();
const vat = agent.service("validate EU VAT numbers", {
  prefer: "@vat-check/validate",                // what `ekl find` suggested; the network may still choose another
  sample: { vat_number: "DE123456789" },
  expect: { type: "object", required: ["valid"] },
});
const result = await vat.call({ vat_number: invoice.vatNumber });
```

- Without `EKL_AGENT_CREDENTIAL`, in your app or in the connector beside it, `call` answers from the stand-in and
  says so (`result.standIn`); so it does with no connector running.
- Once either holds it, the connector chooses a provider with trials on the first call (trying `prefer` first when
  your rules allow it) and reuses the choice. A stand-in is never used then: a connector that isn't answering is an
  error.
- `createServer(callbacks(onEvent))` receives the connector's callbacks at `EKL_CALLBACK_URL`, signed with
  `EKL_CALLBACK_KEY` (callback v2; `ekl agent run` writes the key to `.env`, so the app never holds the credential).
  The signature is checked before anything else; each event is kept 7 days in `.ekl/callbacks/`, so a retry gets the
  answer already given and `onEvent` runs once; every answer is signed. `receiver(onEvent)` does the same for any
  framework, from the raw body and headers. "Once" covers the answer and `ctx.write(key, value)`: key anything else
  `onEvent` does (a paid call, an email) by `ctx.eventId`, e.g. an idempotency key `"call-" + ctx.eventId`.
- The client finds the connector's local API at `EKL_AGENT_API_URL`, else in `.ekl/local-api.json`, which the connector
  writes once it listens (on another port when 8765 is taken), and sends `EKL_LOCAL_TOKEN` with every call.
- `result.safetyFlags` says what screening on this machine found in a result: a category, never the content.
- `eukollab({...})`, as a TypeScript MCP server's experimental capabilities, and `service({...})`, as a tool's
  `_meta`, declare what MCP can't say (plans, data handling, a tool's class); `ekl agent sync` writes them into
  agent.json, as from a Python server.
- `testing.fakeNetwork()` answers from the stand-ins in unit tests.

Install it from your network's Downloads by its address and SHA-512 integrity, never by name from a registry:
`ekl new agent <folder> --lang typescript` writes it into `package.json` for you.

No dependencies; Node 20 or later, and Deno.

## Licence

Copyright © 2026 EuKreate, Inc. Licensed under the [Apache License 2.0](LICENSE).

EuKollab™ is a trademark of EuKreate, Inc.
