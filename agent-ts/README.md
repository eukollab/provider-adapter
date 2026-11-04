# @eukollab/agent

EuKollab for apps written in TypeScript or JavaScript. The connector (`ekl-provider-connector`) runs beside your app
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
- `handle(rawBody, request.headers["x-ekl-signature"])` checks a callback from the connector and returns its event.
- `testing.fakeNetwork()` answers from the stand-ins in unit tests.

Install it from your network's Downloads by its address and SHA-512 integrity, never by name from a registry:
`ekl new agent <folder> --lang typescript` writes it into `package.json` for you.

No dependencies; Node 20 or later, and Deno.

## Licence

Copyright © 2026 EuKreate, Inc. Licensed under the [Apache License 2.0](LICENSE).

EuKollab™ is a trademark of EuKreate, Inc.
