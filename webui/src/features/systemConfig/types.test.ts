import { formatNodeDNSUpstreamsForDisplay } from "./types.ts";

const assert = {
  deepEqual(actual: unknown, expected: unknown, message: string) {
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw new Error(`${message}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
    }
  },
};

assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(["https://dns.example.com", "local"], [true, false]),
  { lines: ["https://dns.example.com", "local"], hasRedacted: true },
  "redacted origin summaries retain an explicit marker",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(["https://dns.example.com"], null),
  { lines: ["https://dns.example.com"], hasRedacted: true },
  "missing flags fail closed instead of claiming a value is not redacted",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(["https://dns.example.com", "local"], [false]),
  { lines: ["https://dns.example.com", "local"], hasRedacted: true },
  "short flag arrays fail closed instead of shifting redaction to another item",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(["https://dns.example.com"], [false, false]),
  { lines: ["https://dns.example.com"], hasRedacted: true },
  "long flag arrays fail closed instead of dropping metadata",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(
    ["https://dns.example.com"],
    [null] as unknown as boolean[],
  ),
  { lines: ["https://dns.example.com"], hasRedacted: true },
  "non-boolean flags fail closed at the runtime boundary",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay([], []),
  { lines: [], hasRedacted: false },
  "empty upstream lists remain empty",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(null, [true]),
  { lines: [], hasRedacted: false },
  "orphan flags cannot create a displayed redaction",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(["local"], null),
  { lines: ["local"], hasRedacted: true },
  "local values still warn when metadata is missing",
);
assert.deepEqual(
  formatNodeDNSUpstreamsForDisplay(["local"], []),
  { lines: ["local"], hasRedacted: true },
  "local values still warn when metadata is misaligned",
);

console.log("system config type contracts passed");
