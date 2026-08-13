import "@testing-library/jest-dom/vitest";
import { afterEach } from "vitest";
import { cleanup, configure } from "@testing-library/react";
import { expect } from "vitest";
import { toHaveNoViolations } from "jest-axe";

expect.extend(toHaveNoViolations);
configure({ asyncUtilTimeout: 5_000 });

// Plotly probes canvas support during import. JSDOM intentionally has no canvas
// implementation, so make the unsupported result quiet and deterministic.
Object.defineProperty(HTMLCanvasElement.prototype, "getContext", {
  configurable: true,
  value: () => null,
});

afterEach(() => cleanup());
