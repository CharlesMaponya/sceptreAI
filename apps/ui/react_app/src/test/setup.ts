import "@testing-library/jest-dom/vitest";
import { afterEach } from "vitest";
import { cleanup } from "@testing-library/react";
import { expect } from "vitest";
import { toHaveNoViolations } from "jest-axe";

expect.extend(toHaveNoViolations);

afterEach(() => cleanup());
