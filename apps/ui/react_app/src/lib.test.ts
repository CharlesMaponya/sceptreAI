import { describe, expect, it } from "vitest";
import { cx, formatBytes, initials, titleCase } from "./lib";

describe("presentation helpers", () => {
  it("formats API names for people", () => {
    expect(titleCase("precheck_running")).toBe("Precheck Running");
    expect(titleCase("time_series")).toBe("Time Series");
  });

  it("formats file sizes at stable boundaries", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(1024)).toBe("1.0 KB");
    expect(formatBytes(1024 * 1024 * 2.5)).toBe("2.5 MB");
    expect(formatBytes(null)).toBe("—");
  });

  it("creates concise initials and class names", () => {
    expect(initials("Ada Lovelace", "ada@example.com")).toBe("AL");
    expect(initials(null, "user@example.com")).toBe("UE");
    expect(cx("card", false, undefined, "active")).toBe("card active");
  });
});
