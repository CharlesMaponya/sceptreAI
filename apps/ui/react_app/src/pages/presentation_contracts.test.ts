import { describe, expect, it } from "vitest";
import {
  buildWordCloudTrace, fallbackPipeline, fallbackPipelineDiagram, formatDuration, formatMetric,
  formatNumber, formatStatistic, formatTargetStatistic, inferPreviewTask, previewColumnProfile,
  previewTaskRationale,
} from "./presentation";

describe("qualification presentation contracts", () => {
  it.each([
    [null, undefined, "clustering"],
    ["observed_at", { semantic_type: "temporal" }, "time_series"],
    ["revenue", { semantic_type: "numerical_continuous" }, "regression"],
    ["status", { semantic_type: "categorical" }, "classification"],
  ])("infers %s as %s", (target, column, expected) => {
    expect(inferPreviewTask(target, column as never)).toBe(expected);
  });

  it.each([
    ["clustering", null, "unsupervised clustering"],
    ["regression", "revenue", "continuous numeric"],
    ["time_series", "observed_at", "temporal"],
    ["classification", "status", "categorical"],
  ])("explains %s previews", (task, target, expected) => {
    expect(previewTaskRationale(task as never, target)).toContain(expected);
  });

  it("normalizes numeric, temporal, and categorical preview columns", () => {
    expect(previewColumnProfile({
      name: "amount", semantic_type: "numerical_continuous", sample_values: ["2", "bad", "3"],
    } as never).preview_values).toEqual([2, 3]);
    expect(previewColumnProfile({
      name: "date", semantic_type: "temporal", sample_values: ["2026-01-01"],
    } as never).preview_values).toEqual(["2026-01-01"]);
    expect(previewColumnProfile({
      name: "status", semantic_type: "categorical", sample_values: ["open", "open", "closed"],
    } as never).preview_distribution).toEqual([
      { label: "open", count: 2 }, { label: "closed", count: 1 },
    ]);
  });

  it.each([
    [null, "—"], [1234.56789, "1,234.568"], ["2026-01-01", "2026-01-01"],
  ])("formats target statistics", (value, expected) => {
    expect(formatTargetStatistic(value)).toBe(expected);
  });

  it.each([
    [null, "—"], [1234.56789, "1,234.5679"], ["stable", "stable"],
  ])("formats profile statistics", (value, expected) => {
    expect(formatStatistic(value)).toBe(expected);
  });

  it.each([
    ["accuracy", .923, "92.3%"],
    ["drift_share", .12, "12.0%"],
    ["p95_latency_ms", 42.4, "42 ms"],
    ["sample_count", 1234.56789, "1,234.5679"],
  ])("formats monitoring metric %s", (name, value, expected) => {
    expect(formatMetric(name, value)).toBe(expected);
  });

  it("bounds and formats resource evidence", () => {
    expect(formatNumber(null, " cores")).toBe("—");
    expect(formatNumber(2.5, " cores")).toBe("2.5 cores");
    expect(formatDuration(9)).toBe("9s");
    expect(formatDuration(125)).toBe("2m 5s");
  });

  it.each([
    ["succeeded", "regression", "completed", "Task-aware holdout and cross-validation."],
    ["running", "time_series", "running", "Ordered holdout and time-series folds."],
    ["failed", "clustering", "planned", "Remove correlated numeric features using completeness."],
  ])("builds fallback stages for %s %s runs", (status, task, expectedStatus, expectedSummary) => {
    const stages = fallbackPipeline({ status, model: "Estimator" } as never, task as never);
    expect(stages.some((stage) => stage.status === expectedStatus)).toBe(true);
    expect(stages.some((stage) => stage.summary === expectedSummary)).toBe(true);
  });

  it("omits supervised selection from a clustering fallback diagram", () => {
    const clustering = fallbackPipelineDiagram({ model: "KMeans" } as never, "clustering");
    const classification = fallbackPipelineDiagram({ model: "LogisticRegression" } as never, "classification");
    expect(clustering.selector).toBeNull();
    expect(classification.selector).toMatchObject({ type: "SelectPercentile" });
  });

  it("filters invalid word-cloud evidence and places equal-frequency words", () => {
    const trace = buildWordCloudTrace([
      { word: "retention", count: 4 }, { word: "service", count: 4 },
      { word: "", count: 9 }, { word: "invalid", count: 0 },
    ]);
    expect(trace.text).toEqual(expect.arrayContaining(["retention", "service"]));
    expect(trace.text).not.toContain("invalid");
    expect(trace.textfont.size.every((size) => Number.isFinite(size))).toBe(true);
  });
});
