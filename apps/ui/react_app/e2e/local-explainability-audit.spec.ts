import { expect, test } from "@playwright/test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { execFileSync } from "node:child_process";

test.use({ trace: "off" });

test("completed SHAP evidence exports readable charts and correct section pages", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_AUDIT_EXPORT || isMobile, "Export previously verified real SHAP evidence");
  test.setTimeout(2 * 60_000);
  page.setDefaultTimeout(30_000);
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  await page.goto("/projects/dc93e5cf-0e16-4fee-a546-5d00e0bfd030/runs");
  await page.getByRole("button", { name: /Worker pod acceptance 1790171741870/ }).click();
  await page.getByRole("button", { name: "DecisionTreeRegressor details", exact: true }).click();
  await page.getByRole("tab", { name: "Pipeline & features", exact: true }).click();
  const pending = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download PDF audit", exact: true }).click();
  const download = await pending;
  const path = testInfo.outputPath("DecisionTreeRegressor-audit.pdf");
  await download.saveAs(path);
  const pages = execFileSync("pdftotext", ["-raw", path, "-"], { encoding: "utf8" }).split("\f");
  const text = pages.join(" ").replace(/\s+/g, " ");
  for (const evidence of ["DecisionTreeRegressor", "actual minus predicted", "perfect agreement",
    "not a confidence interval", "Scorer:", process.env.LIVE_AUDIT_ANALYSIS_ID!]) expect(text).toContain(evidence);
  const performance = pages.findIndex(page => /^5\. Model performance$/m.test(page));
  expect(performance).toBeGreaterThan(4); // This real model has a multi-page preparation section.
  expect(pages[0].replace(/\s+/g, " ")).toContain(`5. Model performance ${performance + 1}`);
  writeFileSync(testInfo.outputPath("export-verification.json"), JSON.stringify({
    analysisId: process.env.LIVE_AUDIT_ANALYSIS_ID, performancePage: performance + 1,
  }));
});

test("fresh SHAP evidence and a readable audit export use the selected real model", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_EXPLANATION_AUDIT || isMobile, "Explicit local evidence run");
  test.setTimeout(25 * 60_000);
  page.setDefaultTimeout(30_000);
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  await page.goto("/projects/dc93e5cf-0e16-4fee-a546-5d00e0bfd030/runs");
  await page.getByRole("button", { name: /Worker pod acceptance 1790171741870/ }).click();
  await page.getByRole("tab", { name: "Validate & explain", exact: true }).click();
  await page.getByRole("combobox", { name: "Candidate model", exact: true }).selectOption("DecisionTreeRegressor");
  await page.getByRole("tab", { name: "SHAP explainability", exact: true }).click();
  await page.screenshot({ path: testInfo.outputPath("before-explanation.png"), fullPage: true });
  await page.getByLabel("Sample rows", { exact: true }).fill("20");
  const launchedIds: string[] = [];
  for (let attempt = 0; attempt < 2; attempt++) {
    const pending = page.waitForResponse(response => response.url().endsWith("/explanations")
      && response.request().method() === "POST");
    await page.getByRole("button", { name: /^(Calculate|Recalculate) SHAP$/ }).click();
    const response = await pending;
    expect(response.ok()).toBe(true);
    expect(response.request().postDataJSON()).toMatchObject({ model_name: "DecisionTreeRegressor", force: true, max_rows: 20 });
    const launched = await response.json();
    expect(launched.cached).toBe(false);
    launchedIds.push(launched.run.id);
    const result = page.locator(`.analysis-result[data-run-id="${launched.run.id}"]`);
    await expect(result).toBeVisible();
    await expect(page.getByRole("button", { name: "SHAP calculation underway", exact: true })).toBeDisabled();
    await expect(result.getByRole("heading", { name: "Feature contribution", exact: true })).toBeVisible({ timeout: 10 * 60_000 });
    await expect(result).toContainText("Sample: 20 rows.");
    await expect(result).toContainText("not cause and effect");
    await expect(page.getByRole("button", { name: "Recalculate SHAP", exact: true })).toBeEnabled();
    await page.screenshot({ path: testInfo.outputPath(`explanation-${attempt + 1}.png`), fullPage: true });
  }
  expect(new Set(launchedIds).size).toBe(2);
  writeFileSync(testInfo.outputPath("analysis-runs.json"), JSON.stringify(launchedIds));
  await page.getByRole("tab", { name: "Leaderboard", exact: true }).click();
  await page.getByRole("button", { name: "DecisionTreeRegressor details", exact: true }).click();
  await page.getByRole("tab", { name: "Pipeline & features", exact: true }).click();
  const downloadPending = page.waitForEvent("download", { timeout: 90_000 });
  await page.getByRole("button", { name: "Download PDF audit", exact: true }).click();
  const download = await downloadPending;
  const path = testInfo.outputPath("DecisionTreeRegressor-audit.pdf");
  await download.saveAs(path);
  const text = execFileSync("pdftotext", ["-raw", path, "-"], { encoding: "utf8" }).replace(/\s+/g, " ");
  for (const evidence of ["DecisionTreeRegressor", "actual minus predicted", "perfect agreement", "Training", "Validation", "not a confidence interval", "Scorer:"]) {
    expect(text).toContain(evidence);
  }
  expect(text).toContain(launchedIds[1]);
  expect(errors).toEqual([]);
});
