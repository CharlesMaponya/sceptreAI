import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";

test("trained taxi model registers, serves a file, and explains through the UI", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY, "requires the rebuilt local cluster");
  test.skip(isMobile, "exercise one real model deployment");
  test.setTimeout(90 * 60_000);
  page.setDefaultTimeout(60_000);
  const directory = process.env.RECOVERY_STATE_DIR;
  if (!directory) throw new Error("RECOVERY_STATE_DIR is required");
  const state = JSON.parse(readFileSync(join(directory, "browser-state.json"), "utf8"));
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  page.on("response", response => {
    if (response.status() >= 400) console.log(`HTTP ${response.status()} ${new URL(response.url()).pathname}`);
  });
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/, { timeout: 30_000 });
  await page.goto(`${state.projectUrl}/runs`);
  const run = page.getByRole("button", { name: /Taxi total amount without fare leakage/ });
  await expect.poll(async () => {
    await page.reload();
    await expect(run.first()).toBeVisible({ timeout: 30_000 });
    return (await run.first().innerText()).includes("Succeeded");
  }, { timeout: 60 * 60_000, intervals: [15_000] }).toBe(true);
  await run.first().click();
  await page.screenshot({ path: testInfo.outputPath("05-results.png"), fullPage: true });
  await page.goto(`${state.projectUrl}/operations`);
  await expect(page.getByRole("heading", { name: "Deploy & monitor", exact: true })).toBeVisible();
  if (await page.locator(".registry-card").count() === 0) {
    await page.goto(`${state.projectUrl}/runs`);
    await page.getByRole("link", { name: "Deploy model", exact: true }).click();
    const register = page.getByRole("dialog", { name: "Register a trained model" });
    await expect(register).toBeVisible();
    await register.getByRole("button", { name: "Register model", exact: true }).click();
    await expect(register).toBeHidden({ timeout: 30_000 });
    console.log("Model registered through browser");
  }
  if (await page.getByRole("button", { name: "API access", exact: true }).count() === 0) {
    const card = page.locator(".registry-card").first();
    await card.getByRole("combobox").selectOption("staging");
    const update = card.getByRole("button", { name: "Update stage", exact: true });
    if (await update.isEnabled()) await update.click();
    await expect(card.getByRole("button", { name: "Deploy", exact: true })).toBeEnabled();
    await card.getByRole("button", { name: "Deploy", exact: true }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Deploy model", exact: true }).click();
  }
  await expect(page.getByRole("button", { name: "API access", exact: true }).first()).toBeVisible({ timeout: 15 * 60_000 });
  console.log("Inference deployment ready in browser");
  await page.screenshot({ path: testInfo.outputPath("06-deployed.png"), fullPage: true });
  await page.getByRole("button", { name: "API access", exact: true }).first().click();
  await page.getByRole("dialog").locator('input[type="file"]').setInputFiles(
    resolve("../../../artifacts/local-recovery/scoring.csv"),
  );
  const downloadEvent = page.waitForEvent("download", { timeout: 120_000 });
  await page.getByRole("button", { name: "Upload and predict", exact: true }).click();
  const download = await downloadEvent;
  const destination = testInfo.outputPath("predictions.csv");
  await download.saveAs(destination);
  const lines = readFileSync(destination, "utf8").trim().split(/\r?\n/);
  expect(lines).toHaveLength(21);
  expect(lines[0].toLowerCase()).toContain("prediction");
  await page.screenshot({ path: testInfo.outputPath("07-predicted.png"), fullPage: true });
  await page.goto(`${state.projectUrl}/runs`);
  const [historyResponse] = await Promise.all([
    page.waitForResponse(response => response.url().endsWith("/analyses")
      && response.request().method() === "GET"),
    page.getByRole("tab", { name: "Validate & explain", exact: true }).click(),
  ]);
  expect(historyResponse.ok()).toBe(true);
  const history = await historyResponse.json();
  const candidate = await page.getByRole("combobox", { name: "Candidate model", exact: true }).inputValue();
  await page.getByRole("tab", { name: "SHAP explainability", exact: true }).click();
  if (!history.some((item: { run_kind: string; status: string; params: { model_name: string } }) =>
    item.run_kind === "explainability" && item.status === "succeeded"
    && item.params.model_name === candidate)) {
    await page.getByLabel("Sample rows", { exact: true }).fill("20");
    await page.getByRole("button", { name: "Calculate SHAP", exact: true }).click();
  }
  await expect(page.getByRole("heading", { name: "Feature contribution", exact: true })).toBeVisible({ timeout: 15 * 60_000 });
  await page.screenshot({ path: testInfo.outputPath("08-explained.png"), fullPage: true });
  console.log("Predictions downloaded and SHAP contributions visible");
  expect(errors).toEqual([]);
});
