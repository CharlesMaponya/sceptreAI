import { expect, test } from "@playwright/test";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";

// Real UI actions against the installed release; no mocked responses or API setup.
test("taxi dataset uploads, prepares, and trains through the UI", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY, "requires the rebuilt local cluster");
  test.skip(isMobile, "upload the large fixture once");
  test.setTimeout(90 * 60_000);
  page.setDefaultTimeout(60_000);
  const datasetPath = process.env.RECOVERY_DATASET;
  if (!datasetPath || !existsSync(datasetPath)) throw new Error("RECOVERY_DATASET must be an existing CSV file");
  const directory = process.env.RECOVERY_STATE_DIR || testInfo.outputPath("state");
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const statePath = join(directory, "browser-state.json");
  const state: { email: string; password: string; registered?: boolean; projectUrl?: string } = existsSync(statePath)
    ? JSON.parse(readFileSync(statePath, "utf8"))
    : { email: `taxi-recovery-${Date.now()}@example.test`, password: `Taxi!${randomBytes(16).toString("hex")}` };
  const save = () => writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
  save();
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("response", (response) => {
    if (response.status() >= 400) console.log(`HTTP ${response.status()} ${new URL(response.url()).pathname}`);
  });
  await page.goto(state.registered ? "/auth" : "/auth?mode=register");
  if (!state.registered) {
    await page.getByLabel("Full name", { exact: true }).fill("Local ML Tester");
    await page.getByLabel("Work email", { exact: true }).fill(state.email);
    await page.getByLabel("Password", { exact: true }).fill(state.password);
    await page.getByLabel("Confirm password", { exact: true }).fill(state.password);
    await page.getByRole("button", { name: "Create account", exact: true }).click();
    await expect(page.getByText("Account created successfully. Sign in to continue.")).toBeVisible({ timeout: 30_000 });
    state.registered = true;
    save();
  }
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/, { timeout: 30_000 });
  if (state.projectUrl) {
    await page.goto(state.projectUrl);
  } else {
    await page.getByRole("button", { name: "New project", exact: true }).click();
    await page.getByLabel("Project name", { exact: true }).fill("NYC taxi recovery");
    await page.getByRole("dialog").getByRole("button", { name: "Create project", exact: true }).click();
    await expect(page).toHaveURL(/\/projects\/[a-f0-9-]+$/, { timeout: 30_000 });
    state.projectUrl = new URL(page.url()).pathname;
    save();
  }
  await expect(page.getByRole("heading", { name: "NYC taxi recovery", exact: true })).toBeVisible();
  // Resume the same experiment after an interrupted browser session.
  await page.goto(`${state.projectUrl}/runs`);
  const existingRun = page.getByRole("button", { name: /Taxi total amount without fare leakage/ }).first();
  await expect(page.getByRole("heading", { name: "Results & validation", exact: true })).toBeVisible();
  if (await existingRun.isVisible()) {
    await expect(existingRun).toContainText("Succeeded", { timeout: 20 * 60_000 });
    await page.screenshot({ path: testInfo.outputPath("04-trained.png"), fullPage: true });
    expect(errors).toEqual([]);
    return;
  }
  await page.goto(state.projectUrl!);
  if (await page.getByRole("link", { name: "Upload data", exact: true }).isVisible()) {
    await page.getByRole("link", { name: "Upload data", exact: true }).click();
    await page.getByRole("button", { name: "Upload dataset", exact: true }).click();
    await page.getByLabel("Dataset name", { exact: true }).fill("January 2015 yellow taxi trips");
    await page.locator('input[type="file"]').setInputFiles(datasetPath);
    await page.getByRole("dialog").getByRole("button", { name: "Upload dataset", exact: true }).click();
    console.log("Full taxi dataset upload started through browser");
    await expect(page).toHaveURL(new RegExp(`${state.projectUrl}$`), { timeout: 30 * 60_000 });
    await page.screenshot({ path: testInfo.outputPath("01-uploaded.png"), fullPage: true });
  }
  await expect(page.getByRole("combobox", { name: /^Target column/ })).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("Checking the latest dataset profile…", { exact: true })).toBeHidden({ timeout: 30_000 });
  if (!(await page.getByRole("link", { name: "Configure training", exact: true }).isVisible())) {
    const target = page.getByRole("combobox", { name: /^Target column/ });
    if (await target.isEnabled()) {
      await target.selectOption("total_amount");
      await page.getByRole("button", { name: /^(Start profile|Reprofile with target)$/ }).click();
    }
    console.log("Waiting for real dataset preparation");
    await page.screenshot({ path: testInfo.outputPath("02-preparing.png"), fullPage: true });
    await expect(page.getByRole("link", { name: "Configure training", exact: true })).toBeVisible({ timeout: 40 * 60_000 });
  }
  await page.screenshot({ path: testInfo.outputPath("02-profiled.png"), fullPage: true });
  await page.getByRole("link", { name: "Configure training", exact: true }).click();
  await expect(page.getByRole("combobox", { name: /^Target column/ })).toHaveValue("total_amount");
  await page.getByText("Exclude features from training", { exact: true }).click();
  for (const column of ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge"]) {
    await page.getByLabel(`Exclude ${column}`, { exact: true }).check();
  }
  await page.getByRole("button", { name: "Clear selection", exact: true }).click();
  await page.getByRole("checkbox", { name: /^Ridge\s/ }).check();
  await page.getByLabel("Search iterations", { exact: true }).fill("5");
  await page.getByRole("combobox", { name: "Cross-validation folds", exact: true }).selectOption("3");
  await page.getByRole("button", { name: "Estimate resources", exact: true }).click();
  await expect(page.getByRole("button", { name: "Launch training", exact: true })).toBeEnabled({ timeout: 60_000 });
  await page.getByLabel("Run name", { exact: true }).fill("Taxi total amount without fare leakage");
  await page.screenshot({ path: testInfo.outputPath("03-estimate.png"), fullPage: true });
  await page.getByRole("button", { name: "Launch training", exact: true }).click();
  await expect(page).toHaveURL(/\/runs$/, { timeout: 60_000 });
  console.log("Training launched through browser");
  await expect(page.getByText("Succeeded", { exact: true }).first()).toBeVisible({ timeout: 20 * 60_000 });
  await page.screenshot({ path: testInfo.outputPath("04-trained.png"), fullPage: true });
  expect(errors).toEqual([]);
});
