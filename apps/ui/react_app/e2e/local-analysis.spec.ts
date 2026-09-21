import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";

// Perturbed taxi fixtures exercise the product flow, not generalization quality.
test("taxi model validates an external file and reports drift through the UI", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY || isMobile, "requires local recovery fixtures");
  test.setTimeout(20 * 60_000);
  page.setDefaultTimeout(60_000);
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  page.on("response", response => {
    if (response.status() >= 400) console.log(`HTTP ${response.status()} ${new URL(response.url()).pathname}`);
  });
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  await page.goto(`${state.projectUrl}/runs`);
  await page.getByRole("button", { name: /Taxi total amount without fare leakage/ }).first().click();
  await page.getByRole("tab", { name: "Validate & explain", exact: true }).click();
  await page.getByRole("tab", { name: "External validation", exact: true }).click();
  await page.locator('.analysis-card input[type="file"]').setInputFiles(resolve("../../../artifacts/local-recovery/validation-smoke.csv"));
  await page.getByRole("button", { name: "Upload and inspect", exact: true }).click();
  await expect(page.getByText(/Schema matched:/)).toBeVisible();
  const overview = await page.context().newPage();
  await overview.goto(state.projectUrl);
  const journey = overview.getByRole("navigation", { name: "Model journey" });
  await expect(journey.getByText("Profile complete", { exact: true })).toBeVisible();
  await expect(journey.getByText("Training complete", { exact: true })).toBeVisible();
  const [launchResponse] = await Promise.all([
    page.waitForResponse(response => response.url().endsWith("/validations")
      && response.request().method() === "POST"),
    page.getByRole("button", { name: "Run validation", exact: true }).click(),
  ]);
  expect(launchResponse.ok()).toBe(true);
  const launched = await launchResponse.json();
  await overview.bringToFront();
  await expect(journey.getByText("Analysis underway", { exact: true })).toBeVisible({ timeout: 60_000 });
  await page.bringToFront();
  const result = page.locator(`.analysis-result[data-run-id="${launched.run.id}"]`);
  await expect(result).toContainText("Succeeded", { timeout: 5 * 60_000 });
  await expect(result).toContainText("Rmse");
  await overview.bringToFront();
  await expect(journey.getByText("Analysis complete", { exact: true })).toBeVisible({ timeout: 30_000 });
  await expect(journey.getByText("Deployed", { exact: true })).toBeVisible();
  await overview.screenshot({ path: testInfo.outputPath("11-model-journey.png") });
  await overview.close();
  await page.screenshot({ path: testInfo.outputPath("09-external-validation.png"), fullPage: true });
  console.log("External validation metrics visible");
  await page.goto(`${state.projectUrl}/operations`);
  await page.locator(".registry-card").first().getByRole("button", { name: "Drift", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Run a drift check" });
  await dialog.locator('input[type="file"]').setInputFiles(resolve("../../../artifacts/local-recovery/drift-smoke.csv"));
  await dialog.getByRole("button", { name: "Upload and inspect", exact: true }).click();
  await expect(dialog.getByText(/Schema matched:/)).toBeVisible();
  await dialog.getByLabel("Maximum rows", { exact: true }).fill("1000");
  const [driftResponse] = await Promise.all([
    page.waitForResponse(response => response.url().endsWith("/drift")
      && response.request().method() === "POST"),
    dialog.getByRole("button", { name: "Run drift check", exact: true }).click(),
  ]);
  expect(driftResponse.ok()).toBe(true);
  const driftLaunch = await driftResponse.json();
  await expect(dialog).toBeHidden();
  const drift = page.locator(".section-card").filter({ has: page.getByRole("heading", { name: "Drift checks", exact: true }) });
  const driftRow = drift.locator(`tbody tr[data-run-id="${driftLaunch.run.id}"]`);
  await expect(driftRow).toContainText(/Succeeded|Failed/, { timeout: 10 * 60_000 });
  await expect(driftRow).toContainText("Succeeded");
  await expect(drift.getByText("Latest drift share", { exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("10-drift.png"), fullPage: true });
  expect(errors).toEqual([]);
});
