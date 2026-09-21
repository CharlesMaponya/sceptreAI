import { expect, test } from "@playwright/test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

test.use({ trace: "off" });

// Uses the previously uploaded full 2 GB taxi dataset and its sealed split.
// Every training action is through the published UI; no mocked responses.
test("two taxi candidates queue on the model worker and finish without a reload", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_CANDIDATE_PODS || isMobile, "Explicit local training acceptance run");
  test.setTimeout(45 * 60_000);
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  const projectUrl = "/projects/dc93e5cf-0e16-4fee-a546-5d00e0bfd030";
  await page.goto(`${projectUrl}/training`);
  await expect(page.getByRole("combobox", { name: /^Target column/ })).toHaveValue("total_amount");
  await page.getByText("Exclude features from training", { exact: true }).click();
  for (const name of ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge"]) {
    await page.getByLabel(`Exclude ${name}`, { exact: true }).check();
  }
  await page.getByRole("button", { name: "Clear selection", exact: true }).click();
  await page.getByRole("checkbox", { name: /^DummyRegressor\s/ }).check();
  await page.getByRole("checkbox", { name: /^DecisionTreeRegressor\s/ }).check();
  await page.getByLabel("Search iterations", { exact: true }).fill("1");
  await page.getByRole("combobox", { name: "Cross-validation folds", exact: true }).selectOption("2");
  await expect(page.getByLabel("Runtime limit", { exact: true })).toHaveValue("unlimited");
  const estimateResponse = page.waitForResponse(response => response.request().method() === "POST" && response.url().endsWith("/training/estimate"));
  await page.getByRole("button", { name: "Estimate resources", exact: true }).click();
  const response = await estimateResponse;
  expect(response.ok()).toBe(true);
  const estimate = await response.json();
  expect(estimate.active_deadline_seconds).toBeNull();
  expect(estimate.cpu_request_cores).toBe(estimate.cpu_limit_cores);
  writeFileSync(testInfo.outputPath("resource-estimate.json"), JSON.stringify(estimate, null, 2));
  await expect(page.getByRole("button", { name: "Launch training", exact: true })).toBeEnabled();
  const name = `Worker pod acceptance ${Date.now()}`;
  await page.getByLabel("Run name", { exact: true }).fill(name);
  const launchResponse = page.waitForResponse(response => response.request().method() === "POST" && response.url().endsWith("/training/runs"));
  await page.getByRole("button", { name: "Launch training", exact: true }).click();
  const launched = await launchResponse;
  expect(launched.ok()).toBe(true);
  const launch = await launched.json();
  writeFileSync(testInfo.outputPath("run.json"), JSON.stringify({ id: launch.run.id, name, projectUrl }));
  await expect(page).toHaveURL(/\/runs$/);
  await expect(page.getByText("Waiting for worker", { exact: true }).first()).toBeVisible({ timeout: 180_000 });
  await page.screenshot({ path: testInfo.outputPath("queued-models.png"), fullPage: true });
  await expect(page.locator(".run-summary")).toContainText("Succeeded", { timeout: 40 * 60_000 });
  await expect(page.getByRole("link", { name: "Deploy DummyRegressor", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Deploy DecisionTreeRegressor", exact: true })).toBeVisible();
  await expect(page.getByRole("tab", { name: "Logs", exact: true })).toHaveCount(0);
  await page.screenshot({ path: testInfo.outputPath("completed-models.png"), fullPage: true });
  await page.goto(projectUrl);
  await expect(page.getByRole("navigation", { name: "Model journey" })).toContainText("Training complete");
  expect(errors).toEqual([]);
});
