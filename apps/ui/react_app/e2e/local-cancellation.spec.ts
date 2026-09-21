import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

test("taxi run cancels and restarts through the UI", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY_RESTART || isMobile, "explicit local recovery exercise");
  test.setTimeout(5 * 60_000);
  const directory = process.env.RECOVERY_STATE_DIR;
  if (!directory) throw new Error("RECOVERY_STATE_DIR is required");
  const state = JSON.parse(readFileSync(join(directory, "browser-state.json"), "utf8"));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/, { timeout: 30_000 });
  await page.goto(`${state.projectUrl}/runs`);
  await page.getByRole("button", { name: /Taxi total amount without fare leakage/ }).first().click();
  await page.getByRole("button", { name: "Cancel run", exact: true }).click();
  await expect(page.getByRole("button", { name: "Restart run", exact: true })).toBeVisible({ timeout: 60_000 });
  await expect(page.locator(".run-summary")).toContainText("Cancelled");
  await page.screenshot({ path: testInfo.outputPath("cancelled.png"), fullPage: true });
  await page.getByRole("button", { name: "Restart run", exact: true }).click();
  const restarted = page.getByRole("button", { name: /Taxi total amount without fare leakage.*restart/i }).first();
  await expect(restarted).toBeVisible({ timeout: 60_000 });
  await restarted.click();
  await expect(page.getByRole("button", { name: "Cancel run", exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("restarted.png"), fullPage: true });
});
