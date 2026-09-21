import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

test("prepared taxi workspace remains usable across viewport sizes", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY, "requires the populated local workspace");
  test.setTimeout(5 * 60_000);
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  const assertFits = async () => {
    await expect.poll(() => page.evaluate(() =>
      document.documentElement.scrollWidth <= window.innerWidth + 1,
    )).toBe(true);
  };
  for (const route of ["", "/data", "/training", "/runs", "/operations"]) {
    await page.goto(`${state.projectUrl}${route}`);
    await expect(page.locator("main h1")).toBeVisible({ timeout: 30_000 });
    await page.evaluate(() => window.scrollTo(0, 0));
    if (route === "/runs") {
      await expect(page.getByRole("table", { name: "Model leaderboard" })).toBeVisible();
      await expect(page.getByRole("tab", { name: "Logs", exact: true })).toHaveCount(0);
    }
    await assertFits();
    await page.screenshot({ path: testInfo.outputPath(`${route.slice(1) || "overview"}.png`) });
  }
  if (isMobile) await page.getByRole("button", { name: "Open menu", exact: true }).click();
  await page.getByRole("link", { name: "Results & validation", exact: true }).click();
  await page.locator(".leaderboard-model__trigger").first().click();
  await expect(page.getByRole("heading", { name: "Metrics", exact: true })).toBeVisible();
  for (const tab of ["Diagnostics", "Parameters", "Pipeline & features"]) {
    await page.getByRole("tab", { name: tab, exact: true }).click();
    await assertFits();
  }
  await expect(page.getByRole("heading", { name: "Training pipeline", exact: true })).toBeVisible();
  await page.getByRole("tab", { name: "Feature selection", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Correlated-feature removal", exact: true })).toBeVisible();
  await assertFits();
  await page.getByRole("heading", { name: "Correlated-feature removal", exact: true })
    .evaluate(element => element.scrollIntoView({ block: "center" }));
  await page.screenshot({ path: testInfo.outputPath("feature-selection.png") });
});
