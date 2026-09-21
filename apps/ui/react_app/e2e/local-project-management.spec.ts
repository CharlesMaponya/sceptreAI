import { expect, test, type Page } from "@playwright/test";
import { existsSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";

test.use({ trace: "off" });
async function login(page: Page) {
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  return state;
}

test("the reported small dataset profiles successfully with a time column", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY || isMobile, "Run the real profile once");
  test.setTimeout(20 * 60_000);
  page.setDefaultTimeout(30_000);
  await login(page);
  const invitePath = join(process.env.RECOVERY_STATE_DIR!, "profile-invite.txt");
  if (existsSync(invitePath)) {
    await page.getByRole("button", { name: "Join project", exact: true }).click();
    await page.getByRole("dialog").getByLabel("Invite token", { exact: true }).fill(readFileSync(invitePath, "utf8").trim());
    await page.getByRole("dialog").getByRole("button", { name: "Join project", exact: true }).click();
    await expect(page).toHaveURL(/\/projects\/[0-9a-f-]+$/);
    unlinkSync(invitePath);
  }
  const projectId = "fc3c2c55-9827-487f-84d4-6d73f7b64a47";
  const latestResponse = page.waitForResponse(response => response.url().endsWith("/profile-jobs/latest"));
  await page.goto(`/projects/${projectId}`);
  await expect(page.getByRole("heading", { name: "Customer churn", exact: true })).toBeVisible();
  const latest = await latestResponse;
  expect(latest.ok()).toBe(true);
  let profile = await latest.json();
  const reuse = profile && !["failed", "cancelled"].includes(profile.status);
  const target = page.getByRole("combobox", { name: /^Target column/ });
  if (!reuse) {
    await expect(target).toBeEnabled();
    await target.selectOption("fraud_type");
  }
  await page.getByText("Time-series preparation", { exact: true }).click();
  const timeColumn = page.getByRole("combobox", { name: /^Time column/ });
  if (!reuse) {
    await timeColumn.selectOption("transaction_date");
    const [response] = await Promise.all([
      page.waitForResponse(response => response.request().method() === "POST" && response.url().endsWith("/profile-jobs")),
      page.getByRole("button", { name: /^(Start profile|Reprofile with target)$/ }).click(),
    ]);
    expect(response.status()).toBe(202);
    profile = await response.json();
  }
  expect(profile.target_column).toBe("fraud_type");
  expect(profile.overview_json.time_column).toBe("transaction_date");
  await expect(target).toHaveValue("fraud_type");
  await expect(timeColumn).toHaveValue("transaction_date");
  writeFileSync(testInfo.outputPath("profile.json"), JSON.stringify({ projectId, profileId: profile.id, reused: Boolean(reuse) }));
  await expect(page.getByRole("link", { name: "Configure training", exact: true })).toBeVisible({ timeout: 15 * 60_000 });
  await expect(page.getByRole("navigation", { name: "Model journey" })).toContainText("Profile complete");
  await expect(page.locator(".js-plotly-plot").first()).toBeVisible({ timeout: 60_000 });
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "instant" }));
  await page.screenshot({ path: testInfo.outputPath("small-dataset-profiled.png"), fullPage: true });
});

test("project invitation pagination, revocation and project deletion work through the UI", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY || isMobile, "Create the temporary workspace once");
  test.setTimeout(5 * 60_000);
  await login(page);
  const resume = process.env.MANAGEMENT_RESUME ? JSON.parse(readFileSync(process.env.MANAGEMENT_RESUME, "utf8")) : null;
  const name = resume?.name || `UI management check ${Date.now()}`;
  if (!resume) {
  await page.getByRole("button", { name: "New project", exact: true }).click();
  await page.getByRole("dialog").getByLabel("Project name", { exact: true }).fill(name);
  await page.getByRole("dialog").getByRole("button", { name: "Create project", exact: true }).click();
  await expect(page).toHaveURL(/\/projects\/[0-9a-f-]+$/);
  }
  const projectUrl = resume?.projectUrl || new URL(page.url()).pathname;
  writeFileSync(testInfo.outputPath("temporary-project.json"), JSON.stringify({ projectUrl, name }));
  await page.goto(`${projectUrl}/members`);
  for (let index = 0; index < (resume ? 0 : 11); index++) {
    await page.getByRole("button", { name: "Create invite token", exact: true }).click();
    await page.getByRole("button", { name: "Create another invite", exact: true }).click();
  }
  const pagination = page.getByRole("navigation", { name: "Invitations pagination" });
  await expect(pagination).toContainText("Page 1");
  await pagination.getByRole("button", { name: "Next", exact: true }).click();
  await expect(pagination).toContainText("Page 2");
  await expect(page.getByRole("button", { name: "Revoke invite", exact: true })).toHaveCount(1);
  await page.getByRole("button", { name: "Revoke invite", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Revoke invite", exact: true }).click();
  await expect(page.getByRole("cell", { name: "Revoked", exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("invitation-history.png"), fullPage: true });
  await page.goto(`${projectUrl}/settings`);
  await page.getByRole("button", { name: "Delete project", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete project", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  await page.getByRole("textbox", { name: "Search projects" }).fill(name);
  await expect(page.getByRole("heading", { name: "No matching projects", exact: true })).toBeVisible({ timeout: 90_000 });
});
