import { expect, test } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

test.use({ trace: "off" });
test("individual failed runs paginate and delete while the uploaded dataset remains", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY || isMobile, "Create isolated failed-run fixtures once");
  test.setTimeout(5 * 60_000);
  page.setDefaultTimeout(30_000);
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  const name = `UI run deletion check ${Date.now()}`;
  await page.getByRole("button", { name: "New project", exact: true }).click();
  await page.getByRole("dialog").getByLabel("Project name", { exact: true }).fill(name);
  await page.getByRole("dialog").getByRole("button", { name: "Create project", exact: true }).click();
  await expect(page).toHaveURL(/\/projects\/[0-9a-f-]+$/);
  const projectUrl = new URL(page.url()).pathname;
  const projectId = projectUrl.split("/").pop()!;
  expect(projectId).toMatch(/^[0-9a-f-]{36}$/);
  writeFileSync(testInfo.outputPath("temporary-project.json"), JSON.stringify({ projectId, projectUrl, name }));
  await page.goto(`${projectUrl}/data`);
  await page.getByRole("button", { name: "Upload dataset", exact: true }).click();
  await page.getByLabel("Dataset name", { exact: true }).fill("Preserved CSV");
  await page.locator('input[type="file"]').setInputFiles({ name: "deletion-check.csv", mimeType: "text/csv", buffer: Buffer.from("value,target\n1,2\n2,4\n3,6\n") });
  await page.getByRole("dialog").getByRole("button", { name: "Upload dataset", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`${projectUrl}$`), { timeout: 90_000 });
  // Seed terminal failures only in the new test-owned project. All deletion and
  // pagination actions below use the published UI and real API; no requests are mocked.
  const sql = `INSERT INTO model_runs (id, project_id, dataset_version_id, created_by_id, run_kind, status, task_type, run_name, params, tags, created_at, updated_at)
    SELECT gen_random_uuid(), p.id, v.id, p.owner_id, 'TRAINING', 'FAILED', 'REGRESSION', 'UI deletion fixture-v' || n, '{}'::jsonb, '{}'::jsonb, now() + n * interval '1 second', now()
    FROM projects p JOIN dataset_versions v ON v.project_id=p.id CROSS JOIN generate_series(1,12) n
    WHERE p.id='${projectId}' AND p.name LIKE 'UI run deletion check %';
    INSERT INTO model_runs (id, project_id, dataset_version_id, created_by_id, run_kind, status, task_type, run_name, params, tags)
    SELECT gen_random_uuid(), project_id, dataset_version_id, created_by_id, 'VALIDATION', 'FAILED', 'REGRESSION', 'Associated failed validation', '{}'::jsonb, jsonb_build_object('source_training_run_id',id::text)
    FROM model_runs WHERE project_id='${projectId}' AND run_name='UI deletion fixture-v12';`;
  execFileSync("kubectl", ["-n", "sceptre", "exec", "sceptre-postgresql-0", "--", "sh", "-c", 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"', "sh", sql], { stdio: "pipe" });
  await page.goto(`${projectUrl}/runs`);
  await expect(page.getByRole("button", { name: /UI deletion fixture-v12/ })).toBeVisible();
  const pagination = page.getByRole("navigation", { name: "Training runs pagination" });
  await pagination.getByRole("button", { name: "Next", exact: true }).click();
  await expect(pagination).toContainText("Page 2");
  await expect(page.getByRole("button", { name: /UI deletion fixture-v1 Regression/ })).toBeVisible();
  await pagination.getByRole("button", { name: "Previous", exact: true }).click();
  await page.getByRole("button", { name: /UI deletion fixture-v12/ }).click();
  await page.getByRole("button", { name: "Delete selected run", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete run", exact: true }).click();
  await expect(page.getByRole("button", { name: /UI deletion fixture-v12/ })).toHaveCount(0, { timeout: 90_000 });
  const remaining = execFileSync("kubectl", ["-n", "sceptre", "exec", "sceptre-postgresql-0", "--", "sh", "-c", 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "$1"', "sh", `SELECT count(*) FROM model_runs WHERE project_id='${projectId}' AND run_kind='VALIDATION'`], { encoding: "utf8" });
  expect(remaining.trim()).toBe("0");
  await page.goto(`${projectUrl}/data`);
  await expect(page.getByText("Preserved CSV", { exact: true }).first()).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("dataset-preserved.png"), fullPage: true });
  await page.goto(`${projectUrl}/settings`);
  await page.getByRole("button", { name: "Delete project", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete project", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
  await page.getByRole("textbox", { name: "Search projects" }).fill(name);
  await expect(page.getByRole("heading", { name: "No matching projects", exact: true })).toBeVisible({ timeout: 90_000 });
});
