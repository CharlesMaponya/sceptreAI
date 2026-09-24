import { expect, test, type Page } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

test.use({ trace: "off" });
async function login(page: Page) {
  const state = JSON.parse(readFileSync(join(process.env.RECOVERY_STATE_DIR!, "browser-state.json"), "utf8"));
  await page.goto("/auth");
  await page.getByLabel("Work email", { exact: true }).fill(state.email);
  await page.getByLabel("Password", { exact: true }).fill(state.password);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/projects$/);
}
function sql(statement: string) {
  return execFileSync("kubectl", ["-n", "sceptre", "exec", "sceptre-postgresql-0", "--", "sh", "-c", 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "$1"', "sh", statement], { encoding: "utf8" });
}
async function chart(page: Page) {
  const plot = page.locator(".target-visualization .js-plotly-plot");
  await expect(plot).toBeVisible({ timeout: 60_000 });
  return plot.evaluate(node => (node as HTMLElement & { data: Array<{ x: unknown[]; y: unknown[] }> }).data.map(series => ({ x: series.x, y: series.y })));
}

test("completed target chart and saved split counts stay consistent across training navigation", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY || isMobile, "Read the reported project once");
  test.setTimeout(3 * 60_000);
  await login(page);
  const projectUrl = "/projects/fc3c2c55-9827-487f-84d4-6d73f7b64a47";
  await page.goto(projectUrl);
  await expect(page.getByText("Training split · completed profile", { exact: true })).toBeVisible();
  const selectedTarget = await page.getByRole("combobox", { name: /^Target column/ }).inputValue();
  expect(selectedTarget).toBeTruthy();
  const before = await chart(page);
  const activeResponse = page.waitForResponse(response => response.url().endsWith("/training/active-run"));
  await page.getByRole("link", { name: "Configure training", exact: true }).click();
  const active = await (await activeResponse).json();
  const estimateButton = page.getByRole("button", { name: "Estimate resources", exact: true });
  if (active?.id) {
    await expect(estimateButton).toBeDisabled();
    await expect(page.getByText(/Training is currently underway in this project/)).toBeVisible();
  } else {
    await expect(estimateButton).toBeEnabled();
    const response = page.waitForResponse(item => item.request().method() === "POST" && item.url().endsWith("/training/estimate"));
    await estimateButton.click();
    const estimateResponse = await response;
    expect(estimateResponse.ok()).toBe(true);
    const estimate = await estimateResponse.json();
    const summary = estimate.sample_tier_summary;
    expect(summary.source_row_count).toBe(1_000_000);
    expect(summary.split_counts).toEqual({ train: 699567, validation: 150161, final_test: 150272 });
    expect(summary.row_count).toBeLessThanOrEqual(summary.split_counts.train);
    expect(summary.validation_row_count).toBe(10_000);
    await expect(page.getByText(/Saved split: 699,567 training rows/)).toBeVisible();
    writeFileSync(testInfo.outputPath("estimate-counts.json"), JSON.stringify(summary));
    await page.screenshot({ path: testInfo.outputPath("actual-split-counts.png"), fullPage: true });
  }
  await page.getByRole("link", { name: "Overview", exact: true }).click();
  await expect(page.getByText("Training split · completed profile", { exact: true })).toBeVisible();
  await expect(page.getByRole("combobox", { name: /^Target column/ })).toHaveValue(selectedTarget);
  expect(await chart(page)).toEqual(before);
  await expect(page.getByText("Instant target preview", { exact: true })).toHaveCount(0);
  await page.reload();
  expect(await chart(page)).toEqual(before);
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "instant" }));
  await page.screenshot({ path: testInfo.outputPath("stable-profile.png"), fullPage: true });
  await page.goto(`${projectUrl}/runs`);
  const partitions = page.getByRole("region", { name: "Saved dataset partitions" });
  await expect(partitions).toContainText("Training rows699,567");
  await expect(partitions).toContainText("Validation rows150,161");
  await expect(partitions).toContainText("Final-test rows150,272");
  const firstModel = page.locator(".training-model-row").first();
  await expect(firstModel).not.toContainText("699,567");
  await expect(partitions).toContainText("Candidate scoring samples use 10,000 rows from the validation partition.");
  await expect(page.getByRole("columnheader", { name: "Dataset rows" })).toHaveCount(0);
  await page.screenshot({ path: testInfo.outputPath("results-full-partitions.png"), fullPage: true });
});

test("saved run partitions show full counts separately from scoring samples", async ({ page }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY, "Read the reported training run");
  await login(page);
  await page.goto("/projects/fc3c2c55-9827-487f-84d4-6d73f7b64a47/runs");
  const partitions = page.getByRole("region", { name: "Saved dataset partitions" });
  await expect(partitions).toContainText("Training rows699,567");
  await expect(partitions).toContainText("Validation rows150,161");
  await expect(partitions).toContainText("Final-test rows150,272");
  const row = page.locator(".training-model-row").first();
  await expect(row).not.toContainText("699,567");
  await expect(partitions).toContainText("Candidate scoring samples use 10,000 rows from the validation partition.");
  await expect(page.getByRole("columnheader", { name: "Dataset rows" })).toHaveCount(0);
  await row.getByRole("button", { name: /details$/ }).click();
  await expect(page.getByRole("heading", { name: "Metrics", exact: true })).toBeVisible();
  await expect(page.getByText("Candidate fitting rows: 699,567 · Validation scoring rows: 10,000", { exact: true })).toBeVisible();
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1)).toBe(true);
  await page.locator(".training-table-scroll").evaluate(element => { element.scrollLeft = 0; });
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "instant" }));
  await page.screenshot({ path: testInfo.outputPath("full-counts-table.png"), fullPage: true });
});

test("active project training disables estimation and re-enables it when the run stops", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY || isMobile, "Create an isolated UI state fixture once");
  test.setTimeout(3 * 60_000);
  await login(page);
  const name = `UI training lock check ${Date.now()}`;
  await page.getByRole("button", { name: "New project", exact: true }).click();
  await page.getByRole("dialog").getByLabel("Project name", { exact: true }).fill(name);
  await page.getByRole("dialog").getByRole("button", { name: "Create project", exact: true }).click();
  await expect(page).toHaveURL(/\/projects\/[0-9a-f-]+$/);
  const projectUrl = new URL(page.url()).pathname;
  const projectId = projectUrl.split("/").pop()!;
  expect(projectId).toMatch(/^[0-9a-f-]{36}$/);
  writeFileSync(testInfo.outputPath("fixture-project.json"), JSON.stringify({ projectUrl, name }));
  await page.goto(`${projectUrl}/data`);
  await page.getByRole("button", { name: "Upload dataset", exact: true }).click();
  await page.getByLabel("Dataset name", { exact: true }).fill("Training lock fixture");
  await page.locator('input[type="file"]').setInputFiles({ name: "lock.csv", mimeType: "text/csv", buffer: Buffer.from("value,target\n1,2\n2,4\n3,6\n") });
  await page.getByRole("dialog").getByRole("button", { name: "Upload dataset", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`${projectUrl}$`), { timeout: 90_000 });
  // Synthetic state fixtures only in this newly created project; no training job
  // or mocked browser/API responses. The user's project and runs are untouched.
  sql(`INSERT INTO profiling_jobs (id,project_id,dataset_id,dataset_version_id,created_by_id,target_column,status,current_stage,overview_json)
    SELECT gen_random_uuid(),p.id,v.dataset_id,v.id,p.owner_id,'target','succeeded','complete','{"task_inference":{"task_type":"regression"}}'::jsonb
    FROM projects p JOIN dataset_versions v ON v.project_id=p.id WHERE p.id='${projectId}' AND p.name LIKE 'UI training lock check %';
    INSERT INTO model_runs (id,project_id,dataset_version_id,created_by_id,run_kind,status,task_type,run_name,params,tags)
    SELECT gen_random_uuid(),p.id,v.id,p.owner_id,'TRAINING','RUNNING','REGRESSION','UI active training fixture','{}'::jsonb,'{}'::jsonb
    FROM projects p JOIN dataset_versions v ON v.project_id=p.id WHERE p.id='${projectId}' AND p.name LIKE 'UI training lock check %';`);
  try {
    await page.goto(`${projectUrl}/training`);
    const estimate = page.getByRole("button", { name: "Estimate resources", exact: true });
    await expect(page.getByText(/Training is currently underway in this project/)).toBeVisible();
    await expect(estimate).toBeDisabled();
    await page.screenshot({ path: testInfo.outputPath("training-disabled.png"), fullPage: true });
    sql(`UPDATE model_runs SET params='{"candidate_models":["Ridge"],"candidate_limit":1}'::jsonb,
      tags='{"current_candidate":"Ridge","candidate_phase":"hyperparameter_search","leaderboard":[{"model":"Ridge","status":"running","metrics":{},"rank":null,"primary_score":null,"duration_seconds":null,"error":null,"training_rows":2,"validation_rows":1}]}'::jsonb
      WHERE project_id='${projectId}' AND run_name='UI active training fixture';`);
    await page.goto(`${projectUrl}/runs`);
    const modelRow = page.locator(".training-model-row").filter({ hasText: "Ridge" });
    await expect(modelRow).toContainText("Hyperparameter search");
    await expect(page.locator(".live-training-summary")).toContainText("Run statusRunning");
    await expect(page.getByText("Current phase", { exact: true })).toHaveCount(0);
    for (const [phase, label] of [["evaluating", "Validation"], ["learning_curve", "Building learning curves"], ["logging_to_mlflow", "Saving to MLflow"]]) {
      sql(`UPDATE model_runs SET tags=jsonb_set(tags,'{candidate_phase}','"${phase}"'::jsonb)
        WHERE project_id='${projectId}' AND run_name='UI active training fixture';`);
      await expect(modelRow).toContainText(label, { timeout: 15_000 });
      await expect(page.locator(".live-training-summary")).toContainText("Run statusRunning");
    }
    await page.screenshot({ path: testInfo.outputPath("model-row-phase.png"), fullPage: true });
    sql(`UPDATE model_runs SET status='SUCCEEDED',finished_at=now(),
      tags=jsonb_set(jsonb_set(tags,'{leaderboard,0,status}','"succeeded"'::jsonb),
        '{leaderboard,0,metrics}','{"rmse":0.1}'::jsonb)
      WHERE project_id='${projectId}' AND run_name='UI active training fixture';`);
    await expect(modelRow).toContainText("Succeeded", { timeout: 15_000 });
    await expect(page.getByRole("link", { name: "Deploy Ridge", exact: true })).toBeVisible();
    await expect(page.locator(".run-summary")).toContainText("Succeeded");
    await page.screenshot({ path: testInfo.outputPath("completion-without-reload.png"), fullPage: true });
    sql(`UPDATE model_runs SET status='RUNNING',finished_at=NULL WHERE project_id='${projectId}' AND run_name='UI active training fixture';`);
    await page.goto(projectUrl);
    const journey = page.getByRole("navigation", { name: "Model journey" });
    await expect(journey).toContainText("Training underway");
    sql(`UPDATE model_runs SET status='SUCCEEDED',finished_at=now() WHERE project_id='${projectId}' AND run_name='UI active training fixture';`);
    await expect(journey).toContainText("Training complete", { timeout: 15_000 });
    await page.screenshot({ path: testInfo.outputPath("journey-updated.png"), fullPage: true });
    await page.goto(`${projectUrl}/training`);
    await expect(estimate).toBeEnabled({ timeout: 15_000 });
  } finally {
    sql(`UPDATE model_runs SET status='CANCELLED',finished_at=now() WHERE project_id='${projectId}' AND run_name='UI active training fixture';`);
    await page.goto(`${projectUrl}/settings`);
    await page.getByRole("button", { name: "Delete project", exact: true }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Delete project", exact: true }).click();
    await expect(page).toHaveURL(/\/projects$/);
  }
});

test("a completed candidate opens deployment while another candidate is still training", async ({ page, isMobile }, testInfo) => {
  test.skip(!process.env.LIVE_RECOVERY || isMobile, "Isolated selection fixture");
  test.setTimeout(3 * 60_000);
  await login(page);
  const name = `UI model choice check ${Date.now()}`;
  await page.getByRole("button", { name: "New project", exact: true }).click();
  await page.getByRole("dialog").getByLabel("Project name", { exact: true }).fill(name);
  await page.getByRole("dialog").getByRole("button", { name: "Create project", exact: true }).click();
  await expect(page).toHaveURL(/\/projects\/[0-9a-f-]+$/);
  const projectUrl = new URL(page.url()).pathname;
  const projectId = projectUrl.split("/").pop()!;
  expect(projectId).toMatch(/^[0-9a-f-]{36}$/);
  writeFileSync(testInfo.outputPath("fixture-project.json"), JSON.stringify({ projectUrl, name }));
  try {
    await page.goto(`${projectUrl}/data`);
    await page.getByRole("button", { name: "Upload dataset", exact: true }).click();
    await page.getByLabel("Dataset name", { exact: true }).fill("Model selection fixture");
    await page.locator('input[type="file"]').setInputFiles({ name: "choice.csv", mimeType: "text/csv", buffer: Buffer.from("x,target\n1,2\n2,4\n3,6\n") });
    await page.getByRole("dialog").getByRole("button", { name: "Upload dataset", exact: true }).click();
    await expect(page).toHaveURL(new RegExp(`${projectUrl}$`), { timeout: 90_000 });
    // Synthetic mixed-status candidates validate selection only, without registering
    // fake artifacts or replacing any of the user's deployed models.
    sql(`INSERT INTO model_runs (id,project_id,dataset_version_id,created_by_id,run_kind,status,task_type,run_name,params,tags,finished_at)
      SELECT gen_random_uuid(),p.id,v.id,p.owner_id,'TRAINING','RUNNING','REGRESSION','UI candidate selection fixture',
      '{"candidate_models":["Ridge","DummyRegressor","RandomForestRegressor"],"candidate_limit":3}'::jsonb,
      '{"completed_candidates":2,"current_candidate":"RandomForestRegressor","candidate_phase":"hyperparameter_search","leaderboard_primary_metric":"rmse","leaderboard":[{"model":"Ridge","status":"succeeded","cost_tier":"low","rank":1,"primary_score":0.1,"duration_seconds":1,"error":null,"metrics":{"rmse":0.1}},{"model":"DummyRegressor","status":"succeeded","cost_tier":"low","rank":2,"primary_score":2.0,"duration_seconds":0.1,"error":null,"metrics":{"rmse":2.0}},{"model":"RandomForestRegressor","status":"running","cost_tier":"medium","rank":null,"primary_score":null,"duration_seconds":null,"error":null,"metrics":{}}]}'::jsonb,NULL
      FROM projects p JOIN dataset_versions v ON v.project_id=p.id WHERE p.id='${projectId}' AND p.name LIKE 'UI model choice check %';`);
    await page.goto(`${projectUrl}/runs`);
    const selected = page.locator(".training-model-row").filter({ hasText: "DummyRegressor" });
    await expect(selected).toContainText("Rank 2");
    await expect(page.locator(".run-summary")).toContainText("Running");
    await expect(page.getByText("Awaiting run completion", { exact: true })).toHaveCount(0);
    await expect(page.getByRole("link", { name: "Deploy RandomForestRegressor", exact: true })).toHaveCount(0);
    await page.screenshot({ path: testInfo.outputPath("completed-candidate-action.png"), fullPage: true });
    await selected.getByRole("link", { name: "Deploy DummyRegressor", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "Register a trained model" });
    await expect(dialog.getByLabel("Successful candidate")).toHaveValue("DummyRegressor");
    await expect(dialog.getByRole("button", { name: "Register model", exact: true })).toBeEnabled();
    await page.screenshot({ path: testInfo.outputPath("nonwinner-selected.png"), fullPage: true });
    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
  } finally {
    sql(`UPDATE model_runs SET status='CANCELLED',finished_at=now() WHERE project_id='${projectId}' AND run_name='UI candidate selection fixture';`);
    await page.goto(`${projectUrl}/settings`);
    await page.getByRole("button", { name: "Delete project", exact: true }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Delete project", exact: true }).click();
    await expect(page).toHaveURL(/\/projects$/);
  }
});
