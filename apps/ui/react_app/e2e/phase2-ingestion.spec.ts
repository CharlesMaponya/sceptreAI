import { expect, test } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("browser reload resumes the persisted provider session", async ({
  isMobile,
  page,
  request,
}) => {
  test.skip(!process.env.PHASE2_LIVE_E2E, "requires the live k3d Phase 2 data plane");
  test.skip(isMobile, "the protocol proof only needs one browser viewport");
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const email = `phase2-browser-${suffix}@example.test`;
  const password = "Phase2Browser!42";
  const registration = await request.post("/api/v1/auth/register", {
    data: { email, password, full_name: "Phase 2 Browser" },
  });
  expect(registration.ok()).toBeTruthy();
  const login = await request.post("/api/v1/auth/login", { data: { email, password } });
  expect(login.ok()).toBeTruthy();
  const session = await login.json();
  const projectResponse = await request.post("/api/v1/projects", {
    data: { name: `Phase 2 browser ${suffix}` },
    headers: { Authorization: `Bearer ${session.tokens.access_token}` },
  });
  expect(projectResponse.ok()).toBeTruthy();
  const project = await projectResponse.json();
  await page.addInitScript((value) => {
    window.localStorage.setItem("sceptre.session", JSON.stringify(value));
  }, session);

  const directory = await mkdtemp(join(tmpdir(), "sceptre-phase2-browser-"));
  const fixturePath = join(directory, "browser-reload.csv");
  const payload = `feature,target\n${"1,0\n".repeat(262_144)}`;
  await writeFile(fixturePath, payload);
  let beginRequests = 0;
  let abortFirstInstruction = true;
  page.on("request", (observed) => {
    if (observed.method() === "POST" && /\/datasets\/uploads$/.test(observed.url())) {
      beginRequests += 1;
    }
  });
  await page.route("**/datasets/uploads/*/instructions", async (route) => {
    if (abortFirstInstruction) {
      abortFirstInstruction = false;
      await route.abort("internetdisconnected");
      return;
    }
    await route.continue();
  });

  try {
    await page.goto(`/projects/${project.id}/data`);
    await page.getByRole("button", { name: "Upload dataset" }).click();
    await page.getByLabel("Dataset name").fill("Browser reload fixture");
    await page.locator('input[type="file"]').setInputFiles(fixturePath);
    await page.getByRole("dialog").getByRole("button", { name: "Upload dataset" }).click();
    await expect(page.getByText(/Failed to fetch|fetch|network/i)).toBeVisible({ timeout: 60_000 });

    if (process.env.PHASE2_EVICT_UI_POD) {
      const pod = execFileSync("kubectl", ["-n", "sceptre", "get", "pod", "-l",
        "app.kubernetes.io/component=ui", "-o", "jsonpath={.items[0].metadata.name}"],
      { encoding: "utf8" }).trim();
      execFileSync("kubectl", ["-n", "sceptre", "delete", "pod", pod, "--wait=true"]);
      execFileSync("kubectl", ["-n", "sceptre", "rollout", "status",
        "deployment/sceptre-ui", "--timeout=180s"]);
    }

    await page.reload();
    await page.getByRole("button", { name: "Upload dataset" }).click();
    await page.getByLabel("Dataset name").fill("Browser reload fixture");
    await page.locator('input[type="file"]').setInputFiles(fixturePath);
    await page.getByRole("dialog").getByRole("button", { name: "Upload dataset" }).click();
    await expect(page).toHaveURL(new RegExp(`/projects/${project.id}$`), { timeout: 180_000 });
    expect(beginRequests).toBe(1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("browser retries a throttled provider transfer without opening a new session", async ({
  isMobile,
  page,
  request,
}) => {
  test.skip(!process.env.PHASE2_LIVE_E2E, "requires the live k3d Phase 2 data plane");
  test.skip(isMobile, "the protocol proof only needs one browser viewport");
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const email = `phase2-throttle-${suffix}@example.test`;
  const password = "Phase2Throttle!42";
  await request.post("/api/v1/auth/register", {
    data: { email, password, full_name: "Phase 2 Throttle" },
  });
  const login = await request.post("/api/v1/auth/login", { data: { email, password } });
  expect(login.ok()).toBeTruthy();
  const session = await login.json();
  const projectResponse = await request.post("/api/v1/projects", {
    data: { name: `Phase 2 throttle ${suffix}` },
    headers: { Authorization: `Bearer ${session.tokens.access_token}` },
  });
  expect(projectResponse.ok()).toBeTruthy();
  const project = await projectResponse.json();
  await page.addInitScript((value) => {
    window.localStorage.setItem("sceptre.session", JSON.stringify(value));
  }, session);

  const directory = await mkdtemp(join(tmpdir(), "sceptre-phase2-throttle-"));
  const fixturePath = join(directory, "browser-throttle.csv");
  await writeFile(fixturePath, `feature,target\n${"2,1\n".repeat(262_144)}`);
  let beginRequests = 0;
  let providerPutAttempts = 0;
  page.on("request", (observed) => {
    if (observed.method() === "POST" && /\/datasets\/uploads$/.test(observed.url())) {
      beginRequests += 1;
    }
  });
  await page.route("**/*", async (route) => {
    const observed = route.request();
    if (observed.method() !== "PUT" || !observed.url().includes("storage.localhost:8080")) {
      await route.continue();
      return;
    }
    providerPutAttempts += 1;
    if (providerPutAttempts === 1) {
      await route.fulfill({
        status: 429,
        headers: {
          "Access-Control-Allow-Origin": "http://localhost:8080",
          "Access-Control-Expose-Headers": "Retry-After",
          "Retry-After": "0",
        },
      });
      return;
    }
    await route.continue();
  });

  try {
    await page.goto(`/projects/${project.id}/data`);
    await page.getByRole("button", { name: "Upload dataset" }).click();
    await page.getByLabel("Dataset name").fill("Browser throttle fixture");
    await page.locator('input[type="file"]').setInputFiles(fixturePath);
    await page.getByRole("dialog").getByRole("button", { name: "Upload dataset" }).click();
    await expect(page).toHaveURL(new RegExp(`/projects/${project.id}$`), { timeout: 180_000 });
    expect(providerPutAttempts).toBe(2);
    expect(beginRequests).toBe(1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
