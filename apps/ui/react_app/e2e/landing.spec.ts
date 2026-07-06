import { expect, test } from "@playwright/test";

test("public landing page communicates the offer and routes to registration", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveTitle(/Sceptre AI/);
  await expect(page.getByRole("heading", {
    name: /Build models your business can trust/i,
  })).toBeVisible();
  await expect(page.getByText("Production discipline without the platform tax.")).toBeVisible();
  await page.getByRole("link", { name: "Build your first model" }).click();
  await expect(page).toHaveURL(/\/auth\?mode=register/);
  await expect(page.getByRole("heading", { name: "Create your account" })).toBeVisible();
});

test("navigation and primary content remain usable at the configured viewport", async ({ page }) => {
  await page.goto("/");
  const menu = page.getByRole("button", { name: "Open navigation" });
  if (await menu.isVisible()) {
    await menu.click();
    await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeVisible();
  }
  await expect(page.getByRole("link", { name: /Start building/i }).first()).toBeVisible();
  await expect(page.locator("body")).not.toHaveCSS("overflow-x", "scroll");
});
