import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
    maxWorkers: 2,
    fileParallelism: false,
    css: true,
    testTimeout: 20_000,
    exclude: ["e2e/**", "node_modules/**", "dist/**"],
    coverage: {
      provider: "v8",
      include: ["src/**/*.{ts,tsx}"],
      exclude: ["src/**/*.test.{ts,tsx}", "src/**/*.d.ts", "src/test/**"],
      reporter: ["text", "json-summary", "lcov"],
      thresholds: {
        statements: 90.01,
        branches: 90.01,
        functions: 90.01,
        lines: 90.01,
      },
    },
  },
});
