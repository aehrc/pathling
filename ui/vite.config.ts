import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  base: "/admin/",
  build: {
    sourcemap: true,
  },
  test: {
    environment: "jsdom",
    globals: true,
  },
});
