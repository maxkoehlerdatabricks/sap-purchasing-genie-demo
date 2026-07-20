import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Build to backend-served ../frontend/dist. During local dev, proxy /api to :8000.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://localhost:8000",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
