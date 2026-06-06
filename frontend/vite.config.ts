import { defineConfig } from "vite";
import preact from "@preact/preset-vite";

// In sviluppo si builda in frontend/dist (sicuro, non tocca viewer/).
// Allo swap finale: outDir '../viewer', emptyOutDir false (preserva viewer/data).
export default defineConfig({
  plugins: [preact()],
  base: "/",
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8080",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
