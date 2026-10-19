import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The edge serves the build straight from `public/` (the site root) and
// `public/assets` (its `/assets` statics). Vite's own `public/` copy-through
// directory would collide with that, so it is `static/` here.
export default defineConfig({
  plugins: [react()],
  publicDir: "static",
  build: {
    outDir: "public",
    emptyOutDir: true,
  },
  server: {
    // `vite dev` has no edge in front of it; send API calls to the Orbit
    // server through the edge's development host instead.
    proxy: {
      "/api": { target: "http://orbit.test:8080", changeOrigin: true },
    },
  },
});
