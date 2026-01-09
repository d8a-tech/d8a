import { defineConfig } from "vite";
import { fileURLToPath, URL } from "node:url";

export default defineConfig({
  // Serve the example as the app root.
  root: fileURLToPath(new URL("./example", import.meta.url)),

  // Allow importing TS sources from ../src without copying or building first.
  server: {
    fs: {
      allow: [fileURLToPath(new URL(".", import.meta.url))],
    },
  },
});
