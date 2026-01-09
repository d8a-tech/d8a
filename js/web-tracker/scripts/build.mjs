import { build } from "esbuild";

const isWatch = process.argv.includes("--watch");

const common = {
  bundle: true,
  sourcemap: true,
  target: ["es2018"],
  logLevel: "info",
};

const banner = `/* web-tracker - built ${new Date().toISOString()} */`;

const ctx = [];

ctx.push(
  build({
    ...common,
    entryPoints: ["src/browser_entry.ts"],
    format: "iife",
    platform: "browser",
    minify: true,
    outfile: "dist/web-tracker.js",
    banner: { js: banner },
  }),
);

ctx.push(
  build({
    ...common,
    entryPoints: ["src/index.ts"],
    format: "esm",
    platform: "browser",
    minify: true,
    outfile: "dist/index.mjs",
    banner: { js: banner },
  }),
);

if (isWatch) {
  console.log("watch mode is not implemented (run build repeatedly)");
}

await Promise.all(ctx);
