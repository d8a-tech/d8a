import { build } from "esbuild";
import fs from "fs";
import path from "path";
import { writeHash } from "./hash-utils.mjs";

const isWatch = process.argv.includes("--watch");

writeHash();

const common = {
  bundle: true,
  sourcemap: true,
  target: ["es2018"],
  logLevel: "info",
};

const banner = `/* web-tracker - built ${new Date().toISOString()} */`;

async function main() {
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

  await Promise.all(ctx);

  // Copy to static location
  const webTrackerStaticDest = "../../pkg/protocol/d8a/static/web-tracker.js";
  const mapTrackerStaticDest = "../../pkg/protocol/d8a/static/web-tracker.js.map";
  if (
    fs.existsSync(path.dirname(webTrackerStaticDest)) &&
    fs.existsSync(path.dirname(mapTrackerStaticDest))
  ) {
    fs.copyFileSync("dist/web-tracker.js", webTrackerStaticDest);
    fs.copyFileSync("dist/web-tracker.js.map", mapTrackerStaticDest);
    console.log(`Copied to ${webTrackerStaticDest} and ${mapTrackerStaticDest}`);
  }

  if (isWatch) {
    console.log("watch mode is not implemented (run build repeatedly)");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
