import { build } from "esbuild";
import fs from "fs";
import path from "path";
import { writeHash } from "./hash-utils.mjs";

const isWatch = process.argv.includes("--watch");

const banner = `/* ga4-duplicator - built ${new Date().toISOString()} */`;

async function main() {
  writeHash();

  await build({
    entryPoints: ["src/ga4-duplicator.ts"],
    bundle: true,
    minify: true,
    sourcemap: true,
    target: ["es2018"],
    format: "iife",
    platform: "browser",
    outfile: "dist/ga4-duplicator.min.js",
    banner: { js: banner },
  });

  // Also build unminified version for reference
  await build({
    entryPoints: ["src/ga4-duplicator.ts"],
    bundle: true,
    minify: false,
    sourcemap: true,
    target: ["es2018"],
    format: "iife",
    platform: "browser",
    outfile: "dist/ga4-duplicator.js",
    banner: { js: banner },
  });

  // Copy to static location
  const staticDest = "../../pkg/protocol/ga4/static/ga4-duplicator.js";
  const mapDest = "../../pkg/protocol/ga4/static/ga4-duplicator.js.map";
  if (fs.existsSync(path.dirname(staticDest)) && fs.existsSync(path.dirname(mapDest))) {
    fs.copyFileSync("dist/ga4-duplicator.min.js", staticDest);
    fs.copyFileSync("dist/ga4-duplicator.js.map", mapDest);
    console.log(`Copied to ${staticDest} and ${mapDest}`);
  }

  if (isWatch) {
    console.log("watch mode is not implemented (run build repeatedly)");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
