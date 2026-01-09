import { build } from "esbuild";
import { execSync } from "child_process";
import fs from "fs";
import path from "path";
import crypto from "crypto";

const isWatch = process.argv.includes("--watch");

async function generateHash() {
  const files = execSync('find src -type f -name "*.ts" -print0 | sort -z', {
    encoding: "buffer",
  })
    .toString()
    .split("\0")
    .filter(Boolean);

  const hash = crypto.createHash("sha256");
  for (const file of files) {
    const content = fs.readFileSync(file);
    hash.update(content);
  }

  const hashSum = hash.digest("hex");
  fs.writeFileSync("src.hash", `${hashSum}  -\n`);
  console.log(`Hash generated: ${hashSum}`);
}

const banner = `/* ga4-duplicator - built ${new Date().toISOString()} */`;

async function main() {
  await generateHash();

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
  const staticDest = "../../pkg/protocol/ga4/static/duplicator.js";
  if (fs.existsSync(path.dirname(staticDest))) {
    fs.copyFileSync("dist/ga4-duplicator.min.js", staticDest);
    console.log(`Copied to ${staticDest}`);
  }

  if (isWatch) {
    console.log("watch mode is not implemented (run build repeatedly)");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
