import { build } from "esbuild";
import fs from "fs";
import path from "path";
import { writeHash } from "./hash-utils.mjs";

const isWatch = process.argv.includes("--watch");

const banner = `/* ga4-duplicator - built ${new Date().toISOString()} */`;

function getVersion() {
  // Prefer explicit override
  if (process.env.D8A_BUILD_VERSION) {
    return process.env.D8A_BUILD_VERSION;
  }

  // Prefer GITHUB_REF_NAME if it looks like a tag (starts with 'v')
  if (process.env.GITHUB_REF_NAME && process.env.GITHUB_REF_NAME.startsWith("v")) {
    return process.env.GITHUB_REF_NAME;
  }

  // Fallback: dev-YY-MM (UTC)
  const now = new Date();
  const year = String(now.getUTCFullYear()).slice(-2);
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  return `dev-${year}-${month}`;
}

const version = getVersion();
const versionDefine = {
  __D8A_VERSION__: JSON.stringify(version),
};

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
    outfile: "dist/gd.min.js",
    banner: { js: banner },
    define: versionDefine,
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
    outfile: "dist/gd.js",
    banner: { js: banner },
    define: versionDefine,
  });

  // Copy to static location
  const copies = [
    {
      src: "dist/gd.min.js",
      dest: "../../pkg/protocol/ga4/static/gd.min.js",
    },
    {
      src: "dist/gd.min.js",
      dest: "../../docs/docs/articles/sources/ga4-duplicator/gd.min.js",
    },
    {
      src: "dist/gd.js",
      dest: "../../docs/docs/articles/sources/ga4-duplicator/gd.js",
    },
    {
      src: "dist/gd.js.map",
      dest: "../../pkg/protocol/ga4/static/gd.min.js.map",
    },
  ];

  for (const { src, dest } of copies) {
    if (fs.existsSync(path.dirname(dest))) {
      fs.copyFileSync(src, dest);
      console.log(`Copied ${src} to ${dest}`);
    }
  }

  if (isWatch) {
    console.log("watch mode is not implemented (run build repeatedly)");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
