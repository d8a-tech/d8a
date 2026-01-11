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
  const copies = [
    { src: "dist/ga4-duplicator.min.js", dest: "../../pkg/protocol/ga4/static/ga4-duplicator.min.js" },
    {
      src: "dist/ga4-duplicator.min.js",
      dest: "../../docs/docs/articles/sources/ga4-duplicator/ga4-duplicator.min.js",
    },
    {
      src: "dist/ga4-duplicator.js",
      dest: "../../docs/docs/articles/sources/ga4-duplicator/ga4-duplicator.js",
    },
    {
      src: "dist/ga4-duplicator.js.map",
      dest: "../../pkg/protocol/ga4/static/ga4-duplicator.min.js.map",
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
