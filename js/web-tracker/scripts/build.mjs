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
      outfile: "dist/wt.min.js",
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
      outfile: "dist/index.min.mjs",
      banner: { js: banner },
    }),
  );

  await Promise.all(ctx);

  // Copy to static location
  const copies = [
    { src: "dist/wt.min.js", dest: "../../pkg/protocol/d8a/static/wt.min.js" },
    {
      src: "dist/wt.min.js.map",
      dest: "../../pkg/protocol/d8a/static/wt.min.js.map",
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
