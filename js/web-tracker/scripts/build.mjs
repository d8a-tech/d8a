import { build } from "esbuild";
import fs from "fs";
import path from "path";
import { writeHash } from "./hash-utils.mjs";

const isWatch = process.argv.includes("--watch");

writeHash();

function getVersion() {
  // Prefer explicit override
  if (process.env.D8A_BUILD_VERSION) {
    return process.env.D8A_BUILD_VERSION;
  }

  // Prefer GITHUB_REF_NAME if it looks like a tag (starts with 'v')
  if (process.env.GITHUB_REF_NAME && process.env.GITHUB_REF_NAME.startsWith("v")) {
    return process.env.GITHUB_REF_NAME;
  }

  // Fall back to package.json version with 'v' prefix
  const packageJson = JSON.parse(fs.readFileSync("package.json", "utf-8"));
  if (packageJson.version) {
    return `v${packageJson.version}`;
  }

  // Final fallback: dev-YY-MM
  const now = new Date();
  const year = String(now.getUTCFullYear()).slice(-2);
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  return `dev-${year}-${month}`;
}

const version = getVersion();
const versionDefine = {
  D8A_VERSION_PLACEHOLDER: JSON.stringify(version),
};

const common = {
  bundle: true,
  sourcemap: true,
  target: ["es2018"],
  logLevel: "info",
  define: versionDefine,
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
