import { execSync } from "child_process";
import fs from "fs";
import crypto from "crypto";

export function getSourceHash() {
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

  return hash.digest("hex");
}

export function getStoredHash() {
  if (!fs.existsSync("src.hash")) {
    return null;
  }
  const content = fs.readFileSync("src.hash", "utf8");
  return content.split(" ")[0].trim();
}

export function writeHash() {
  const hashSum = getSourceHash();
  fs.writeFileSync("src.hash", `${hashSum}  -\n`);
  console.log(`Hash generated: ${hashSum}`);
}
