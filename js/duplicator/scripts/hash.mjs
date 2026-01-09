import { execSync } from "child_process";
import fs from "fs";
import crypto from "crypto";

function getSourceHash() {
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

function getStoredHash() {
  if (!fs.existsSync("src.hash")) {
    return null;
  }
  const content = fs.readFileSync("src.hash", "utf8");
  return content.split(" ")[0].trim();
}

const currentHash = getSourceHash();
const storedHash = getStoredHash();

if (currentHash === storedHash) {
  console.log("Hash matches.");
  process.exit(0);
} else {
  console.error(`Hash mismatch!\n  Current: ${currentHash}\n  Stored:  ${storedHash}`);
  process.exit(1);
}
