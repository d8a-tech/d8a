import { getSourceHash, getStoredHash } from "./hash-utils.mjs";

const currentHash = getSourceHash();
const storedHash = getStoredHash();

if (currentHash === storedHash) {
  console.log("Hash matches.");
  process.exit(0);
} else {
  console.error(
    `Hash mismatch!\n  Current: ${currentHash}\n  Stored:  ${storedHash}`
  );
  process.exit(1);
}
