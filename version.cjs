const { readFileSync, writeFileSync } = require("node:fs");
const { join, resolve } = require("node:path");

const ROOT_DIR = resolve(__dirname);
const VERSION = process.argv[process.argv.indexOf("--version") + 1];

setPackageVersions(ROOT_DIR, VERSION);

function setPackageVersions(rootDir, version) {
  const packagePath = join(rootDir, "package.json");
  const packageJson = JSON.parse(readFileSync(packagePath, "utf-8"));

  packageJson.version = version;

  writeFileSync(packagePath, JSON.stringify(packageJson, null, 2));
}
