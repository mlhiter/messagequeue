import { createHash } from "node:crypto";
import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const output = resolve(root, "dist");
await rm(output, { recursive: true, force: true });
await mkdir(output, { recursive: true });
for (const file of ["index.html", "styles.css", "app.js", "config.js", "logo.svg"]) await cp(resolve(root, file), resolve(output, file));

const indexPath = resolve(output, "index.html");
let index = await readFile(indexPath, "utf8");
for (const asset of ["styles.css", "config.js", "app.js"]) {
  const content = await readFile(resolve(output, asset));
  const version = createHash("sha256").update(content).digest("hex").slice(0, 12);
  index = index.replaceAll(`"${asset}"`, `"${asset}?v=${version}"`);
}
await writeFile(indexPath, index);

console.log(`Built frontend to ${output}`);
