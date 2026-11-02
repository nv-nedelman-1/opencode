#!/usr/bin/env bun

import { Script } from "@opencode/script"
import { $ } from "bun"
import { fileURLToPath } from "node:url"

process.chdir(fileURLToPath(new URL("..", import.meta.url)))

const original = await Bun.file("package.json").text()
const pkg = JSON.parse(original) as {
  name: string
  version: string
  exports: Record<string, string | { import: string; types: string }>
  imports: Record<string, Record<string, string>>
}
const packOnly = process.argv.includes("--pack-only")
const output = (value: string) => value.replace("./src/", "./dist/").replace(/\.ts$/, ".js")

if (!packOnly && (await $`npm view ${pkg.name}@${pkg.version} version`.nothrow()).exitCode === 0) {
  console.log(`already published ${pkg.name}@${pkg.version}`)
  process.exit(0)
}

await $`bun run typecheck`
await $`bun run build`
pkg.exports = Object.fromEntries(
  Object.entries(pkg.exports).map(([key, value]) => [
    key,
    typeof value === "string" ? { import: output(value), types: output(value).replace(/\.js$/, ".d.ts") } : value,
  ]),
)
pkg.imports = Object.fromEntries(
  Object.entries(pkg.imports).map(([key, conditions]) => [
    key,
    Object.fromEntries(Object.entries(conditions).map(([condition, value]) => [condition, output(value)])),
  ]),
)

try {
  await Bun.write("package.json", JSON.stringify(pkg, null, 2) + "\n")
  await $`bun pm pack`
  if (!packOnly)
    await $`npm publish ${pkg.name.replace("@", "").replace("/", "-")}-${pkg.version}.tgz --tag ${Script.channel} --access public`
} finally {
  await Bun.write("package.json", original)
}
