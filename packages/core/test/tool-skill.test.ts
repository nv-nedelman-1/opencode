import fs from "fs/promises"
import path from "path"
import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Layer } from "effect"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Permission } from "@opencode/core/permission"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import type { SkillLoad } from "@opencode/plugin/effect/skill"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { Skill } from "@opencode/core/skill"
import { Config } from "@opencode/core/config"
import { ManagedPolicy } from "@opencode/core/managed-policy"
import { SkillTool } from "@opencode/core/tool/plugin/skill"
import { Tool } from "@opencode/core/tool"
import { tmpdir } from "./fixture/tmpdir"
import { Image } from "@opencode/core/image"
import { it } from "./lib/effect"
import { imagePassthrough } from "./lib/image"
import { permissionLayer } from "./lib/permission"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { FSUtil } from "@opencode/util/fs-util"
import { toolIdentity, executeTool, registerToolPlugin, toolDefinitions } from "./lib/tool"
import { registerIntegrationPolicy } from "./fixture/policy"

const skillToolNode = makeLocationNode({
  name: "test/skill-tool-plugin",
  layer: Layer.effectDiscard(registerToolPlugin(SkillTool.Plugin)),
  deps: [Tool.node, FSUtil.node, Skill.node, Permission.node, PluginHooks.node],
})

const sessionID = Session.ID.make("ses_skill_tool_test")

describe("SkillTool", () => {
  it.effect("refuses a policy-blocked skill before authorization or content preparation", () =>
    Effect.gen(function* () {
      const skills = yield* Skill.Service
      const managed = yield* ManagedPolicy.Service
      const tools = yield* Tool.Service
      // This path does not exist; an attempted content scan would fail the test.
      yield* skills.transform((editor) =>
        editor.add(
          Skill.Info.make({
            id: Skill.ID.make("private"),
            name: Skill.Name.make("Private"),
            path: AbsolutePath.make("/unavailable/private/SKILL.md"),
            content: "Private instructions",
          }),
        ),
      )
      yield* managed.set({ statements: [{ action: "integration.use", resource: "skill:private", effect: "deny" }] })
      yield* registerIntegrationPolicy({ skill: skills })
      expect(
        yield* executeTool(tools, {
          sessionID,
          ...toolIdentity,
          call: { type: "tool-call", id: "call-policy-denied-skill", name: "skill", input: { id: "private" } },
        }),
      ).toEqual({ status: "error", error: { type: "tool.execution", message: "Unable to load skill private" } })
    }).pipe(
      Effect.provide(
        AppNodeBuilder.build(LayerNode.group([Tool.node, skillToolNode, Skill.node, Config.node, ManagedPolicy.node]), [
          Config.node.replace(Config.testLayer()),
          Permission.node.replace(permissionLayer({ assert: () => Effect.die("Blocked skill reached authorization") })),
          Image.node.replace(imagePassthrough),
        ]),
      ),
    ),
  )

  it.live("lists available skills, authorizes the selected ID, and loads model-facing content", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          const directory = path.join(tmp.path, "effect")
          const location = path.join(directory, "SKILL.md")
          const reference = path.join(directory, "reference.md")
          yield* Effect.promise(() => fs.mkdir(directory, { recursive: true }))
          yield* Effect.promise(() =>
            Promise.all([fs.writeFile(location, "unused"), fs.writeFile(reference, "reference")]),
          )

          const info: Skill.Info = {
            id: Skill.ID.make("effect"),
            name: Skill.Name.make("Effect"),
            description: "Use Effect",
            path: AbsolutePath.make(location),
            content: "# Effect\n\nGuidance",
          }
          let current = [info]
          const assertions: Permission.AssertInput[] = []
          let deny = false
          let interrupt = false
          const permission = permissionLayer({
            assert: (input) =>
              Effect.sync(() => assertions.push(input)).pipe(
                Effect.andThen(
                  interrupt
                    ? Effect.interrupt
                    : deny
                      ? Effect.fail(
                          new Permission.BlockedError({
                            rules: [],
                            permission: input.action,
                            resources: input.resources,
                          }),
                        )
                      : Effect.void,
                ),
              ),
          })
          const skills = Layer.mock(Skill.Service, {
            resolve: (id) => {
              const skill = current.find((skill) => skill.id === id)
              return Effect.succeed(skill ? { skill, sourceKind: "unknown" as const } : undefined)
            },
            get: (id) => Effect.succeed(current.find((skill) => skill.id === id)),
            list: () => Effect.succeed(current),
          })
          const skillToolLayer = AppNodeBuilder.build(LayerNode.group([Tool.node, skillToolNode, PluginHooks.node]), [
            Permission.node.replace(permission),
            Skill.node.replace(skills),
            Image.node.replace(imagePassthrough),
          ])

          return yield* Effect.gen(function* () {
            const registry = yield* Tool.Service
            const hooks = yield* PluginHooks.Service
            const loads: SkillLoad[] = []
            yield* hooks.register("skill", "load", (event) =>
              Effect.sync(() => {
                loads.push(event)
              }),
            )
            expect((yield* toolDefinitions(registry))[0]).toMatchObject({
              name: "skill",
              description: SkillTool.description,
            })
            expect(
              yield* executeTool(registry, {
                sessionID,
                ...toolIdentity,
                call: { type: "tool-call", id: "call-skill", name: "skill", input: { id: "effect" } },
              }),
            ).toMatchObject({
              status: "completed",
              content: [{ type: "text", text: Skill.toModelOutput(info, [reference]) }],
            })
            expect(Skill.toModelOutput(info, [reference])).toContain(`Base directory for this skill: ${directory}`)
            expect(
              yield* executeTool(registry, {
                sessionID,
                ...toolIdentity,
                call: { type: "tool-call", id: "call-skill-overflow", name: "skill", input: { id: "effect" } },
              }),
            ).toEqual({
              status: "completed",
              output: { name: "Effect", directory, output: Skill.toModelOutput(info, [reference]) },
              content: [{ type: "text", text: Skill.toModelOutput(info, [reference]) }],
              metadata: { name: "Effect", directory },
            })
            expect(assertions).toMatchObject([
              { sessionID, action: "skill", resources: ["effect"], save: ["effect"] },
              { sessionID, action: "skill", resources: ["effect"], save: ["effect"] },
            ])
            expect(
              yield* executeTool(registry, {
                sessionID,
                ...toolIdentity,
                call: { type: "tool-call", id: "call-missing-skill", name: "skill", input: { id: "missing" } },
              }),
            ).toEqual({
              status: "error",
              error: { type: "tool.execution", message: "Unable to load skill missing" },
            })
            deny = true
            expect(
              yield* executeTool(registry, {
                sessionID,
                ...toolIdentity,
                call: { type: "tool-call", id: "call-denied-skill", name: "skill", input: { id: "effect" } },
              }),
            ).toEqual({
              status: "error",
              error: { type: "permission.rejected", message: "Permission denied: skill" },
            })
            deny = false
            interrupt = true
            const interrupted = yield* executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call", id: "call-interrupted-skill", name: "skill", input: { id: "effect" } },
            }).pipe(Effect.exit)
            expect(Exit.isFailure(interrupted) && Cause.hasInterruptsOnly(interrupted.cause)).toBe(true)
            interrupt = false
            const flat = Skill.Info.make({
              id: Skill.ID.make("public"),
              name: Skill.Name.make("Public"),
              description: "Public guidance",
              path: AbsolutePath.make(path.join(tmp.path, "public.md")),
              content: "Public",
            })
            yield* Effect.promise(() =>
              Promise.all([
                fs.writeFile(flat.path, "public"),
                fs.writeFile(path.join(tmp.path, "secret.md"), "secret"),
              ]),
            )
            current = [flat]
            expect(
              yield* executeTool(registry, {
                sessionID,
                ...toolIdentity,
                call: { type: "tool-call", id: "call-flat-skill", name: "skill", input: { id: "public" } },
              }),
            ).toMatchObject({
              status: "completed",
              content: [{ type: "text", text: Skill.toModelOutput(flat, []) }],
            })
            expect(loads.map((load) => [load.channel, load.status, load.skill?.id])).toEqual([
              ["tool", "completed", Skill.ID.make("effect")],
              ["tool", "completed", Skill.ID.make("effect")],
              ["tool", "unavailable", undefined],
              ["tool", "failed", Skill.ID.make("effect")],
              ["tool", "interrupted", Skill.ID.make("effect")],
              ["tool", "completed", Skill.ID.make("public")],
            ])
          }).pipe(Effect.provide(skillToolLayer))
        }),
      ),
    ),
  )
})
