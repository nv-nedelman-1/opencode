export * as Skill from "./skill.js"

import { makeLocationNode } from "@opencode/util/effect/app-node"
import type { FSUtil } from "@opencode/util/fs-util"
import type { SkillLoad, SkillSourceKind } from "@opencode/plugin/effect/skill"
import path from "path"
import { Cause, Context, Effect, Exit, Layer, Types } from "effect"
import { Skill } from "@opencode/schema/skill"
import { Bus } from "./bus.js"
import { Permission } from "./permission.js"
import { State } from "./state.js"
import type { PluginHooks } from "./plugin/hooks.js"

export const DirectorySource = Skill.DirectorySource
export type DirectorySource = Skill.DirectorySource

export const UrlSource = Skill.UrlSource
export type UrlSource = Skill.UrlSource

export const EmbeddedSource = Skill.EmbeddedSource
export type EmbeddedSource = Skill.EmbeddedSource

export const Source = Skill.Source
export type Source = Skill.Source

export const Info = Skill.Info
export type Info = Skill.Info
export const ID = Skill.ID
export type ID = Skill.ID
export const Name = Skill.Name
export type Name = Skill.Name

export { Event } from "@opencode/schema/skill"

export const available = (skills: ReadonlyArray<Info>, permissions: Permission.Ruleset) =>
  skills.filter((skill) => Permission.evaluate("skill", skill.id, permissions).effect !== "deny")

export const toModelOutput = (skill: Info, files: ReadonlyArray<string>) => {
  const directory = path.dirname(skill.path)
  return [
    `<skill_content name="${skill.name}">`,
    `# Skill: ${skill.name}`,
    "",
    skill.content.trim(),
    "",
    `Base directory for this skill: ${directory}`,
    "Relative paths in this skill (e.g., scripts/, reference/) are relative to this base directory.",
    "Note: file list is sampled.",
    "",
    "<skill_files>",
    ...files.map((file) => `<file>${file}</file>`),
    "</skill_files>",
    "</skill_content>",
  ].join("\n")
}

export const prepare = Effect.fn("Skill.prepare")(function* (fs: FSUtil.Interface, skill: Info) {
  const directory = path.dirname(skill.path)
  const files =
    path.basename(skill.path) === "SKILL.md"
      ? (yield* fs.scan("**/*", { cwd: directory, absolute: true, include: "file", dot: true }))
          .filter((file) => path.basename(file) !== "SKILL.md")
          .toSorted()
          .slice(0, 10)
      : []
  return {
    directory,
    output: toModelOutput(skill, files),
  }
})

export type Data = {
  skills: Map<ID, { skill: Types.DeepMutable<Info>; sourceKind: SkillSourceKind }>
}

export type Editor = {
  list: () => readonly Types.DeepMutable<Info>[]
  get: (id: string) => Types.DeepMutable<Info> | undefined
  add: (skill: Info, sourceKind?: SkillSourceKind) => void
  update: (id: string, update: (skill: Types.DeepMutable<Info>) => void) => void
  remove: (id: string) => void
}

export interface Interface extends State.Transformable<Editor> {
  readonly get: (id: ID) => Effect.Effect<Info | undefined>
  readonly list: () => Effect.Effect<Info[]>
  readonly resolve: (
    id: ID,
  ) => Effect.Effect<{ readonly skill: Info; readonly sourceKind: SkillSourceKind } | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Skill") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service

    const state = State.create<Data, Editor>({
      name: "skill",
      initial: () => ({ skills: new Map() }),
      editor: (editor) => ({
        list: () => Array.from(editor.skills.values(), (entry) => entry.skill),
        get: (id) => editor.skills.get(ID.make(id))?.skill,
        add: (skill, sourceKind = "unknown") => {
          editor.skills.set(skill.id, { skill: { ...skill } as Types.DeepMutable<Info>, sourceKind })
        },
        update: (id, update) => {
          const current = editor.skills.get(ID.make(id))?.skill
          if (!current) return
          update(current)
          current.id = ID.make(id)
        },
        remove: (id) => {
          editor.skills.delete(ID.make(id))
        },
      }),
      notify: () => bus.publish(Skill.Event.Updated, {}).pipe(Effect.asVoid),
    })

    return Service.of({
      transform: state.transform,
      reload: state.reload,
      resolve: Effect.fn("Skill.resolve")(function* (id) {
        return state.get().skills.get(id)
      }),
      get: Effect.fn("Skill.get")(function* (id) {
        return state.get().skills.get(id)?.skill
      }),
      list: Effect.fn("Skill.list")(function* () {
        return Array.from(state.get().skills.values(), (entry) => entry.skill)
      }),
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Bus.node],
})

export const observeLoad = <A, E, R>(
  hooks: PluginHooks.Interface,
  input: Pick<SkillLoad, "sessionID" | "channel"> & { readonly skill?: Info; readonly sourceKind?: SkillSourceKind },
  effect: Effect.Effect<A, E, R>,
) => {
  const skill = input.skill === undefined ? undefined : Object.freeze({ ...input.skill })
  return effect.pipe(
    Effect.onExit((exit) =>
      hooks
        .notify(
          "skill",
          "load",
          Object.freeze({
            sessionID: input.sessionID,
            channel: input.channel,
            sourceKind: input.sourceKind ?? "unknown",
            status: Exit.isSuccess(exit)
              ? "completed"
              : Cause.hasInterruptsOnly(exit.cause)
                ? "interrupted"
                : input.skill === undefined
                  ? "unavailable"
                  : "failed",
            skill,
          }),
        )
        .pipe(Effect.ignoreCause),
    ),
  )
}
