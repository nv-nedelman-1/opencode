export * as SessionSkill from "./skill.js"

import type { Session } from "@opencode/schema/session"
import { Effect } from "effect"
import { Instance } from "../instance/service.js"
import { Plugin } from "../plugin/service.js"
import { PluginHooks } from "../plugin/hooks.js"
import { Skill } from "../skill.js"
import { SkillNotFoundError } from "./error.js"

export const get = Effect.fn("SessionSkill.get")(function* (input: { session: Session.Info; skill: Skill.ID }) {
  const instances = yield* Instance.Service
  return yield* Effect.gen(function* () {
    yield* Plugin.awaitActivation
    const skills = yield* Skill.Service
    const hooks = yield* PluginHooks.Service
    const resolved = yield* skills.resolve(input.skill)
    const skill = resolved?.skill
    return yield* Skill.observeLoad(
      hooks,
      {
        sessionID: input.session.id,
        channel: "user_activation",
        skill,
        sourceKind: resolved?.sourceKind,
      },
      skill ? Effect.succeed(skill) : Effect.fail(new SkillNotFoundError({ skill: input.skill })),
    )
  }).pipe(instances.provide(input.session))
})
