import type { SkillApi } from "@opencode/client/effect/api"
import { Skill } from "@opencode/schema/skill"
import type { Session } from "@opencode/schema/session"
import type { Effect, Types } from "effect"
import type { Hooks, Transform } from "./registration.js"

export type SkillSourceKind = "directory" | "url" | "embedded" | "unknown"

/** A host load attempt, not evidence that the model followed its instructions. */
export interface SkillLoad {
  readonly sessionID: Session.ID
  readonly channel: "tool" | "user_mention" | "user_activation"
  readonly status: "completed" | "unavailable" | "failed" | "interrupted"
  readonly sourceKind: SkillSourceKind
  readonly skill?: Skill.Info
}

export interface SkillHooks {
  readonly load: SkillLoad
}

export interface SkillEditor {
  list(): readonly Types.DeepMutable<Skill.Info>[]
  get(id: string): Types.DeepMutable<Skill.Info> | undefined
  add(skill: Skill.Info): void
  update(id: string, update: (skill: Types.DeepMutable<Skill.Info>) => void): void
  remove(id: string): void
}

export interface SkillDomain extends SkillApi<unknown> {
  readonly hook: Hooks<SkillHooks>
  readonly transform: Transform<SkillEditor>
  readonly reload: () => Effect.Effect<void>
}
