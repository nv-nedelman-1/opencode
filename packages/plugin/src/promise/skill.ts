import type { SkillApi } from "@opencode/client/promise/api"
import type { Skill } from "@opencode/schema/skill"
import type { Session } from "@opencode/schema/session"
import type { Observers, Transform } from "./registration.js"
import type { DeepMutable } from "./types.js"

export type SkillSourceKind = "directory" | "url" | "embedded" | "unknown"

/** A host load attempt, not evidence that the model followed its instructions. */
export interface SkillLoad {
  readonly sessionID: Session.ID
  readonly channel: "tool" | "user_mention" | "user_activation"
  readonly status: "completed" | "unavailable" | "failed" | "interrupted"
  readonly sourceKind: SkillSourceKind
  readonly skill?: Skill.Info
}

export interface SkillObservations {
  readonly load: SkillLoad
}

export interface SkillEditor {
  list(): readonly DeepMutable<Skill.Info>[]
  get(id: string): DeepMutable<Skill.Info> | undefined
  add(skill: Skill.Info): void
  update(id: string, update: (skill: DeepMutable<Skill.Info>) => void): void
  remove(id: string): void
}

export interface SkillDomain extends SkillApi {
  readonly observe: Observers<SkillObservations>
  readonly transform: Transform<SkillEditor>
  readonly reload: () => Promise<void>
}
