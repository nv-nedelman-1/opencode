import type { SkillApi } from "@opencode/client/effect/api"
import { Skill } from "@opencode/schema/skill"
import type { Session } from "@opencode/schema/session"
import type { Effect, Types } from "effect"
import type { Observers, Transform } from "./registration.js"

export type SkillSourceKind = "directory" | "url" | "embedded" | "unknown"

/** A host load attempt, not evidence that the model followed its instructions. */
export interface SkillLoad {
  readonly sessionID: Session.ID
  readonly channel: "tool" | "user_mention" | "user_activation"
  readonly status: "completed" | "unavailable" | "failed" | "interrupted"
  readonly sourceKind: SkillSourceKind
  readonly skill?: Skill.Info
}

/** A change to the effective catalog, not evidence of a user creating, installing, or deleting a skill. */
export interface SkillCatalogChange {
  readonly action: "added" | "updated" | "removed"
  readonly sourceKind: SkillSourceKind
  readonly previousSourceKind?: SkillSourceKind
  readonly skill: Skill.Info
  /** Whether an existing entry's content changed; false for additions and removals. */
  readonly contentChanged: boolean
}

export interface SkillCatalog {
  readonly changes: readonly SkillCatalogChange[]
}

export interface SkillObservations {
  readonly load: SkillLoad
  readonly catalog: SkillCatalog
}

export interface SkillEditor {
  list(): readonly Types.DeepMutable<Skill.Info>[]
  get(id: string): Types.DeepMutable<Skill.Info> | undefined
  add(skill: Skill.Info): void
  update(id: string, update: (skill: Types.DeepMutable<Skill.Info>) => void): void
  remove(id: string): void
}

export interface SkillDomain extends SkillApi<unknown> {
  readonly observe: Observers<SkillObservations>
  readonly transform: Transform<SkillEditor>
  readonly reload: () => Effect.Effect<void>
}
