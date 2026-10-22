export * as RelayBinding from "./binding.js"

export const load = () => Promise.all([import("nemo-relay-node"), import("nemo-relay-node/plugin")])
