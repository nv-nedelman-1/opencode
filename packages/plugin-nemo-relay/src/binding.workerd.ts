export * as RelayBinding from "./binding.workerd.js"

// workerd cannot load native addons, so the integration stays inactive there.
export const load = (): Promise<never> => Promise.reject(new Error("NeMo Relay is unavailable on workerd"))
