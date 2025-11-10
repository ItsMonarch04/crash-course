/* tslint:disable */
/* eslint-disable */
export function state(handle: SimHandle): string;
/**
 * Advance one persistent simulator by a virtual-time budget.
 */
export function step(handle: SimHandle, virtual_ns: bigint): string;
export function init(spec_json: string): SimHandle;
export function history_verdict(handle: SimHandle): string;
/**
 * Append a data-described fault to the same persistent run used by `step`.
 */
export function inject(handle: SimHandle, action_json: string): void;
export class SimHandle {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_simhandle_free: (a: number, b: number) => void;
  readonly history_verdict: (a: number) => [number, number];
  readonly init: (a: number, b: number) => number;
  readonly inject: (a: number, b: number, c: number) => void;
  readonly state: (a: number) => [number, number];
  readonly step: (a: number, b: bigint) => [number, number];
  readonly __wbindgen_externrefs: WebAssembly.Table;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
