/* tslint:disable */
/* eslint-disable */
/**
 * Retain one complete in-memory simulator image.  `SimCluster: Clone`
 * deliberately includes scheduler/RNG/network/disk/driver volatile state;
 * this is not a replay-from-zero token disguised as a checkpoint.
 */
export function checkpoint(handle: SimHandle): bigint;
export function state(handle: SimHandle): string;
export function trace_hash(handle: SimHandle): string;
/**
 * Advance one persistent simulator by a virtual-time budget.
 */
export function step(handle: SimHandle, virtual_ns: bigint): string;
export function history_verdict(handle: SimHandle): string;
export function init(spec_json: string): SimHandle;
export function restore(handle: SimHandle, checkpoint_id: bigint): string;
/**
 * Append a data-described fault to the same persistent run used by `step`.
 */
export function inject(handle: SimHandle, action_json: string): void;
export function trace_page(handle: SimHandle, cursor: bigint, max_events: number): string;
export function drop_checkpoint(handle: SimHandle, checkpoint_id: bigint): void;
export class SimHandle {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_simhandle_free: (a: number, b: number) => void;
  readonly checkpoint: (a: number) => [bigint, number, number];
  readonly drop_checkpoint: (a: number, b: bigint) => [number, number];
  readonly history_verdict: (a: number) => [number, number];
  readonly init: (a: number, b: number) => [number, number, number];
  readonly inject: (a: number, b: number, c: number) => [number, number];
  readonly restore: (a: number, b: bigint) => [number, number, number, number];
  readonly state: (a: number) => [number, number];
  readonly step: (a: number, b: bigint) => [number, number, number, number];
  readonly trace_hash: (a: number) => [number, number];
  readonly trace_page: (a: number, b: bigint, c: number) => [number, number, number, number];
  readonly __wbindgen_externrefs: WebAssembly.Table;
  readonly __externref_table_dealloc: (a: number) => void;
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
