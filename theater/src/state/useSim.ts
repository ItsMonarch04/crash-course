// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { useCallback, useEffect, useRef, useState } from "react";

import type { SimHandle } from "../wasm/cc_wasm.js";

export type WasmModule = {
  default: (moduleOrPath?: string) => Promise<unknown>;
  init: (spec: string) => SimHandle;
  state: (handle: SimHandle) => string;
  step: (handle: SimHandle, virtualNs: bigint) => string;
  inject: (handle: SimHandle, action: string) => void;
  checkpoint: (handle: SimHandle) => bigint;
  restore: (handle: SimHandle, checkpointId: bigint) => string;
  dropCheckpoint: (handle: SimHandle, checkpointId: bigint) => void;
  traceHash: (handle: SimHandle) => string;
  tracePage: (handle: SimHandle, cursor: bigint, maxEvents: number) => string;
};

export type WasmRuntime = {
  module: WasmModule;
  handle: SimHandle;
  checkpoints: Map<number, bigint>;
};

/** One owner for every wasm handle and retained checkpoint. */
export function useSim() {
  const runtime = useRef<WasmRuntime | null>(null);
  const disposed = useRef(new WeakSet<WasmRuntime>());
  const owned = useRef(new WeakSet<WasmRuntime>());
  const [liveHandles, setLiveHandles] = useState(0);

  const disposeRuntime = useCallback((candidate?: WasmRuntime | null) => {
    const selected = candidate === undefined ? runtime.current : candidate;
    if (!selected || disposed.current.has(selected)) return;
    disposed.current.add(selected);
    for (const checkpoint of selected.checkpoints.values()) {
      try {
        selected.module.dropCheckpoint(selected.handle, checkpoint);
      } catch {
        // The handle is freed below even if an already-invalid checkpoint id
        // cannot be dropped independently.
      }
    }
    selected.checkpoints.clear();
    selected.handle.free();
    if (owned.current.has(selected)) setLiveHandles(0);
    if (runtime.current === selected) runtime.current = null;
  }, []);

  const installRuntime = useCallback((next: WasmRuntime) => {
    if (runtime.current !== next) disposeRuntime(runtime.current);
    runtime.current = next;
    owned.current.add(next);
    setLiveHandles(1);
  }, [disposeRuntime]);

  useEffect(() => () => disposeRuntime(), [disposeRuntime]);

  return { runtime, installRuntime, disposeRuntime, liveHandles } as const;
}
