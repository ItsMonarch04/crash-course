// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles/tokens.css";
import { loadMuseum, type MuseumManifest } from "./museum";
import wasmInit, { init as wasmCreate, inject as wasmInject, state as wasmStateJson, step as wasmStep, type SimHandle } from "./wasm/cc_wasm.js";

type Role = "follower" | "candidate" | "leader";
type NodeState = { id: number; role: Role; term: number; commit: number; applied: number; durable: number };
type EventMarker = { t: number; kind: string; note: string };

type TraceEvent = { seq: number; time_ns: number; node: number | null; kind: string; payload_hex: string };
type TraceFixture = { trace_version: number; seed: string; events: TraceEvent[] };
type WasmNode = { id: number; status: string; role: Role; term: number; commit: number; applied: number; durable_bytes: number; log_tail: number[] };
type WasmState = { virtual_time_ns: number; history_len: number; completed_operations: number; had_leader: boolean; nodes: WasmNode[]; trace: TraceFixture };
type FaultSpec = { action: string; node?: number; offset_ms?: number; latency_ms?: number };
type WasmModule = {
  default: (moduleOrPath?: string) => Promise<unknown>;
  init: (spec: string) => SimHandle;
  state: (handle: SimHandle) => string;
  step: (handle: SimHandle, virtualNs: bigint) => string;
  inject: (handle: SimHandle, action: string) => void;
};

const EMPTY_NODES: NodeState[] = [1, 2, 3, 4, 5].map((id) => ({ id, role: "follower", term: 0, commit: 0, applied: 0, durable: 0 }));

function hexText(value: string): string {
  return value.replace(/../g, (part) => String.fromCharCode(Number.parseInt(part, 16)));
}

function deriveNodes(trace: TraceFixture | null): NodeState[] {
  if (!trace || trace.events.length === 0) return EMPTY_NODES;
  const nodes = new Map(EMPTY_NODES.map((node) => [node.id, { ...node }]));
  trace.events.forEach((event) => {
    if (event.node === null) return;
    const current = nodes.get(event.node) ?? { id: event.node, role: "follower", term: 0, commit: 0, applied: 0, durable: 0 };
    const payload = hexText(event.payload_hex);
    if (event.kind === "RoleChange") {
      const role = payload.split(">").at(-1);
      if (role === "leader" || role === "candidate" || role === "follower") current.role = role;
      current.term += role === "leader" ? 1 : 0;
    } else if (event.kind === "Apply") {
      current.applied += 1;
      current.commit = Math.max(current.commit, current.applied);
    } else if (event.kind === "IoDone" || event.kind === "Flush") {
      current.durable += 1;
    }
    nodes.set(event.node, current);
  });
  return [...nodes.values()].sort((left, right) => left.id - right.id);
}

function wasmNodes(state: WasmState | null): NodeState[] | null {
  if (!state || state.nodes.length === 0) return null;
  return state.nodes.map((node) => ({
    id: node.id,
    role: node.role,
    term: node.term,
    commit: node.commit,
    applied: node.applied,
    durable: node.durable_bytes,
  }));
}

function deriveMarkers(trace: TraceFixture | null): EventMarker[] {
  if (!trace || trace.events.length === 0) return [];
  const end = Math.max(...trace.events.map((event) => event.time_ns), 1);
  return trace.events
    .filter((event) => ["RoleChange", "Commit", "Fault", "SnapshotInstall"].includes(event.kind))
    .map((event) => ({
      t: event.time_ns / end,
      kind: event.kind.toLowerCase(),
      note: `${event.kind}${event.node === null ? "" : ` · n${event.node}`}`,
    }));
}

function seedFromUrl(): string {
  const query = new URLSearchParams(window.location.hash.replace(/^#/, ""));
  return query.get("seed") ?? "0x000000000000002a";
}

function scenarioFromUrl(): { seed?: string; profile?: string; faults: FaultSpec[] } {
  const query = new URLSearchParams(window.location.hash.replace(/^#/, ""));
  const raw = query.get("run_spec");
  if (!raw) return { faults: [] };
  try {
    const parsed = JSON.parse(raw) as { seed?: string; profile?: string; faults?: FaultSpec[] };
    return { seed: parsed.seed, profile: parsed.profile, faults: parsed.faults ?? [] };
  } catch {
    return { faults: [] };
  }
}

function profileFromUrl(): string {
  const query = new URLSearchParams(window.location.hash.replace(/^#/, ""));
  return query.get("profile") ?? scenarioFromUrl().profile ?? "rough";
}

function faultsFromUrl(): FaultSpec[] {
  return scenarioFromUrl().faults;
}

function drawTopology(canvas: HTMLCanvasElement, nodes: NodeState[], selected: number | null, partitioned: boolean) {
  const context = canvas.getContext("2d");
  if (!context) return;
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  context.scale(ratio, ratio);
  context.fillStyle = "#0d131c";
  context.fillRect(0, 0, width, height);
  const center = { x: width * 0.48, y: height * 0.51 };
  const radius = Math.min(width, height) * 0.31;
  const points = nodes.map((_, index) => {
    const angle = -Math.PI / 2 + (index / nodes.length) * Math.PI * 2;
    return { x: center.x + Math.cos(angle) * radius, y: center.y + Math.sin(angle) * radius };
  });
  points.forEach((point, index) => {
    const leader = nodes[index].role === "leader";
    context.strokeStyle = partitioned && index === 0 ? "#ed6a5a" : "#334354";
    context.lineWidth = partitioned && index === 0 ? 3 : 1;
    context.beginPath();
    context.moveTo(center.x, center.y);
    context.lineTo(point.x, point.y);
    context.stroke();
    context.fillStyle = leader ? "#58d6b2" : nodes[index].role === "candidate" ? "#f2b84b" : "#8998aa";
    context.beginPath();
    context.arc(point.x, point.y, selected === nodes[index].id ? 31 : 25, 0, Math.PI * 2);
    context.fill();
    context.strokeStyle = selected === nodes[index].id ? "#f5f7fa" : "#17212d";
    context.lineWidth = 3;
    context.stroke();
    context.fillStyle = "#071018";
    context.font = "600 12px ui-monospace, monospace";
    context.textAlign = "center";
    context.fillText(`n${nodes[index].id}`, point.x, point.y + 4);
    context.fillStyle = "#b9c5d3";
    context.font = "10px ui-monospace, monospace";
    context.fillText(`t${nodes[index].term} · c${nodes[index].commit}`, point.x, point.y + 44);
  });
  context.fillStyle = "#182432";
  context.beginPath();
  context.arc(center.x, center.y, 52, 0, Math.PI * 2);
  context.fill();
  context.strokeStyle = "#58d6b2";
  context.lineWidth = 2;
  context.stroke();
  context.fillStyle = "#e6edf5";
  context.font = "600 12px ui-monospace, monospace";
  context.fillText("TRACE", center.x, center.y - 4);
  context.fillStyle = "#7990a8";
  context.font = "10px ui-monospace, monospace";
  context.fillText("live core", center.x, center.y + 12);
}

function App() {
  const canvas = useRef<HTMLCanvasElement>(null);
  const wasmRuntime = useRef<{ module: WasmModule; handle: SimHandle } | null>(null);
  const [trace, setTrace] = useState<TraceFixture | null>(null);
  const [wasmState, setWasmState] = useState<WasmState | null>(null);
  const [engineFailed, setEngineFailed] = useState(false);
  const [virtualTime, setVirtualTime] = useState(0);
  const [nodes, setNodes] = useState<NodeState[]>(EMPTY_NODES);
  const [selected, setSelected] = useState<number | null>(1);
  const [playing, setPlaying] = useState(false);
  const [partitioned, setPartitioned] = useState(false);
  const [speed, setSpeed] = useState("1×");
  const [seed, setSeed] = useState(seedFromUrl);
  const [profile, setProfile] = useState(profileFromUrl);
  const [faults, setFaults] = useState<FaultSpec[]>(faultsFromUrl);
  const [checkpoint, setCheckpoint] = useState(0.42);
  const [shared, setShared] = useState(false);
  const [museumFilter, setMuseumFilter] = useState("all");
  const [museum, setMuseum] = useState<MuseumManifest>({ schema_version: 1, build: "loading", exhibits: [] });
  const markers = useMemo(() => deriveMarkers(trace), [trace]);
  const memoryCheckpoints = useMemo(() => wasmState ? Array.from({ length: 13 }, (_, index) => index / 12) : markers.filter((marker) => marker.kind === "snapshot").map((marker) => marker.t), [markers, wasmState]);
  const recentEvents = useMemo(() => trace?.events.filter((event) => event.node === selected).slice(-3).reverse() ?? [], [trace, selected]);
  const ackedWrites = trace?.events.filter((event) => event.kind === "ClientOk").length ?? 0;
  const lostWrites = trace?.events.filter((event) => event.kind === "ClientTimeout").length ?? 0;
  const selectedNode = nodes.find((node) => node.id === selected) ?? nodes[0] ?? EMPTY_NODES[0];
  const visibleExhibits = museum.exhibits.filter((exhibit) => museumFilter === "all" || exhibit.kind === museumFilter);

  useEffect(() => {
    let cancelled = false;
    const sharedFaults = faultsFromUrl();
    setFaults(sharedFaults);
    loadMuseum().then(setMuseum);
    fetch(`/fixtures/seed-${seed.replace(/^0x/, "")}.json`)
      .then((response) => (response.ok ? response.json() as Promise<TraceFixture> : Promise.reject(new Error("fixture not found"))))
      .then((fixture) => {
        if (!cancelled) setTrace(fixture);
      })
      .catch(() => setTrace(null));
    void (async () => {
      try {
        const module: WasmModule = {
          default: wasmInit,
          init: wasmCreate,
          state: wasmStateJson,
          step: wasmStep,
          inject: wasmInject,
        };
        await module.default("/wasm/cc_wasm_bg.wasm");
        const handle = module.init(JSON.stringify({ seed, profile }));
        sharedFaults.forEach((action) => module.inject(handle, JSON.stringify(action)));
        const state = JSON.parse(module.state(handle)) as WasmState;
        if (!cancelled) {
          wasmRuntime.current = { module, handle };
          setWasmState(state);
          setTrace(state.trace);
        } else {
          handle.free();
        }
      } catch {
        wasmRuntime.current = null;
        if (!cancelled) {
          setWasmState(null);
          setEngineFailed(true);
        }
      }
    })();
    if ("serviceWorker" in navigator) {
      void navigator.serviceWorker.register("./sw.js").catch(() => undefined);
    }
    return () => {
      cancelled = true;
      wasmRuntime.current?.handle.free();
      wasmRuntime.current = null;
    };
  }, [seed, profile]);

  useEffect(() => {
    setNodes(wasmNodes(wasmState) ?? deriveNodes(trace));
  }, [trace, wasmState]);

  useEffect(() => {
    if (canvas.current) drawTopology(canvas.current, nodes, selected, partitioned);
    const resize = () => canvas.current && drawTopology(canvas.current, nodes, selected, partitioned);
    window.addEventListener("resize", resize);
    return () => window.removeEventListener("resize", resize);
  }, [nodes, selected, partitioned]);

  useEffect(() => {
    if (!playing) return;
    const timer = window.setInterval(() => {
      const runtime = wasmRuntime.current;
      if (runtime) {
        try {
          const state = JSON.parse(runtime.module.step(runtime.handle, 500_000_000n)) as WasmState;
          setWasmState(state);
          setTrace(state.trace);
          setVirtualTime(Math.min(1, state.virtual_time_ns / 60_000_000_000));
          if (state.virtual_time_ns >= 60_000_000_000) setPlaying(false);
        } catch {
          setPlaying(false);
        }
      } else {
        setVirtualTime((current) => Math.min(1, current + 0.02));
      }
    }, 500);
    return () => window.clearInterval(timer);
  }, [playing]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === "k") killLeader();
      if (event.key === " ") {
        event.preventDefault();
        setPlaying((value) => !value);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  });

  function killLeader() {
    const leader = nodes.find((node) => node.role === "leader")?.id ?? 1;
    inject({ action: "crash", node: leader });
  }

  function inject(action: FaultSpec) {
    const runtime = wasmRuntime.current;
    if (!runtime) return;
    runtime.module.inject(runtime.handle, JSON.stringify(action));
    const state = JSON.parse(runtime.module.state(runtime.handle)) as WasmState;
    setFaults((current) => [...current, action]);
    setWasmState(state);
    setTrace(state.trace);
  }

  function scrubTo(progress: number) {
    setCheckpoint(progress);
    const runtime = wasmRuntime.current;
    if (!runtime) {
      setVirtualTime(progress);
      return;
    }
    const targetNs = Math.round(progress * 60_000_000_000);
    const currentNs = wasmState?.virtual_time_ns ?? 0;
    // The run is deterministic and monotonic, so scrubbing forward is just the
    // live sim stepped further. Only scrubbing backwards needs a replay.
    let handle = runtime.handle;
    let elapsed = currentNs;
    if (targetNs < currentNs) {
      const replayed = runtime.module.init(JSON.stringify({ seed, profile }));
      faults.forEach((action) => runtime.module.inject(replayed, JSON.stringify(action)));
      runtime.handle.free();
      handle = replayed;
      elapsed = 0;
    }
    let state = JSON.parse(runtime.module.state(handle)) as WasmState;
    for (; elapsed < targetNs; elapsed += 500_000_000) {
      state = JSON.parse(runtime.module.step(handle, 500_000_000n)) as WasmState;
    }
    wasmRuntime.current = { module: runtime.module, handle };
    setWasmState(state);
    setTrace(state.trace);
    setVirtualTime(Math.min(1, state.virtual_time_ns / 60_000_000_000));
  }

  function shareScenario() {
    const query = new URLSearchParams({ seed, profile, run_spec: JSON.stringify({ seed, profile, faults }) });
    const url = `${window.location.origin}${window.location.pathname}#${query.toString()}`;
    window.history.replaceState({}, "", url);
    void navigator.clipboard?.writeText(url);
    setShared(true);
  }

  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand"><span className="brand-mark">CC</span><div><strong>CRASH COURSE</strong><small>DETERMINISTIC FLIGHT RECORDER</small></div></div>
        <div className="verdict"><span className={`led ${partitioned || lostWrites > 0 ? "danger-led" : ""}`} /> {partitioned ? "OPEN" : "SAFE"} <b>{ackedWrites} acked · {lostWrites} lost</b></div>
        <span className="engine-state" data-testid="engine-state">{wasmState ? "LIVE SIM" : engineFailed ? "ENGINE UNAVAILABLE — RECORDED TRACE" : "STARTING"}</span>
        <span className="engine-state" data-testid="leader-id">{nodes.find((node) => node.role === "leader")?.id ?? "none"}</span>
        <button className="quiet-button" onClick={() => setPlaying((value) => !value)}>{playing ? "PAUSE" : "PLAY"}</button>
      </header>
      <section className="control-strip">
        <label>SEED<input value={seed} onChange={(event) => setSeed(event.target.value)} aria-label="Seed" /></label>
        <label>PROFILE<select value={profile} onChange={(event) => setProfile(event.target.value)}><option>calm</option><option>gentle</option><option>rough</option><option>brutal</option><option>membership</option></select></label>
        <label>CLUSTER<select defaultValue="5"><option>3 nodes</option><option>5 nodes</option><option>7 nodes</option></select></label>
        <label>SPEED<select value={speed} onChange={(event) => setSpeed(event.target.value)}><option>¼×</option><option>1×</option><option>4×</option><option>16×</option><option>64×</option></select></label>
        <span className="spacer" />
        <button className="outline-button" onClick={() => { setPartitioned(false); inject({ action: "heal" }); }}>HEAL ALL</button>
        <button className="accent-button" onClick={killLeader}>KILL LEADER <span>k</span></button>
      </section>
      <section className="workspace">
        <aside className="chaos-panel panel">
          <div className="panel-kicker">CHAOS PALETTE</div>
          <button onClick={killLeader}><span className="icon danger">✕</span><span><b>Crash node</b><small>choose a voter</small></span></button>
          <button onClick={() => { setPartitioned(true); inject({ action: "partition", node: selectedNode.id }); }}><span className="icon warning">╱</span><span><b>Partition</b><small>drag nodes apart</small></span></button>
          <button onClick={() => { setPartitioned(false); inject({ action: "heal" }); }}><span className="icon calm">⌁</span><span><b>Heal all</b><small>restore every link</small></span></button>
          <div className="palette-divider" />
          <div className="slider-label"><span>PACKET LOSS</span><b>{partitioned ? "18%" : "0%"}</b></div><input type="range" min="0" max="100" value={partitioned ? 18 : 0} readOnly />
          <div className="slider-label"><span>CLOCK SKEW · n{selectedNode.id}</span><b>0 ms</b></div><input type="range" min="0" max="100" defaultValue="0" onChange={(event) => inject({ action: "clock-skew", node: selectedNode.id, offset_ms: Number(event.target.value) })} />
          <div className="slider-label"><span>DISK LATENCY · n{selectedNode.id}</span><b>1 ms</b></div><input type="range" min="0" max="100" defaultValue="1" onChange={(event) => inject({ action: "disk-degrade", node: selectedNode.id, latency_ms: Number(event.target.value) })} />
          <div className="panel-footnote">Every control appends data to the run spec. Share it, replay it, shrink it.</div>
        </aside>
        <section className="topology-panel panel"><div className="panel-heading"><span><span className="panel-kicker">TOPOLOGY / LIVE</span><small>virtual {Math.round(virtualTime * 60).toString().padStart(2, "0")}s · {speed}</small></span><span className="status-chip">{partitioned ? "PARTITIONED" : "HEALTHY"}</span></div><canvas ref={canvas} onClick={() => setSelected(selected === 1 ? 2 : 1)} aria-label="Cluster topology" /></section>
        <aside className="inspector panel"><div className="panel-kicker">NODE INSPECTOR</div><div className="node-title"><span className={`role-dot ${selectedNode.role}`} /> n{selectedNode.id}<span className="subtle" data-testid="selected-role">{selectedNode.role}</span></div><div className="metric-grid"><Metric label="TERM" value={`t${selectedNode.term}`} /><Metric label="COMMIT" value={`i${selectedNode.commit}`} /><Metric label="APPLIED" value={`i${selectedNode.applied}`} /><Metric label="DURABLE" value={`${selectedNode.durable} rec`} /></div><div className="inspector-section"><div className="section-title">LOG TAIL <span>captured</span></div><div className="log-tail">{Array.from({ length: Math.max(8, selectedNode.durable) }, (_, index) => <i key={index} className={index < selectedNode.applied ? "committed" : "pending"} style={{ opacity: 0.35 + (index % 5) / 8 }} />)}</div></div><div className="inspector-section"><div className="section-title">EVENT STREAM <span>n{selectedNode.id}</span></div>{recentEvents.length === 0 ? <p className="event-line"><b>—</b> no events captured</p> : recentEvents.map((event) => <p className="event-line" key={event.seq}><b>{(event.time_ns / 1e9).toFixed(3)}</b> {event.kind} <em>seq={event.seq}</em></p>)}</div></aside>
      </section>
      <section className="timeline panel"><div className="timeline-head"><span className="panel-kicker">TIMELINE / RE-EXECUTION</span><span className="timeline-actions"><button onClick={() => setPlaying(false)}>◀</button><button onClick={() => setPlaying((value) => !value)}>{playing ? "Ⅱ" : "▶"}</button><button onClick={() => setPlaying(false)}>▶</button><span data-testid="virtual-time">{Math.round(virtualTime * 60)}s / 60s</span></span></div><div className="timeline-track">{markers.map((marker) => <button key={`${marker.t}-${marker.kind}`} className={`marker ${marker.kind}`} style={{ left: `${marker.t * 100}%` }} title={marker.note} onClick={() => scrubTo(marker.t)} />)}<div className="playhead" style={{ left: `${(playing ? virtualTime : checkpoint) * 100}%` }} /></div><div className="timeline-labels"><span>00:00</span><span>captured events</span><span>60:00</span></div><div className="checkpoint-row">MEMORY CHECKPOINTS {memoryCheckpoints.length === 0 ? <span className="subtle">none captured</span> : memoryCheckpoints.map((value) => <button key={value} onClick={() => scrubTo(value)} className={checkpoint === value ? "selected-checkpoint" : ""}>{Math.round(value * 60)}s</button>)}</div></section>
      <section className="museum panel"><div className="timeline-head"><span><span className="panel-kicker">MUSEUM / VERIFIED EXHIBITS</span><small>manifest build {museum.build}</small></span><span className="museum-tools"><select aria-label="Museum category" value={museumFilter} onChange={(event) => setMuseumFilter(event.target.value)}><option value="all">all</option><option value="raft">raft</option><option value="wal">wal</option><option value="store">store</option><option value="membership">membership</option><option value="checker">checker</option></select><span className="status-chip">{visibleExhibits.length} LOADED</span></span></div>{museum.exhibits.length === 0 ? <p className="museum-empty">No verified failure exhibits are published yet. The wing stays empty until a real shrunk trace earns a pinned build.</p> : visibleExhibits.length === 0 ? <p className="museum-empty">No exhibits match this category.</p> : <div className="exhibit-grid">{visibleExhibits.map((exhibit) => <button key={exhibit.id} className="exhibit-card" onClick={() => setSeed(exhibit.seed)}><b>{exhibit.title}</b><small>{exhibit.kind} · {exhibit.verdict} · {exhibit.chapters.length} chapters</small></button>)}</div>}</section>
      <footer><span>THEATER ABI 1 · TRACE v1 · BUILD fixture</span><span>Reduced motion: <button className="link-button">honor system preference</button> · <button className="link-button" onClick={shareScenario}>{shared ? "URL copied ✓" : "Share this scenario ↗"}</button></span></footer>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: string }) { return <div className="metric"><span>{label}</span><b>{value}</b></div>; }

createRoot(document.getElementById("root")!).render(<App />);
