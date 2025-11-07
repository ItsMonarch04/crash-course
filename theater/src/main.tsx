// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles/tokens.css";
import { loadMuseum, type MuseumManifest } from "./museum";

type Role = "follower" | "candidate" | "leader";
type NodeState = { id: number; role: Role; term: number; commit: number; applied: number; durable: number };
type EventMarker = { t: number; kind: string; note: string };

const initialNodes: NodeState[] = [
  { id: 1, role: "leader", term: 7, commit: 184, applied: 184, durable: 192 },
  { id: 2, role: "follower", term: 7, commit: 184, applied: 182, durable: 188 },
  { id: 3, role: "follower", term: 7, commit: 184, applied: 184, durable: 184 },
  { id: 4, role: "follower", term: 7, commit: 180, applied: 179, durable: 181 },
  { id: 5, role: "follower", term: 7, commit: 184, applied: 184, durable: 184 },
];

const markers: EventMarker[] = [
  { t: 0.12, kind: "election", note: "n1 becomes leader for term 7" },
  { t: 0.36, kind: "commit", note: "current-term no-op commits" },
  { t: 0.56, kind: "fault", note: "partition palette is ready" },
  { t: 0.78, kind: "snapshot", note: "checkpoint boundary" },
];

const memoryCheckpoints = [0.12, 0.36, 0.56, 0.78];

function seedFromUrl(): string {
  const query = new URLSearchParams(window.location.hash.replace(/^#/, ""));
  return query.get("seed") ?? "0x000000000000002a";
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
  const [nodes, setNodes] = useState(initialNodes);
  const [selected, setSelected] = useState<number | null>(1);
  const [playing, setPlaying] = useState(false);
  const [partitioned, setPartitioned] = useState(false);
  const [speed, setSpeed] = useState("1×");
  const [seed, setSeed] = useState(seedFromUrl);
  const [profile, setProfile] = useState("rough");
  const [checkpoint, setCheckpoint] = useState(0.42);
  const [shared, setShared] = useState(false);
  const [museumFilter, setMuseumFilter] = useState("all");
  const [museum, setMuseum] = useState<MuseumManifest>({ schema_version: 1, build: "loading", exhibits: [] });
  const selectedNode = nodes.find((node) => node.id === selected) ?? nodes[0];
  const visibleExhibits = museum.exhibits.filter((exhibit) => museumFilter === "all" || exhibit.kind === museumFilter);

  useEffect(() => {
    loadMuseum().then(setMuseum);
    if ("serviceWorker" in navigator) {
      void navigator.serviceWorker.register("./sw.js").catch(() => undefined);
    }
  }, []);

  useEffect(() => {
    if (canvas.current) drawTopology(canvas.current, nodes, selected, partitioned);
    const resize = () => canvas.current && drawTopology(canvas.current, nodes, selected, partitioned);
    window.addEventListener("resize", resize);
    return () => window.removeEventListener("resize", resize);
  }, [nodes, selected, partitioned]);

  useEffect(() => {
    if (!playing) return;
    const timer = window.setInterval(() => {
      setNodes((current) => current.map((node) => ({ ...node, applied: Math.min(node.commit, node.applied + 1) })));
    }, 500);
    return () => window.clearInterval(timer);
  }, [playing]);

  function killLeader() {
    setNodes((current) => current.map((node) => (node.role === "leader" ? { ...node, role: "candidate", term: node.term + 1 } : node)));
    window.setTimeout(() => setNodes((current) => current.map((node, index) => ({ ...node, role: index === 1 ? "leader" : "follower", term: node.term }))), 800);
  }

  function shareScenario() {
    const query = new URLSearchParams({ seed, profile });
    const url = `${window.location.origin}${window.location.pathname}#${query.toString()}`;
    window.history.replaceState({}, "", url);
    void navigator.clipboard?.writeText(url);
    setShared(true);
  }

  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand"><span className="brand-mark">CC</span><div><strong>CRASH COURSE</strong><small>DETERMINISTIC FLIGHT RECORDER</small></div></div>
        <div className="verdict"><span className={`led ${partitioned ? "danger-led" : ""}`} /> {partitioned ? "OPEN" : "SAFE"} <b>0 lost writes</b></div>
        <button className="quiet-button" onClick={() => setPlaying((value) => !value)}>{playing ? "PAUSE" : "PLAY"}</button>
      </header>
      <section className="control-strip">
        <label>SEED<input value={seed} onChange={(event) => setSeed(event.target.value)} aria-label="Seed" /></label>
        <label>PROFILE<select value={profile} onChange={(event) => setProfile(event.target.value)}><option>calm</option><option>gentle</option><option>rough</option><option>brutal</option><option>membership</option></select></label>
        <label>CLUSTER<select defaultValue="5"><option>3 nodes</option><option>5 nodes</option><option>7 nodes</option></select></label>
        <label>SPEED<select value={speed} onChange={(event) => setSpeed(event.target.value)}><option>¼×</option><option>1×</option><option>4×</option><option>16×</option><option>64×</option></select></label>
        <span className="spacer" />
        <button className="outline-button" onClick={() => setPartitioned(false)}>HEAL ALL</button>
        <button className="accent-button" onClick={killLeader}>KILL LEADER <span>k</span></button>
      </section>
      <section className="workspace">
        <aside className="chaos-panel panel">
          <div className="panel-kicker">CHAOS PALETTE</div>
          <button onClick={killLeader}><span className="icon danger">✕</span><span><b>Crash node</b><small>choose a voter</small></span></button>
          <button onClick={() => setPartitioned(true)}><span className="icon warning">╱</span><span><b>Partition</b><small>drag nodes apart</small></span></button>
          <button onClick={() => setPartitioned(false)}><span className="icon calm">⌁</span><span><b>Heal all</b><small>restore every link</small></span></button>
          <div className="palette-divider" />
          <div className="slider-label"><span>PACKET LOSS</span><b>{partitioned ? "18%" : "0%"}</b></div><input type="range" min="0" max="100" value={partitioned ? 18 : 0} readOnly />
          <div className="slider-label"><span>CLOCK SKEW · n{selectedNode.id}</span><b>0 ms</b></div><input type="range" min="-100" max="100" defaultValue="0" />
          <div className="slider-label"><span>DISK LATENCY · n{selectedNode.id}</span><b>1 ms</b></div><input type="range" min="0" max="100" defaultValue="1" />
          <div className="panel-footnote">Every control appends data to the run spec. Share it, replay it, shrink it.</div>
        </aside>
        <section className="topology-panel panel"><div className="panel-heading"><span><span className="panel-kicker">TOPOLOGY / LIVE</span><small>virtual 00:04.82 · {speed}</small></span><span className="status-chip">{partitioned ? "PARTITIONED" : "HEALTHY"}</span></div><canvas ref={canvas} onClick={() => setSelected(selected === 1 ? 2 : 1)} aria-label="Cluster topology" /></section>
        <aside className="inspector panel"><div className="panel-kicker">NODE INSPECTOR</div><div className="node-title"><span className={`role-dot ${selectedNode.role}`} /> n{selectedNode.id}<span className="subtle">{selectedNode.role}</span></div><div className="metric-grid"><Metric label="TERM" value={`t${selectedNode.term}`} /><Metric label="COMMIT" value={`i${selectedNode.commit}`} /><Metric label="APPLIED" value={`i${selectedNode.applied}`} /><Metric label="DURABLE" value={`${selectedNode.durable} rec`} /></div><div className="inspector-section"><div className="section-title">LOG TAIL <span>last 40</span></div><div className="log-tail">{Array.from({ length: 18 }, (_, index) => <i key={index} className={index < 14 ? "committed" : "pending"} style={{ opacity: 0.35 + (index % 5) / 8 }} />)}</div></div><div className="inspector-section"><div className="section-title">EVENT STREAM <span>n{selectedNode.id}</span></div><p className="event-line"><b>04.821</b> Apply <em>i184</em></p><p className="event-line"><b>04.770</b> AppendAck <em>match=184</em></p><p className="event-line"><b>04.650</b> TimerFire <em>heartbeat</em></p></div></aside>
      </section>
      <section className="timeline panel"><div className="timeline-head"><span className="panel-kicker">TIMELINE / RE-EXECUTION</span><span className="timeline-actions"><button onClick={() => setPlaying(false)}>◀</button><button onClick={() => setPlaying((value) => !value)}>{playing ? "Ⅱ" : "▶"}</button><button onClick={() => setPlaying(false)}>▶</button><span>00:04.82 / 00:60.00</span></span></div><div className="timeline-track">{markers.map((marker) => <button key={marker.t} className={`marker ${marker.kind}`} style={{ left: `${marker.t * 100}%` }} title={marker.note} onClick={() => setCheckpoint(marker.t)} />)}<div className="playhead" style={{ left: `${(playing ? 0.48 : checkpoint) * 100}%` }} /></div><div className="timeline-labels"><span>00:00</span><span>election</span><span>commit</span><span>fault</span><span>snapshot</span><span>01:00</span></div><div className="checkpoint-row">MEMORY CHECKPOINTS {memoryCheckpoints.map((value) => <button key={value} onClick={() => setCheckpoint(value)} className={checkpoint === value ? "selected-checkpoint" : ""}>{Math.round(value * 60)}s</button>)}</div></section>
      <section className="museum panel"><div className="timeline-head"><span><span className="panel-kicker">MUSEUM / VERIFIED EXHIBITS</span><small>manifest build {museum.build}</small></span><span className="museum-tools"><select aria-label="Museum category" value={museumFilter} onChange={(event) => setMuseumFilter(event.target.value)}><option value="all">all</option><option value="raft">raft</option><option value="wal">wal</option><option value="store">store</option><option value="membership">membership</option><option value="checker">checker</option></select><span className="status-chip">{visibleExhibits.length} LOADED</span></span></div>{museum.exhibits.length === 0 ? <p className="museum-empty">No verified failure exhibits are published yet. The wing stays empty until a real shrunk trace earns a pinned build.</p> : visibleExhibits.length === 0 ? <p className="museum-empty">No exhibits match this category.</p> : <div className="exhibit-grid">{visibleExhibits.map((exhibit) => <button key={exhibit.id} className="exhibit-card" onClick={() => setSeed(exhibit.seed)}><b>{exhibit.title}</b><small>{exhibit.kind} · {exhibit.verdict} · {exhibit.chapters.length} chapters</small></button>)}</div>}</section>
      <footer><span>THEATER ABI 1 · TRACE v1 · BUILD fixture</span><span>Reduced motion: <button className="link-button">honor system preference</button> · <button className="link-button" onClick={shareScenario}>{shared ? "URL copied ✓" : "Share this scenario ↗"}</button></span></footer>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: string }) { return <div className="metric"><span>{label}</span><b>{value}</b></div>; }

createRoot(document.getElementById("root")!).render(<App />);
