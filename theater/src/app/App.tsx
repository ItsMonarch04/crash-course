// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "../styles/tokens.css";
import { emptyManifest, loadMuseum, type MuseumManifest } from "../museum";
import { Metric } from "../panels/Metric";
import { useSim, type WasmModule, type WasmRuntime } from "../state/useSim";
import wasmInit, {
	checkpoint as wasmCheckpoint,
	drop_checkpoint as wasmDropCheckpoint,
	init as wasmCreate,
	inject as wasmInject,
	restore as wasmRestore,
	state as wasmStateJson,
	step as wasmStep,
	trace_hash as wasmTraceHash,
	trace_page as wasmTracePage,
	type SimHandle,
} from "../wasm/cc_wasm.js";

type Role = "follower" | "candidate" | "leader";
type NodeState = {
	id: number;
	role: Role;
	term: number;
	commit: number;
	applied: number;
	durable: number;
};
type EventMarker = { seq: number; t: number; kind: string; note: string };

type TraceEvent = {
	seq: number;
	time_ns: number;
	node: number | null;
	kind: string;
	payload_hex: string;
};
type TraceFixture = { trace_version: number; seed: string; events: TraceEvent[] };
type WasmNode = {
	id: number;
	status: string;
	role: Role;
	term: number;
	commit: number;
	applied: number;
	durable_bytes: number;
	disk_service_delay_ms: number;
	clock_offset_ms: number;
	log_tail: number[];
};
type WasmState = {
	theater_abi: number;
	virtual_time_ns: number;
	history_len: number;
	completed_operations: number;
	had_leader: boolean;
	link_drop_percent: number;
	checkpoint_count: number;
	checkpoint_bytes: number;
	nodes: WasmNode[];
};
type FaultSpec = {
	action: string;
	node?: number;
	to?: number;
	offset_ms?: number;
	latency_ms?: number;
	drop_percent?: number;
};
type LessonName = "free" | "figure8" | "asymmetric" | "herd" | "snapshot";
type MotionPreference = "system" | "on" | "off";

const CLUSTER_SIZES = [3, 5, 7] as const;
const DEFAULT_CLUSTER_SIZE = 5;
const SPEED_STEP_NS: Record<string, number> = {
	"¼×": 125_000_000,
	"1×": 500_000_000,
	"4×": 2_000_000_000,
	"16×": 8_000_000_000,
	"64×": 32_000_000_000,
};
const CHECKPOINT_INTERVAL_NS = 5_000_000_000;

function readTrace(module: WasmModule, handle: SimHandle): TraceFixture {
	const events: TraceEvent[] = [];
	let cursor = 0n;
	for (;;) {
		const page = JSON.parse(module.tracePage(handle, cursor, 512)) as {
			events: TraceEvent[];
			next_cursor: number;
			done: boolean;
		};
		events.push(...page.events);
		const next = BigInt(page.next_cursor);
		if (page.done) break;
		if (next <= cursor) throw new Error("non-advancing trace cursor");
		cursor = next;
	}
	return { trace_version: 1, seed: "", events };
}

function advanceWithCheckpoints(runtime: WasmRuntime, targetNs: number): WasmState {
	let state = JSON.parse(runtime.module.state(runtime.handle)) as WasmState;
	while (state.virtual_time_ns < targetNs) {
		const nextBoundary = Math.min(
			targetNs,
			(Math.floor(state.virtual_time_ns / CHECKPOINT_INTERVAL_NS) + 1) * CHECKPOINT_INTERVAL_NS,
		);
		state = JSON.parse(
			runtime.module.step(runtime.handle, BigInt(nextBoundary - state.virtual_time_ns)),
		) as WasmState;
		if (
			state.virtual_time_ns % CHECKPOINT_INTERVAL_NS === 0 &&
			!runtime.checkpoints.has(state.virtual_time_ns)
		) {
			runtime.checkpoints.set(state.virtual_time_ns, runtime.module.checkpoint(runtime.handle));
			state = JSON.parse(runtime.module.state(runtime.handle)) as WasmState;
		}
	}
	return state;
}

function storedMotionPreference(): MotionPreference {
	try {
		const stored = window.localStorage.getItem("crash-course-motion");
		return stored === "on" || stored === "off" ? stored : "system";
	} catch {
		return "system";
	}
}

function nextMotionPreference(value: MotionPreference): MotionPreference {
	return value === "system" ? "on" : value === "on" ? "off" : "system";
}

function emptyNodes(size: number): NodeState[] {
	return Array.from({ length: size }, (_, index) => ({
		id: index + 1,
		role: "follower" as Role,
		term: 0,
		commit: 0,
		applied: 0,
		durable: 0,
	}));
}

function clusterSizeFromUrl(): number {
	const raw = Number(new URLSearchParams(window.location.hash.replace(/^#/, "")).get("nodes"));
	return CLUSTER_SIZES.includes(raw as (typeof CLUSTER_SIZES)[number]) ? raw : DEFAULT_CLUSTER_SIZE;
}

function hexText(value: string): string {
	return value.replace(/../g, (part) => String.fromCharCode(Number.parseInt(part, 16)));
}

function deriveNodes(trace: TraceFixture | null, size: number): NodeState[] {
	if (!trace || trace.events.length === 0) return emptyNodes(size);
	const nodes = new Map(emptyNodes(size).map((node) => [node.id, { ...node }]));
	trace.events.forEach((event) => {
		if (event.node === null) return;
		const current = nodes.get(event.node) ?? {
			id: event.node,
			role: "follower",
			term: 0,
			commit: 0,
			applied: 0,
			durable: 0,
		};
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
			seq: event.seq,
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

function embeddedFromUrl(): boolean {
	return new URLSearchParams(window.location.hash.replace(/^#/, "")).get("embed") === "1";
}

const LESSONS: Record<
	Exclude<LessonName, "free">,
	{ title: string; chapter: string; action: FaultSpec }
> = {
	figure8: {
		title: "Figure-8 reconstruction",
		chapter:
			"Isolate one voter, let the majority advance, then heal. Watch the old prefix yield to the committed one.",
		action: { action: "partition", node: 1 },
	},
	asymmetric: {
		title: "Asymmetric election",
		chapter:
			"Cut one voter from the majority. Terms can advance on the minority while commits remain quorum-bound.",
		action: { action: "partition", node: 2 },
	},
	herd: {
		title: "Thundering herd",
		chapter:
			"Crash the current leader. Randomized deterministic timers prevent every survivor from winning at once.",
		action: { action: "crash", node: 1 },
	},
	snapshot: {
		title: "Snapshot catch-up",
		chapter:
			"Slow a follower disk while the log advances, then restore it and observe state transfer.",
		action: { action: "slow-disk", node: 3, latency_ms: 80 },
	},
};

const NODE_RADIUS = 25;

// Single source of truth for where a node sits. The renderer and the click
// hit-test both read it, so a node can never be drawn somewhere the pointer
// cannot reach.
function topologyLayout(width: number, height: number, count: number) {
	const center = { x: width * 0.48, y: height * 0.51 };
	const radius = Math.min(width, height) * 0.31;
	const points = Array.from({ length: count }, (_, index) => {
		const angle = -Math.PI / 2 + (index / count) * Math.PI * 2;
		return { x: center.x + Math.cos(angle) * radius, y: center.y + Math.sin(angle) * radius };
	});
	return { center, points };
}

function nodeAtPoint(
	canvas: HTMLCanvasElement,
	nodes: NodeState[],
	clientX: number,
	clientY: number,
): number | null {
	const bounds = canvas.getBoundingClientRect();
	const { points } = topologyLayout(bounds.width, bounds.height, nodes.length);
	const x = clientX - bounds.left;
	const y = clientY - bounds.top;
	const hits = points
		.map((point, index) => ({
			id: nodes[index].id,
			distance: Math.hypot(point.x - x, point.y - y),
		}))
		.filter((hit) => hit.distance <= NODE_RADIUS + 8)
		.sort((left, right) => left.distance - right.distance);
	return hits.length > 0 ? hits[0].id : null;
}

function drawTopology(
	canvas: HTMLCanvasElement,
	nodes: NodeState[],
	selected: number | null,
	partitioned: boolean,
) {
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
	const { center, points } = topologyLayout(width, height, nodes.length);
	points.forEach((point, index) => {
		const leader = nodes[index].role === "leader";
		context.strokeStyle = partitioned && index === 0 ? "#ed6a5a" : "#334354";
		context.lineWidth = partitioned && index === 0 ? 3 : 1;
		context.beginPath();
		context.moveTo(center.x, center.y);
		context.lineTo(point.x, point.y);
		context.stroke();
		context.fillStyle = leader
			? "#58d6b2"
			: nodes[index].role === "candidate"
				? "#f2b84b"
				: "#8998aa";
		context.beginPath();
		context.arc(
			point.x,
			point.y,
			selected === nodes[index].id ? NODE_RADIUS + 6 : NODE_RADIUS,
			0,
			Math.PI * 2,
		);
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

export default function App() {
	const canvas = useRef<HTMLCanvasElement>(null);
	const { runtime: wasmRuntime, installRuntime, disposeRuntime, liveHandles } = useSim();
	const [trace, setTrace] = useState<TraceFixture | null>(null);
	const [wasmState, setWasmState] = useState<WasmState | null>(null);
	const [engineFailed, setEngineFailed] = useState(false);
	const [virtualTime, setVirtualTime] = useState(0);
	const [clusterSize, setClusterSize] = useState(clusterSizeFromUrl);
	const [nodes, setNodes] = useState<NodeState[]>(() => emptyNodes(clusterSizeFromUrl()));
	const [selected, setSelected] = useState<number | null>(1);
	const [playing, setPlaying] = useState(false);
	const [partitioned, setPartitioned] = useState(false);
	const [speed, setSpeed] = useState("1×");
	const [seed, setSeed] = useState(seedFromUrl);
	const [profile, setProfile] = useState(profileFromUrl);
	const [faults, setFaults] = useState<FaultSpec[]>(faultsFromUrl);
	const [checkpoint, setCheckpoint] = useState(0.42);
	const [shared, setShared] = useState(false);
	const [determinism, setDeterminism] = useState<"idle" | "running" | "match" | "diverged">("idle");
	const [traceHash, setTraceHash] = useState("");
	const [lesson, setLesson] = useState<LessonName>("free");
	const [embedded, setEmbedded] = useState(embeddedFromUrl);
	const [museumFilter, setMuseumFilter] = useState("all");
	const [museum, setMuseum] = useState<MuseumManifest>({ ...emptyManifest, build: "loading" });
	const [museumError, setMuseumError] = useState("");
	const [motionPreference, setMotionPreference] =
		useState<MotionPreference>(storedMotionPreference);
	const [systemReducedMotion, setSystemReducedMotion] = useState(
		() => window.matchMedia("(prefers-reduced-motion: reduce)").matches,
	);
	const [roleAnnouncement, setRoleAnnouncement] = useState("");
	const [faultAnnouncement, setFaultAnnouncement] = useState("");
	const [checkpointTimes, setCheckpointTimes] = useState<number[]>([]);
	const [simError, setSimError] = useState("");
	const [lastRestoreFrom, setLastRestoreFrom] = useState(0);
	const [lastReplayNs, setLastReplayNs] = useState(0);
	const scrubGeneration = useRef(0);
	const roleFingerprint = nodes.map((node) => `${node.id}:${node.role}`).join(",");
	const previousRoleFingerprint = useRef<string | null>(null);
	const reducedMotion =
		motionPreference === "on" || (motionPreference === "system" && systemReducedMotion);
	const markers = useMemo(() => deriveMarkers(trace), [trace]);
	const memoryCheckpoints = useMemo(
		() => checkpointTimes.map((time) => time / 60_000_000_000),
		[checkpointTimes],
	);
	const recentEvents = useMemo(
		() =>
			trace?.events
				.filter((event) => event.node === selected)
				.slice(-3)
				.reverse() ?? [],
		[trace, selected],
	);
	const ackedWrites = trace?.events.filter((event) => event.kind === "ClientOk").length ?? 0;
	const lostWrites = trace?.events.filter((event) => event.kind === "ClientTimeout").length ?? 0;
	const selectedNode =
		nodes.find((node) => node.id === selected) ?? nodes[0] ?? emptyNodes(clusterSize)[0];
	const leaderNode = nodes.find((node) => node.role === "leader");
	const packetLossPercent = wasmState?.link_drop_percent ?? 0;
	const diskLatencyMs =
		wasmState?.nodes.find((node) => node.id === selectedNode.id)?.disk_service_delay_ms ?? 0;
	const clockSkewMs =
		wasmState?.nodes.find((node) => node.id === selectedNode.id)?.clock_offset_ms ?? 0;
	const visibleExhibits = museum.exhibits.filter(
		(exhibit) => museumFilter === "all" || exhibit.kind === museumFilter,
	);
	const maxScrubSeconds = wasmRuntime.current
		? Math.min(60, ((checkpointTimes.at(-1) ?? 0) + CHECKPOINT_INTERVAL_NS) / 1_000_000_000)
		: 0;

	useEffect(() => {
		const update = () => setEmbedded(embeddedFromUrl());
		window.addEventListener("hashchange", update);
		return () => window.removeEventListener("hashchange", update);
	}, []);

	useEffect(() => {
		const media = window.matchMedia("(prefers-reduced-motion: reduce)");
		const update = () => setSystemReducedMotion(media.matches);
		media.addEventListener("change", update);
		return () => media.removeEventListener("change", update);
	}, []);

	useEffect(() => {
		try {
			window.localStorage.setItem("crash-course-motion", motionPreference);
		} catch {
			// Storage can be unavailable in private or embedded contexts. The live
			// preference still applies for this page lifetime.
		}
	}, [motionPreference]);

	useEffect(() => {
		if (reducedMotion) setPlaying(false);
	}, [reducedMotion]);

	useEffect(() => {
		if (previousRoleFingerprint.current && previousRoleFingerprint.current !== roleFingerprint) {
			const leader = nodes.find((node) => node.role === "leader");
			setRoleAnnouncement(
				leader
					? `Role change: node ${leader.id} is leader.`
					: "Role change: no leader is currently elected.",
			);
		}
		previousRoleFingerprint.current = roleFingerprint;
	}, [nodes, roleFingerprint]);

	useEffect(() => {
		let cancelled = false;
		let candidate: WasmRuntime | null = null;
		disposeRuntime();
		const sharedFaults = faultsFromUrl();
		setFaults(sharedFaults);
		setMuseumError("");
		loadMuseum()
			.then((loaded) => {
				if (!cancelled) setMuseum(loaded);
			})
			.catch((error: unknown) => {
				if (!cancelled) {
					setMuseum(emptyManifest);
					setMuseumError(
						`Museum import failed. ${error instanceof Error ? error.message : String(error)}`,
					);
				}
			});
		fetch(`./fixtures/seed-${seed.replace(/^0x/, "")}.json`)
			.then((response) =>
				response.ok
					? (response.json() as Promise<TraceFixture>)
					: Promise.reject(new Error("fixture not found")),
			)
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
					checkpoint: wasmCheckpoint,
					restore: wasmRestore,
					dropCheckpoint: wasmDropCheckpoint,
					traceHash: wasmTraceHash,
					tracePage: wasmTracePage,
				};
				// Document-relative, like `./exhibits/manifest.json` and
				// `./sw.js`. The site is served from a project subpath
				// (`/crash-course/`), where a root-absolute URL resolves
				// off the deployment and the engine never loads.
				await module.default("./wasm/cc_wasm_bg.wasm");
				const handle = module.init(JSON.stringify({ seed, profile, nodes: clusterSize }));
				sharedFaults.forEach((action) => {
					module.inject(handle, JSON.stringify(action));
				});
				const state = JSON.parse(module.state(handle)) as WasmState;
				if (state.theater_abi !== 2)
					throw new Error(`unsupported Theater ABI ${state.theater_abi}`);
				const checkpoints = new Map<number, bigint>();
				checkpoints.set(state.virtual_time_ns, module.checkpoint(handle));
				candidate = { module, handle, checkpoints };
				if (!cancelled) {
					installRuntime(candidate);
					setWasmState(state);
					setTrace(readTrace(module, handle));
					setCheckpointTimes([...checkpoints.keys()]);
					setEngineFailed(false);
					setSimError("");
				} else {
					disposeRuntime(candidate);
					candidate = null;
				}
			} catch (error) {
				disposeRuntime(candidate);
				candidate = null;
				if (!cancelled) {
					setWasmState(null);
					setEngineFailed(true);
					setSimError(
						`The simulator failed to initialize; controls are paused. ${error instanceof Error ? error.message : String(error)}`,
					);
				}
			}
		})();
		if ("serviceWorker" in navigator) {
			void navigator.serviceWorker.register("./sw.js").catch(() => undefined);
		}
		return () => {
			cancelled = true;
			disposeRuntime(candidate ?? wasmRuntime.current);
			candidate = null;
			setCheckpointTimes([]);
		};
	}, [seed, profile, clusterSize, disposeRuntime, installRuntime, wasmRuntime]);

	useEffect(() => {
		setNodes(wasmNodes(wasmState) ?? deriveNodes(trace, clusterSize));
	}, [trace, wasmState, clusterSize]);

	// Selecting a node that a smaller cluster no longer has would leave the
	// inspector and every targeted fault pointing at nothing.
	useEffect(() => {
		setSelected((current) => (current !== null && current <= clusterSize ? current : 1));
	}, [clusterSize]);

	useEffect(() => {
		if (canvas.current) drawTopology(canvas.current, nodes, selected, partitioned);
		const resize = () =>
			canvas.current && drawTopology(canvas.current, nodes, selected, partitioned);
		window.addEventListener("resize", resize);
		return () => window.removeEventListener("resize", resize);
	}, [nodes, selected, partitioned]);

	useEffect(() => {
		if (!playing) return;
		const stepNs = SPEED_STEP_NS[speed] ?? SPEED_STEP_NS["1×"];
		const timer = window.setInterval(() => {
			const runtime = wasmRuntime.current;
			if (runtime) {
				try {
					const current = JSON.parse(runtime.module.state(runtime.handle)) as WasmState;
					const state = advanceWithCheckpoints(
						runtime,
						Math.min(60_000_000_000, current.virtual_time_ns + stepNs),
					);
					setWasmState(state);
					setTrace(readTrace(runtime.module, runtime.handle));
					setCheckpointTimes([...runtime.checkpoints.keys()].sort((left, right) => left - right));
					setVirtualTime(Math.min(1, state.virtual_time_ns / 60_000_000_000));
					if (state.virtual_time_ns >= 60_000_000_000) setPlaying(false);
				} catch (error) {
					setPlaying(false);
					setSimError(error instanceof Error ? error.message : String(error));
					disposeRuntime();
					setWasmState(null);
					setEngineFailed(true);
				}
			} else {
				setVirtualTime((current) => Math.min(1, current + 0.02));
			}
		}, 500);
		return () => window.clearInterval(timer);
	}, [playing, speed, disposeRuntime, wasmRuntime]);

	const inject = useCallback(
		(action: FaultSpec) => {
			const runtime = wasmRuntime.current;
			if (!runtime) return;
			try {
				runtime.module.inject(runtime.handle, JSON.stringify(action));
				const state = JSON.parse(runtime.module.state(runtime.handle)) as WasmState;
				for (const [time, id] of runtime.checkpoints) {
					runtime.module.dropCheckpoint(runtime.handle, id);
					runtime.checkpoints.delete(time);
				}
				runtime.checkpoints.set(state.virtual_time_ns, runtime.module.checkpoint(runtime.handle));
				setFaults((current) => [...current, action]);
				setWasmState(state);
				setTrace(readTrace(runtime.module, runtime.handle));
				setCheckpointTimes([...runtime.checkpoints.keys()]);
				setFaultAnnouncement(
					`Fault completed: ${action.action}${action.node ? ` on node ${action.node}` : ""}.`,
				);
				setSimError("");
			} catch (error) {
				setPlaying(false);
				setSimError(error instanceof Error ? error.message : String(error));
				disposeRuntime(runtime);
				setWasmState(null);
				setEngineFailed(true);
			}
		},
		[disposeRuntime, wasmRuntime],
	);

	const killLeader = useCallback(() => {
		if (leaderNode) inject({ action: "crash", node: leaderNode.id });
	}, [leaderNode, inject]);

	useEffect(() => {
		const onKeyDown = (event: KeyboardEvent) => {
			const target = event.target;
			if (
				target instanceof HTMLInputElement ||
				target instanceof HTMLSelectElement ||
				target instanceof HTMLTextAreaElement
			)
				return;
			if (event.key.toLowerCase() === "k") killLeader();
			if (event.key === " " && !reducedMotion && wasmState) {
				event.preventDefault();
				setPlaying((value) => !value);
			}
		};
		window.addEventListener("keydown", onKeyDown);
		return () => window.removeEventListener("keydown", onKeyDown);
	}, [reducedMotion, wasmState, killLeader]);

	// Step to the previous/next event marker. These buttons used to be labelled
	// as step controls while both merely paused, which made the timeline look
	// navigable when it was not.
	function stepToMarker(direction: -1 | 1) {
		setPlaying(false);
		const here = playing ? virtualTime : checkpoint;
		const stops = [0, ...markers.map((marker) => marker.t), 1].sort((left, right) => left - right);
		const epsilon = 1e-6;
		const target =
			direction === 1
				? stops.find((stop) => stop > here + epsilon)
				: [...stops].reverse().find((stop) => stop < here - epsilon);
		if (target !== undefined) scrubTo(target);
	}

	function scrubTo(progress: number) {
		setCheckpoint(progress);
		const generation = ++scrubGeneration.current;
		queueMicrotask(() => {
			if (generation !== scrubGeneration.current) return;
			const runtime = wasmRuntime.current;
			if (!runtime) return;
			try {
				const targetNs = Math.round(progress * 60_000_000_000);
				const nearest = [...runtime.checkpoints.keys()]
					.filter((time) => time <= targetNs)
					.sort((left, right) => right - left)[0];
				if (nearest === undefined || targetNs - nearest > CHECKPOINT_INTERVAL_NS) {
					throw new Error(
						"No complete checkpoint is available within the five-second replay horizon.",
					);
				}
				const checkpointId = runtime.checkpoints.get(nearest);
				if (checkpointId === undefined) {
					throw new Error(
						"No complete checkpoint is available within the five-second replay horizon.",
					);
				}
				runtime.module.restore(runtime.handle, checkpointId);
				const state = advanceWithCheckpoints(runtime, targetNs);
				setWasmState(state);
				setTrace(readTrace(runtime.module, runtime.handle));
				setCheckpointTimes([...runtime.checkpoints.keys()].sort((left, right) => left - right));
				setVirtualTime(Math.min(1, state.virtual_time_ns / 60_000_000_000));
				setLastRestoreFrom(nearest);
				setLastReplayNs(targetNs - nearest);
				setSimError("");
			} catch (error) {
				setPlaying(false);
				setSimError(error instanceof Error ? error.message : String(error));
				disposeRuntime(runtime);
				setWasmState(null);
				setEngineFailed(true);
			}
		});
	}

	function shareScenario() {
		const query = new URLSearchParams({
			seed,
			profile,
			run_spec: JSON.stringify({ seed, profile, faults }),
		});
		const url = `${window.location.origin}${window.location.pathname}#${query.toString()}`;
		window.history.replaceState({}, "", url);
		void navigator.clipboard?.writeText(url);
		setShared(true);
	}

	function chooseLesson(value: LessonName) {
		setLesson(value);
		if (value === "free") return;
		const preset = LESSONS[value];
		if (value === "herd") {
			killLeader();
		} else {
			inject(preset.action);
			if (value === "figure8" || value === "asymmetric") setPartitioned(true);
		}
	}

	async function proveDeterminism() {
		const runtime = wasmRuntime.current;
		if (!runtime) return;
		setDeterminism("running");
		const targetNs = wasmState?.virtual_time_ns ?? 0;
		const traces: string[] = [];
		const currentTrace = runtime.module.traceHash(runtime.handle);
		for (let pass = 0; pass < 2; pass += 1) {
			const handle = runtime.module.init(JSON.stringify({ seed, profile }));
			const passRuntime: WasmRuntime = { module: runtime.module, handle, checkpoints: new Map() };
			try {
				faults.forEach((action) => {
					runtime.module.inject(handle, JSON.stringify(action));
				});
				advanceWithCheckpoints(passRuntime, targetNs);
				traces.push(runtime.module.traceHash(handle));
			} finally {
				disposeRuntime(passRuntime);
			}
		}
		const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(traces[0]));
		setTraceHash(
			[...new Uint8Array(digest)]
				.map((byte) => byte.toString(16).padStart(2, "0"))
				.join("")
				.slice(0, 12),
		);
		setDeterminism(traces[0] === traces[1] && traces[0] === currentTrace ? "match" : "diverged");
	}

	return (
		<main className={`shell${embedded ? " embed" : ""}${reducedMotion ? " reduce-motion" : ""}`}>
			<header className="topbar">
				<div className="brand">
					<span className="brand-mark">CC</span>
					<div>
						<strong>CRASH COURSE</strong>
						<small>DETERMINISTIC FLIGHT RECORDER</small>
					</div>
				</div>
				<div className="verdict">
					<span className={`led ${partitioned || lostWrites > 0 ? "danger-led" : ""}`} />{" "}
					{partitioned ? "OPEN" : "SAFE"}{" "}
					<b>
						{ackedWrites} acked · {lostWrites} lost
					</b>
				</div>
				<span className="engine-state" data-testid="engine-state">
					{wasmState
						? "LIVE SIM"
						: engineFailed
							? "ENGINE UNAVAILABLE — RECORDED TRACE"
							: "STARTING"}
				</span>
				<span className="engine-state" data-testid="leader-id">
					{nodes.find((node) => node.role === "leader")?.id ?? "none"}
				</span>
				<button
					type="button"
					className="quiet-button"
					data-control="play"
					disabled={reducedMotion || !wasmState}
					title={
						reducedMotion
							? "Autoplay is disabled by the motion preference."
							: !wasmState
								? "The simulator is not ready."
								: undefined
					}
					onClick={() => setPlaying((value) => !value)}
				>
					{playing ? "PAUSE" : "PLAY"}
				</button>
				<button
					type="button"
					className="quiet-button"
					data-control="motion-preference"
					aria-label={`Motion preference: ${motionPreference}`}
					onClick={() => setMotionPreference(nextMotionPreference(motionPreference))}
				>
					MOTION {motionPreference.toUpperCase()}
				</button>
			</header>
			{simError && (
				<p className="sim-error" role="alert">
					SIMULATOR PAUSED · {simError}
				</p>
			)}
			<span className="visually-hidden" data-testid="live-wasm-handles">
				{liveHandles}
			</span>
			<section className="control-strip">
				<label>
					SEED
					<input
						data-control="seed"
						value={seed}
						onChange={(event) => setSeed(event.target.value)}
						aria-label="Seed"
					/>
				</label>
				<label>
					PROFILE
					<select
						data-control="profile"
						aria-label="Profile"
						value={profile}
						onChange={(event) => setProfile(event.target.value)}
					>
						<option>calm</option>
						<option>gentle</option>
						<option>rough</option>
						<option>brutal</option>
						<option>membership</option>
					</select>
				</label>
				<label>
					LESSON
					<select
						data-control="lesson"
						aria-label="Lesson"
						value={lesson}
						onChange={(event) => chooseLesson(event.target.value as LessonName)}
					>
						<option value="free">free explore</option>
						<option value="figure8">figure-8</option>
						<option value="asymmetric">asymmetric</option>
						<option value="herd">thundering herd</option>
						<option value="snapshot">snapshot catch-up</option>
					</select>
				</label>
				<label>
					CLUSTER
					<select
						data-control="cluster-size"
						aria-label="Cluster size"
						value={String(clusterSize)}
						onChange={(event) => setClusterSize(Number(event.target.value))}
					>
						{CLUSTER_SIZES.map((size) => (
							<option key={size} value={size}>
								{size} nodes
							</option>
						))}
					</select>
				</label>
				<label>
					SPEED
					<select
						data-control="speed"
						aria-label="Speed"
						value={speed}
						onChange={(event) => setSpeed(event.target.value)}
					>
						<option>¼×</option>
						<option>1×</option>
						<option>4×</option>
						<option>16×</option>
						<option>64×</option>
					</select>
				</label>
				<span className="spacer" />
				<button
					type="button"
					className="outline-button"
					data-control="heal-all"
					disabled={!partitioned || !wasmState}
					title={!partitioned ? "No partition is active." : undefined}
					onClick={() => {
						setPartitioned(false);
						inject({ action: "heal" });
					}}
				>
					HEAL ALL
				</button>
				<button
					type="button"
					className="outline-button"
					data-control="determinism-proof"
					onClick={() => void proveDeterminism()}
					data-testid="determinism-proof"
				>
					{determinism === "idle"
						? "RUN TWICE"
						: determinism === "running"
							? "CHECKING…"
							: determinism === "match"
								? `MATCH ${traceHash}`
								: "DIVERGED"}
				</button>
				<button
					type="button"
					className="accent-button"
					data-control="kill-leader"
					disabled={!leaderNode || !wasmState}
					title={!leaderNode ? "No elected leader can be crashed." : undefined}
					onClick={killLeader}
				>
					KILL LEADER <span>k</span>
				</button>
			</section>
			<section className="workspace">
				<aside className="chaos-panel panel">
					<div className="panel-kicker">CHAOS PALETTE</div>
					<button
						type="button"
						data-control="crash-selected"
						disabled={!wasmState}
						title={!wasmState ? "The simulator is not ready." : undefined}
						onClick={() => inject({ action: "crash", node: selectedNode.id })}
					>
						<span className="icon danger">✕</span>
						<span>
							<b>Crash node</b>
							<small>choose a voter</small>
						</span>
					</button>
					<button
						type="button"
						data-control="partition-selected"
						onClick={() => {
							setPartitioned(true);
							inject({ action: "partition", node: selectedNode.id });
						}}
					>
						<span className="icon warning">╱</span>
						<span>
							<b>Partition</b>
							<small>drag nodes apart</small>
						</span>
					</button>
					<button
						type="button"
						data-control="heal-palette"
						disabled={!partitioned || !wasmState}
						title={!partitioned ? "No partition is active." : undefined}
						onClick={() => {
							setPartitioned(false);
							inject({ action: "heal" });
						}}
					>
						<span className="icon calm">⌁</span>
						<span>
							<b>Heal all</b>
							<small>restore every link</small>
						</span>
					</button>
					<div className="palette-divider" />
					<div className="slider-label">
						<span>
							PACKET LOSS · n{selectedNode.id} → n
							{selectedNode.id === clusterSize ? 1 : selectedNode.id + 1}
						</span>
						<b data-testid="packet-loss-value">{packetLossPercent}%</b>
					</div>
					<input
						data-control="packet-loss"
						type="range"
						min="0"
						max="100"
						aria-label="Packet loss"
						value={packetLossPercent}
						onChange={(event) => {
							const dropPercent = Number(event.target.value);
							const to = selectedNode.id === clusterSize ? 1 : selectedNode.id + 1;
							inject({
								action: "link-degrade",
								node: selectedNode.id,
								to,
								drop_percent: dropPercent,
							});
						}}
					/>
					<div className="slider-label">
						<span>CLOCK SKEW · n{selectedNode.id}</span>
						<b data-testid="clock-skew-value">{clockSkewMs} ms</b>
					</div>
					<input
						data-control="clock-skew"
						type="range"
						min="0"
						max="100"
						aria-label="Clock skew"
						value={clockSkewMs}
						onChange={(event) =>
							inject({
								action: "clock-skew",
								node: selectedNode.id,
								offset_ms: Number(event.target.value),
							})
						}
					/>
					<div className="slider-label">
						<span>DISK LATENCY · n{selectedNode.id}</span>
						<b data-testid="disk-latency-value">{diskLatencyMs} ms</b>
					</div>
					<input
						data-control="disk-latency"
						type="range"
						min="0"
						max="5000"
						aria-label="Disk latency"
						value={diskLatencyMs}
						onChange={(event) =>
							inject({
								action: "slow-disk",
								node: selectedNode.id,
								latency_ms: Number(event.target.value),
							})
						}
					/>
					<div className="panel-footnote">
						Every control appends data to the run spec. Share it, replay it, shrink it.
					</div>
				</aside>
				<section className="topology-panel panel">
					<div className="panel-heading">
						<span>
							<span className="panel-kicker">TOPOLOGY / LIVE</span>
							<small>
								virtual{" "}
								{Math.round(virtualTime * 60)
									.toString()
									.padStart(2, "0")}
								s · {speed}
							</small>
						</span>
						<span className="status-chip">{partitioned ? "PARTITIONED" : "HEALTHY"}</span>
					</div>
					{lesson !== "free" && (
						<div className="lesson-overlay">
							<b>{LESSONS[lesson].title}</b>
							<span>{LESSONS[lesson].chapter}</span>
							<button type="button" data-control="take-controls" onClick={() => setLesson("free")}>
								TAKE THE CONTROLS
							</button>
						</div>
					)}
					<canvas
						ref={canvas}
						onClick={(event) => {
							const hit =
								canvas.current && nodeAtPoint(canvas.current, nodes, event.clientX, event.clientY);
							if (hit) setSelected(hit);
						}}
						aria-label="Cluster topology"
						aria-describedby="topology-description topology-mirror"
					/>
					<p id="topology-description" className="visually-hidden">
						Live canvas topology. Use the selected node control for keyboard node selection; the
						live node table contains the same state.
					</p>
					<div id="topology-mirror" className="topology-mirror">
						<table>
							<caption>Live node state</caption>
							<thead>
								<tr>
									<th>Node</th>
									<th>Role</th>
									<th>Term</th>
									<th>Commit</th>
									<th>Applied</th>
									<th>Durable</th>
								</tr>
							</thead>
							<tbody>
								{nodes.map((node) => (
									<tr key={node.id}>
										<th scope="row">n{node.id}</th>
										<td>{node.role}</td>
										<td>{node.term}</td>
										<td>{node.commit}</td>
										<td>{node.applied}</td>
										<td>{node.durable}</td>
									</tr>
								))}
							</tbody>
						</table>
					</div>
					<output className="visually-hidden" aria-live="polite">
						{roleAnnouncement} {faultAnnouncement}
					</output>
				</section>
				<aside className="inspector panel">
					<div className="panel-kicker">NODE INSPECTOR</div>
					<label className="node-select">
						SELECTED NODE
						<select
							data-control="selected-node"
							aria-label="Selected node"
							value={selectedNode.id}
							onChange={(event) => setSelected(Number(event.target.value))}
						>
							{nodes.map((node) => (
								<option key={node.id} value={node.id}>
									node {node.id}
								</option>
							))}
						</select>
					</label>
					<div className="node-title">
						<span className={`role-dot ${selectedNode.role}`} /> n{selectedNode.id}
						<span className="subtle" data-testid="selected-role">
							{selectedNode.role}
						</span>
					</div>
					<div className="metric-grid">
						<Metric label="TERM" value={`t${selectedNode.term}`} />
						<Metric label="COMMIT" value={`i${selectedNode.commit}`} />
						<Metric label="APPLIED" value={`i${selectedNode.applied}`} />
						<Metric label="DURABLE" value={`${selectedNode.durable} rec`} />
					</div>
					<div className="inspector-section">
						<div className="section-title">
							LOG TAIL <span>captured</span>
						</div>
						<div className="log-tail">
							{Array.from({ length: Math.max(8, selectedNode.durable) }, (_, index) => (
								<i
									// biome-ignore lint/suspicious/noArrayIndexKey: decorative ticks have no identity other than position
									key={index}
									className={index < selectedNode.applied ? "committed" : "pending"}
									style={{ opacity: 0.35 + (index % 5) / 8 }}
								/>
							))}
						</div>
					</div>
					<div className="inspector-section">
						<div className="section-title">
							EVENT STREAM <span>n{selectedNode.id}</span>
						</div>
						{recentEvents.length === 0 ? (
							<p className="event-line">
								<b>—</b> no events captured
							</p>
						) : (
							recentEvents.map((event) => (
								<p className="event-line" key={event.seq}>
									<b>{(event.time_ns / 1e9).toFixed(3)}</b> {event.kind} <em>seq={event.seq}</em>
								</p>
							))
						)}
					</div>
				</aside>
			</section>
			<section className="timeline panel">
				<div className="timeline-head">
					<span className="panel-kicker">TIMELINE / RE-EXECUTION</span>
					<span className="timeline-actions">
						<button
							type="button"
							data-control="previous-event"
							aria-label="Previous event"
							title="Previous event"
							onClick={() => stepToMarker(-1)}
						>
							◀
						</button>
						<button
							type="button"
							data-control="timeline-play"
							disabled={reducedMotion || !wasmState}
							title={reducedMotion ? "Autoplay is disabled by the motion preference." : undefined}
							aria-label={playing ? "Pause timeline" : "Play timeline"}
							onClick={() => setPlaying((value) => !value)}
						>
							{playing ? "Ⅱ" : "▶"}
						</button>
						<button
							type="button"
							data-control="next-event"
							aria-label="Next event"
							title="Next event"
							onClick={() => stepToMarker(1)}
						>
							▶
						</button>
						<span data-testid="virtual-time">{Math.round(virtualTime * 60)}s / 60s</span>
					</span>
				</div>
				<div className="timeline-track">
					<input
						data-control="timeline"
						className="timeline-range"
						type="range"
						min="0"
						max={maxScrubSeconds}
						step="1"
						aria-label="Timeline"
						aria-valuemax={maxScrubSeconds}
						aria-valuetext={`${Math.round((playing ? virtualTime : checkpoint) * 60)} virtual seconds`}
						value={Math.min(maxScrubSeconds, Math.round((playing ? virtualTime : checkpoint) * 60))}
						onChange={(event) => scrubTo(Number(event.target.value) / 60)}
					/>
					{markers.map((marker) => (
						<button
							type="button"
							data-control="timeline-marker"
							key={marker.seq}
							className={`marker ${marker.kind}`}
							style={{ left: `${marker.t * 100}%` }}
							aria-label={marker.note}
							title={marker.note}
							onClick={() => scrubTo(marker.t)}
						/>
					))}
					<div
						className="playhead"
						style={{ left: `${(playing ? virtualTime : checkpoint) * 100}%` }}
					/>
				</div>
				<span className="visually-hidden" data-testid="restore-from-ns">
					{lastRestoreFrom}
				</span>
				<span className="visually-hidden" data-testid="replay-ns">
					{lastReplayNs}
				</span>
				<div className="timeline-labels">
					<span>00:00</span>
					<span>captured events</span>
					<span>60:00</span>
				</div>
				<div className="checkpoint-row">
					MEMORY CHECKPOINTS{" "}
					{memoryCheckpoints.length === 0 ? (
						<span className="subtle">none captured</span>
					) : (
						memoryCheckpoints.map((value) => (
							<button
								type="button"
								data-control="checkpoint"
								key={value}
								onClick={() => scrubTo(value)}
								className={checkpoint === value ? "selected-checkpoint" : ""}
							>
								{Math.round(value * 60)}s
							</button>
						))
					)}
				</div>
			</section>
			<section className="museum panel">
				<div className="timeline-head">
					<span>
						<span className="panel-kicker">MUSEUM / VERIFIED EXHIBITS</span>
						<small>
							manifest build {museum.build} · ABI {museum.theater_abi}
						</small>
					</span>
					<span className="museum-tools">
						<select
							data-control="museum-category"
							aria-label="Museum category"
							value={museumFilter}
							onChange={(event) => setMuseumFilter(event.target.value)}
						>
							<option value="all">all</option>
							<option value="raft">raft</option>
							<option value="wal">wal</option>
							<option value="store">store</option>
							<option value="membership">membership</option>
							<option value="checker">checker</option>
						</select>
						<span className="status-chip">{visibleExhibits.length} LOADED</span>
					</span>
				</div>
				{museumError ? (
					<p className="museum-empty" role="alert">
						{museumError}
					</p>
				) : museum.exhibits.length === 0 ? (
					<p className="museum-empty">
						No verified failure exhibits are published yet. The wing stays empty until a real shrunk
						trace earns a pinned build.
					</p>
				) : visibleExhibits.length === 0 ? (
					<p className="museum-empty">No exhibits match this category.</p>
				) : (
					<div className="exhibit-grid">
						{visibleExhibits.map((exhibit) => (
							<button
								type="button"
								data-control="museum-exhibit"
								key={exhibit.id}
								className="exhibit-card"
								onClick={() => setSeed(exhibit.seed)}
							>
								<b>{exhibit.title}</b>
								<small>
									{exhibit.kind} · {exhibit.verdict} · {exhibit.anomaly} · ABI {exhibit.theater_abi}
									{exhibit.readonly ? " read-only" : ""} · {exhibit.chapters.length} chapters
								</small>
							</button>
						))}
					</div>
				)}
			</section>
			<footer>
				<span>THEATER ABI 2 · TRACE v1 · BUILD fixture</span>
				<span>
					Keyboard: <kbd>Tab</kbd> controls · <kbd>Space</kbd> play/pause · <kbd>K</kbd> kill leader
					· motion {motionPreference} ·{" "}
					<button
						type="button"
						data-control="share"
						className="link-button"
						onClick={shareScenario}
					>
						{shared ? "URL copied ✓" : "Share this scenario ↗"}
					</button>
				</span>
			</footer>
		</main>
	);
}
