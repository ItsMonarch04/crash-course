// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
export type ExhibitKind = "raft" | "wal" | "store" | "membership" | "checker";
export type ExhibitVerdict = "fixed" | "known-failure" | "inconclusive";

const DEFAULT_HORIZON_NS = 60_000_000_000;
const DEFAULT_CHECKPOINT_INTERVAL_NS = 5_000_000_000;
export const MAX_MUSEUM_CHECKPOINT_COUNT = 13;

export type Exhibit = {
	id: string;
	title: string;
	kind: ExhibitKind;
	seed: string;
	trace: string;
	verdict: ExhibitVerdict;
	anomaly: string;
	chapters: string[];
	theater_abi: 1 | 2;
	horizon_ns: number;
	checkpoint_interval_ns: number;
	readonly: boolean;
};

export type MuseumManifest = {
	schema_version: 1 | 2;
	theater_abi: 1 | 2;
	build: string;
	exhibits: Exhibit[];
};

export const emptyManifest: MuseumManifest = {
	schema_version: 2,
	theater_abi: 2,
	build: "unknown",
	exhibits: [],
};

/** Parse at the trust boundary so incompatible captures never look like an empty wing. */
export function parseMuseum(value: unknown): MuseumManifest {
	if (!value || typeof value !== "object") throw new Error("Museum manifest is not an object.");
	const manifest = value as Record<string, unknown>;
	if (manifest.synthetic === true) {
		throw new Error("Synthetic artifacts belong to the kata wing, not the museum.");
	}
	const schema = manifest.schema_version;
	if (schema !== 1 && schema !== 2) {
		throw new Error(`Unsupported museum schema ${String(schema)}.`);
	}
	const manifestAbi = manifest.theater_abi ?? schema;
	if (manifestAbi !== 1 && manifestAbi !== 2) {
		throw new Error(`Unsupported Theater ABI ${String(manifestAbi)} in museum manifest.`);
	}
	if (!Array.isArray(manifest.exhibits)) throw new Error("Museum exhibits must be an array.");

	return {
		schema_version: schema,
		theater_abi: manifestAbi,
		build:
			typeof manifest.build === "string" && manifest.build.length > 0 ? manifest.build : "unknown",
		exhibits: manifest.exhibits.map((candidate, index) =>
			adaptExhibit(candidate, manifestAbi, index),
		),
	};
}

export async function loadMuseum(): Promise<MuseumManifest> {
	const response = await fetch("./exhibits/manifest.json", { cache: "no-store" });
	if (!response.ok) throw new Error(`Museum manifest request failed with HTTP ${response.status}.`);
	return parseMuseum(await response.json());
}

function adaptExhibit(value: unknown, manifestAbi: 1 | 2, index: number): Exhibit {
	if (!isRawExhibit(value)) throw new Error(`Museum exhibit ${index} is malformed.`);
	const candidate = value as Record<string, unknown>;
	if (candidate.synthetic === true) {
		throw new Error(`Synthetic artifact ${String(candidate.id)} is not a museum exhibit.`);
	}
	const abi = candidate.theater_abi ?? manifestAbi;
	if (abi !== 1 && abi !== 2) {
		throw new Error(`Unsupported Theater ABI ${String(abi)} in museum exhibit ${value.id}.`);
	}

	// ABI 1 captures predate explicit replay bounds. The compatibility adapter
	// supplies the historical built-in 60s/5s contract and never makes them
	// executable or writable through ABI 2.
	const horizon = abi === 1 ? DEFAULT_HORIZON_NS : candidate.horizon_ns;
	const interval = abi === 1 ? DEFAULT_CHECKPOINT_INTERVAL_NS : candidate.checkpoint_interval_ns;
	if (!isPositiveSafeInteger(horizon) || !isPositiveSafeInteger(interval)) {
		throw new Error(
			`Museum exhibit ${value.id} must declare a finite horizon and checkpoint interval.`,
		);
	}
	const checkpointCount = Math.ceil(horizon / interval) + 1;
	if (checkpointCount > MAX_MUSEUM_CHECKPOINT_COUNT) {
		throw new Error(
			`Museum exhibit ${value.id} requires ${checkpointCount} checkpoints; regenerate it with a shorter horizon or larger interval (maximum ${MAX_MUSEUM_CHECKPOINT_COUNT}).`,
		);
	}

	return {
		...value,
		anomaly: typeof value.anomaly === "string" ? value.anomaly : "unclassified",
		theater_abi: abi,
		horizon_ns: horizon,
		checkpoint_interval_ns: interval,
		readonly: abi === 1,
	};
}

function isPositiveSafeInteger(value: unknown): value is number {
	return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isRawExhibit(
	value: unknown,
): value is Omit<
	Exhibit,
	"anomaly" | "theater_abi" | "horizon_ns" | "checkpoint_interval_ns" | "readonly"
> & { anomaly?: string } {
	if (!value || typeof value !== "object") return false;
	const candidate = value as Partial<Exhibit>;
	return (
		typeof candidate.id === "string" &&
		typeof candidate.title === "string" &&
		["raft", "wal", "store", "membership", "checker"].includes(String(candidate.kind)) &&
		typeof candidate.seed === "string" &&
		typeof candidate.trace === "string" &&
		["fixed", "known-failure", "inconclusive"].includes(String(candidate.verdict)) &&
		(candidate.anomaly === undefined || typeof candidate.anomaly === "string") &&
		Array.isArray(candidate.chapters) &&
		candidate.chapters.every((chapter) => typeof chapter === "string")
	);
}
