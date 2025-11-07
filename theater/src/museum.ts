// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
export type ExhibitKind = "raft" | "wal" | "store" | "membership" | "checker";
export type ExhibitVerdict = "fixed" | "known-failure" | "inconclusive";

export type Exhibit = {
  id: string;
  title: string;
  kind: ExhibitKind;
  seed: string;
  trace: string;
  verdict: ExhibitVerdict;
  chapters: string[];
};

export type MuseumManifest = {
  schema_version: 1;
  build: string;
  exhibits: Exhibit[];
};

const emptyManifest: MuseumManifest = { schema_version: 1, build: "unknown", exhibits: [] };

export async function loadMuseum(): Promise<MuseumManifest> {
  try {
    const response = await fetch("./exhibits/manifest.json", { cache: "no-store" });
    if (!response.ok) return emptyManifest;
    const value = (await response.json()) as Partial<MuseumManifest>;
    if (value.schema_version !== 1 || !Array.isArray(value.exhibits)) return emptyManifest;
    return {
      schema_version: 1,
      build: typeof value.build === "string" ? value.build : "unknown",
      exhibits: value.exhibits.filter(isExhibit),
    };
  } catch {
    return emptyManifest;
  }
}

function isExhibit(value: unknown): value is Exhibit {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<Exhibit>;
  return typeof candidate.id === "string"
    && typeof candidate.title === "string"
    && typeof candidate.kind === "string"
    && typeof candidate.seed === "string"
    && typeof candidate.trace === "string"
    && typeof candidate.verdict === "string"
    && Array.isArray(candidate.chapters);
}
