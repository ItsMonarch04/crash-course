// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
export function Metric({ label, value }: { label: string; value: string }) {
	return (
		<div className="metric">
			<span>{label}</span>
			<b>{value}</b>
		</div>
	);
}
