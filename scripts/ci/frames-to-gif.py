#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
"""Assemble captured PNG frames into an animated GIF using only the stdlib.

The repository deliberately avoids dependencies, and the only other way to
produce the README animation is ffmpeg, which is not guaranteed to exist on a
contributor's machine. This decodes the 8-bit PNG frames Playwright writes,
reduces them to a shared 256-colour palette by median cut, and emits a looping
GIF89a. It is a build tool, not part of the database.

usage: frames-to-gif.py FRAME_DIR OUTPUT_GIF [--fps N] [--scale N]
"""

from __future__ import annotations

import struct
import sys
import zlib
from pathlib import Path

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def decode_png(path: Path) -> tuple[int, int, bytearray]:
    """Return (width, height, RGB bytes) for an 8-bit truecolour PNG."""
    data = path.read_bytes()
    if not data.startswith(PNG_MAGIC):
        raise ValueError(f"{path}: not a PNG")
    pos = len(PNG_MAGIC)
    width = height = 0
    channels = 0
    idat = bytearray()
    while pos < len(data):
        (length,) = struct.unpack(">I", data[pos : pos + 4])
        kind = data[pos + 4 : pos + 8]
        body = data[pos + 8 : pos + 8 + length]
        pos += 12 + length  # length + type + body + crc
        if kind == b"IHDR":
            width, height, depth, colour = struct.unpack(">IIBB", body[:10])
            if depth != 8 or colour not in (2, 6):
                raise ValueError(f"{path}: only 8-bit RGB/RGBA PNGs are supported")
            channels = 3 if colour == 2 else 4
        elif kind == b"IDAT":
            idat += body
        elif kind == b"IEND":
            break

    raw = zlib.decompress(bytes(idat))
    stride = width * channels
    out = bytearray(width * height * 3)
    previous = bytearray(stride)
    at = 0
    for row in range(height):
        filter_type = raw[at]
        at += 1
        line = bytearray(raw[at : at + stride])
        at += stride
        # PNG filters are defined per byte against the pixel to the left (a),
        # the byte above (b), and above-left (c).
        for index in range(stride):
            a = line[index - channels] if index >= channels else 0
            b = previous[index]
            c = previous[index - channels] if index >= channels else 0
            value = line[index]
            if filter_type == 1:
                value += a
            elif filter_type == 2:
                value += b
            elif filter_type == 3:
                value += (a + b) >> 1
            elif filter_type == 4:
                delta = a + b - c
                da, db, dc = abs(delta - a), abs(delta - b), abs(delta - c)
                value += a if (da <= db and da <= dc) else (b if db <= dc else c)
            line[index] = value & 0xFF
        previous = line
        base = row * width * 3
        for pixel in range(width):
            source = pixel * channels
            out[base + pixel * 3 : base + pixel * 3 + 3] = line[source : source + 3]
    return width, height, out


def downscale(width: int, height: int, rgb: bytearray, factor: int):
    """Box-average by an integer factor so text stays legible."""
    if factor <= 1:
        return width, height, rgb
    new_w, new_h = width // factor, height // factor
    out = bytearray(new_w * new_h * 3)
    area = factor * factor
    for y in range(new_h):
        for x in range(new_w):
            r = g = b = 0
            for dy in range(factor):
                row = ((y * factor + dy) * width + x * factor) * 3
                for dx in range(factor):
                    at = row + dx * 3
                    r += rgb[at]
                    g += rgb[at + 1]
                    b += rgb[at + 2]
            at = (y * new_w + x) * 3
            out[at] = r // area
            out[at + 1] = g // area
            out[at + 2] = b // area
    return new_w, new_h, out


def median_cut(colours: list[tuple[int, int, int]], depth: int) -> list[tuple[int, int, int]]:
    if not colours:
        return [(0, 0, 0)]
    if depth == 0 or len(colours) == 1:
        count = len(colours)
        return [
            (
                sum(c[0] for c in colours) // count,
                sum(c[1] for c in colours) // count,
                sum(c[2] for c in colours) // count,
            )
        ]
    ranges = [max(c[i] for c in colours) - min(c[i] for c in colours) for i in range(3)]
    axis = ranges.index(max(ranges))
    colours.sort(key=lambda c: c[axis])
    middle = len(colours) // 2
    return median_cut(colours[:middle], depth - 1) + median_cut(colours[middle:], depth - 1)


def build_palette(frames: list[tuple[int, int, bytearray]]) -> list[tuple[int, int, int]]:
    seen: set[tuple[int, int, int]] = set()
    for _, _, rgb in frames:
        # Sampling keeps the palette pass fast; the UI is a flat dark theme so
        # a stride of 7 pixels still sees every distinct colour in practice.
        for at in range(0, len(rgb) - 2, 3 * 7):
            seen.add((rgb[at], rgb[at + 1], rgb[at + 2]))
    palette = median_cut(list(seen), 8)
    unique: list[tuple[int, int, int]] = []
    for colour in palette:
        if colour not in unique:
            unique.append(colour)
    while len(unique) < 2:
        unique.append((0, 0, 0))
    # One index is reserved for transparency by the frame differ.
    return unique[:255]


def quantise(rgb: bytearray, palette: list[tuple[int, int, int]], cache: dict) -> bytearray:
    out = bytearray(len(rgb) // 3)
    for index in range(len(out)):
        at = index * 3
        key = (rgb[at], rgb[at + 1], rgb[at + 2])
        entry = cache.get(key)
        if entry is None:
            entry = min(
                range(len(palette)),
                key=lambda i: (palette[i][0] - key[0]) ** 2
                + (palette[i][1] - key[1]) ** 2
                + (palette[i][2] - key[2]) ** 2,
            )
            cache[key] = entry
        out[index] = entry
    return out


def lzw_encode(indices: bytearray, minimum_code_size: int) -> bytes:
    clear_code = 1 << minimum_code_size
    end_code = clear_code + 1
    code_size = minimum_code_size + 1
    table: dict[tuple, int] = {}
    next_code = end_code + 1

    bits = 0
    bit_count = 0
    out = bytearray()

    def emit(code: int) -> None:
        nonlocal bits, bit_count
        bits |= code << bit_count
        bit_count += code_size
        while bit_count >= 8:
            out.append(bits & 0xFF)
            bits >>= 8
            bit_count -= 8

    emit(clear_code)
    prefix: tuple = ()
    for value in indices:
        candidate = prefix + (value,)
        if candidate in table or len(candidate) == 1:
            prefix = candidate
            if len(candidate) == 1:
                continue
        else:
            emit(table[prefix] if len(prefix) > 1 else prefix[0])
            table[candidate] = next_code
            next_code += 1
            if next_code > (1 << code_size) and code_size < 12:
                code_size += 1
            elif next_code >= 4096:
                emit(clear_code)
                table.clear()
                next_code = end_code + 1
                code_size = minimum_code_size + 1
            prefix = (value,)
    if prefix:
        emit(table[prefix] if len(prefix) > 1 else prefix[0])
    emit(end_code)
    if bit_count:
        out.append(bits & 0xFF)

    blocked = bytearray()
    for start in range(0, len(out), 255):
        chunk = out[start : start + 255]
        blocked.append(len(chunk))
        blocked += chunk
    blocked.append(0)
    return bytes(blocked)


def diff_frame(previous: bytearray, current: bytearray, width: int, transparent: int):
    """Reduce a frame to the rectangle that changed, blanking untouched pixels.

    A dashboard is mostly static between frames, so emitting only the dirty
    rectangle with everything else transparent shrinks the file dramatically.
    """
    left, right, top, bottom = width, -1, len(current) // width, -1
    for index, value in enumerate(current):
        if value != previous[index]:
            y, x = divmod(index, width)
            if x < left:
                left = x
            if x > right:
                right = x
            if y < top:
                top = y
            if y > bottom:
                bottom = y
    if right < 0:
        return None
    box_w = right - left + 1
    box_h = bottom - top + 1
    out = bytearray(box_w * box_h)
    for y in range(box_h):
        source = (top + y) * width + left
        target = y * box_w
        for x in range(box_w):
            value = current[source + x]
            out[target + x] = value if value != previous[source + x] else transparent
    return left, top, box_w, box_h, out


def write_gif(path: Path, width: int, height: int, frames, palette, delay_cs: int) -> None:
    transparent = len(palette)
    bits = max(1, transparent.bit_length())
    table_size = 1 << bits
    out = bytearray(b"GIF89a")
    out += struct.pack("<HHBBB", width, height, 0xF0 | (bits - 1), 0, 0)
    for index in range(table_size):
        colour = palette[index] if index < len(palette) else (0, 0, 0)
        out += bytes(colour)
    out += b"\x21\xff\x0bNETSCAPE2.0\x03\x01\x00\x00\x00"  # loop forever
    minimum = max(2, bits)
    previous = None
    for indices in frames:
        if previous is None:
            region = (0, 0, width, height, indices)
            packed = 0x00
        else:
            diff = diff_frame(previous, indices, width, transparent)
            if diff is None:
                # Nothing moved; extend the previous frame's delay instead.
                continue
            region = diff
            packed = 0x05  # disposal "do not dispose" + transparency flag
        left, top, box_w, box_h, payload = region
        out += b"\x21\xf9\x04" + bytes([packed])
        out += struct.pack("<H", delay_cs) + bytes([transparent]) + b"\x00"
        out += b"\x2c" + struct.pack("<HHHHB", left, top, box_w, box_h, 0)
        out += bytes([minimum])
        out += lzw_encode(payload, minimum)
        previous = indices
    out += b"\x3b"
    path.write_bytes(bytes(out))


def main() -> int:
    args = sys.argv[1:]
    if len(args) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    frame_dir, output = Path(args[0]), Path(args[1])
    fps = 12
    scale = 2
    for index, value in enumerate(args):
        if value == "--fps" and index + 1 < len(args):
            fps = int(args[index + 1])
        if value == "--scale" and index + 1 < len(args):
            scale = int(args[index + 1])

    paths = sorted(frame_dir.glob("frame-*.png"))
    if not paths:
        print(f"frames-to-gif: no frames in {frame_dir}", file=sys.stderr)
        return 1

    decoded = []
    for path in paths:
        width, height, rgb = decode_png(path)
        decoded.append(downscale(width, height, rgb, scale))
    width, height = decoded[0][0], decoded[0][1]
    if any(frame[0] != width or frame[1] != height for frame in decoded):
        print("frames-to-gif: frames differ in size", file=sys.stderr)
        return 1

    palette = build_palette(decoded)
    cache: dict = {}
    indexed = [quantise(rgb, palette, cache) for _, _, rgb in decoded]
    output.parent.mkdir(parents=True, exist_ok=True)
    write_gif(output, width, height, indexed, palette, max(1, round(100 / fps)))
    print(
        f"frames-to-gif: PASS frames={len(indexed)} size={width}x{height} "
        f"colours={len(palette)} bytes={output.stat().st_size} output={output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
