# app/transform.py
from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple


# ================== helpers geometri ==================

def _avg(values: Sequence[float]) -> float:
    return (sum(values) / len(values)) if values else 0.0


def _iter_points(bbox_obj) -> List[Tuple[float, float]]:
    """
    Robust extractor: FR v2.1 biasanya memberi list of Point(x,y).
    Kita dukung juga tuple/list [x,y].
    """
    pts: List[Tuple[float, float]] = []
    if not bbox_obj:
        return pts
    for p in bbox_obj:
        x = getattr(p, "x", None)
        y = getattr(p, "y", None)
        if x is not None and y is not None:
            pts.append((float(x), float(y)))
        else:
            try:
                x2 = float(p[0])  # type: ignore[index]
                y2 = float(p[1])  # type: ignore[index]
                pts.append((x2, y2))
            except Exception:
                # skip bentuk yang tidak dikenali
                pass
    return pts


def _rect_from_points(points: List[Tuple[float, float]]) -> Tuple[float, float, float, float] | None:
    if not points:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return (min(xs), min(ys), max(xs), max(ys))  # (xmin, ymin, xmax, ymax)


def _rect_contains_point(rect: Tuple[float, float, float, float], px: float, py: float) -> bool:
    xmin, ymin, xmax, ymax = rect
    return (xmin <= px <= xmax) and (ymin <= py <= ymax)


def _rect_intersects(r1: Tuple[float, float, float, float], r2: Tuple[float, float, float, float]) -> bool:
    ax1, ay1, ax2, ay2 = r1
    bx1, by1, bx2, by2 = r2
    return not (ax2 < bx1 or bx2 < ax1 or ay2 < by1 or by2 < ay1)


def _centroid(points: List[Tuple[float, float]]) -> Tuple[float, float] | None:
    if not points:
        return None
    sx = sum(p[0] for p in points)
    sy = sum(p[1] for p in points)
    n = len(points)
    return (sx / n, sy / n)


# ================== extraction utama ==================

def extract_pages_text_conf(
    pages: List[Any],
    *,
    confidence_method: str = "words_avg",
    exclude_table_text: bool = True,
) -> List[Tuple[str, float]]:
    """
    Ekstraksi teks & confidence per halaman.
    - text = gabungan FormLine.text
    - Jika exclude_table_text=True, baris/word yang berada DI DALAM area tabel dihapus
      agar tidak duplikat dengan output "tables".
    - confidence = rata-rata confidence word dari teks yang dipertahankan.
    """
    results: List[Tuple[str, float]] = []

    for page in pages or []:
        # --- bangun daftar rect sel tabel untuk halaman ini ---
        table_cell_rects: List[Tuple[float, float, float, float]] = []
        if exclude_table_text:
            for tbl in getattr(page, "tables", None) or []:
                for cell in getattr(tbl, "cells", None) or []:
                    rb = _rect_from_points(_iter_points(getattr(cell, "bounding_box", None)))
                    if rb:
                        table_cell_rects.append(rb)

        # --- siapkan kolektor teks dan confidence ---
        kept_line_texts: List[str] = []
        kept_word_confs: List[float] = []

        lines = getattr(page, "lines", None) or []
        for ln in lines:
            line_text = getattr(ln, "text", None)
            if not isinstance(line_text, str) or not line_text.strip():
                continue

            # bbox & centroid line
            ln_pts = _iter_points(getattr(ln, "bounding_box", None))
            ln_rect = _rect_from_points(ln_pts)
            ln_ctr = _centroid(ln_pts)

            # tentukan apakah line dianggap "di dalam tabel"
            in_table = False
            if exclude_table_text and table_cell_rects:
                # 1) cek centroid berada di salah satu sel
                if ln_ctr:
                    for rc in table_cell_rects:
                        if _rect_contains_point(rc, ln_ctr[0], ln_ctr[1]):
                            in_table = True
                            break

                # 2) kalau belum terdeteksi, cek interseksi kasar bbox
                if not in_table and ln_rect:
                    for rc in table_cell_rects:
                        if _rect_intersects(rc, ln_rect):
                            in_table = True
                            break

            if in_table:
                # lewati line ini beserta word-nya
                continue

            kept_line_texts.append(line_text)

            if confidence_method == "words_avg":
                # ambil confidence dari words milik line yang dipertahankan
                for w in getattr(ln, "words", None) or []:
                    conf = getattr(w, "confidence", None)
                    if conf is None:
                        continue
                    try:
                        kept_word_confs.append(float(conf))
                    except Exception:
                        pass

        page_text = "\n".join(kept_line_texts).strip()
        page_conf = _avg(kept_word_confs) if confidence_method == "words_avg" else 0.0
        results.append((page_text, round(page_conf, 6)))

    return results


def _dup_span_into_matrix(
    rows: List[List[str]],
    base_r: int,
    base_c: int,
    row_span: int,
    col_span: int,
    content: str,
) -> None:
    max_r = len(rows)
    max_c = len(rows[0]) if rows else 0
    for dr in range(max(1, row_span)):
        for dc in range(max(1, col_span)):
            rr = base_r + dr
            cc = base_c + dc
            if 0 <= rr < max_r and 0 <= cc < max_c:
                rows[rr][cc] = content


def extract_tables_from_pages(
    pages: List[Any],
    *,
    page_offset: int = 0,
    duplicate_span_content: bool = True,
) -> List[Dict[str, Any]]:
    """
    Konversi tabel dari FormPage → list dict:
      { table_ref, page, rows, confidence }
    """
    out: List[Dict[str, Any]] = []

    for p_idx, page in enumerate(pages or []):
        page_num = int(page_offset + p_idx + 1)
        page_tables = getattr(page, "tables", None) or []
        if not page_tables:
            continue

        for t_idx, tbl in enumerate(page_tables):
            row_count = int(getattr(tbl, "row_count", 0) or 0)
            col_count = int(getattr(tbl, "column_count", 0) or 0)

            if row_count <= 0 or col_count <= 0:
                # fallback jika ukuran tidak tersedia
                cells = getattr(tbl, "cells", None) or []
                max_r = max([int(getattr(c, "row_index", 0) or 0) for c in cells], default=-1)
                max_c = max([int(getattr(c, "column_index", 0) or 0) for c in cells], default=-1)
                row_count = max(0, max_r + 1)
                col_count = max(0, max_c + 1)

            rows: List[List[str]] = [["" for _ in range(col_count)] for __ in range(row_count)]
            cell_confs: List[float] = []

            for cell in getattr(tbl, "cells", None) or []:
                r = int(getattr(cell, "row_index", 0) or 0)
                c = int(getattr(cell, "column_index", 0) or 0)
                rs = int(getattr(cell, "row_span", 1) or 1)
                cs = int(getattr(cell, "column_span", 1) or 1)
                content = getattr(cell, "text", "") or ""

                if 0 <= r < row_count and 0 <= c < col_count:
                    rows[r][c] = content

                if duplicate_span_content and (rs > 1 or cs > 1):
                    _dup_span_into_matrix(rows, r, c, rs, cs, content)

                conf = getattr(cell, "confidence", None)
                if conf is not None:
                    try:
                        cell_confs.append(float(conf))
                    except Exception:
                        pass

            tbl_conf = _avg(cell_confs)
            out.append(
                {
                    "table_ref": f"p{page_num}-t{t_idx + 1}",
                    "page": page_num,
                    "rows": rows,
                    "confidence": round(tbl_conf, 6),
                }
            )
    return out


def build_output_schema(
    file_name: str,
    pages_text_conf: List[Tuple[str, float]],
    tables: List[Dict[str, Any]],
    language: str,
) -> Dict[str, Any]:
    return {
        "document": {"file_name": file_name, "language": language or ""},
        "data": {"pages": [{"text": t, "confidence": float(f"{conf:.6f}")} for t, conf in pages_text_conf]},
        "tables": tables or [],
    }
