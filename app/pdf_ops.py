# app/pdf_ops.py
from __future__ import annotations

import logging
import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional

from pypdf import PdfReader, PdfWriter

__all__ = [
    "compress_pdf_if_needed",
    "count_pages",
    "split_pdf_two_pages",
]

log = logging.getLogger("azure-ocr.pdf_ops")


# ---------- Utilities ----------

def _which(candidate: str) -> Optional[str]:
    """Wrapper untuk shutil.which agar mudah di-mock saat testing."""
    return shutil.which(candidate)


def _select_ghostscript_bin(preference: str = "auto") -> Optional[str]:
    """
    Pilih executable Ghostscript berdasarkan OS & preferensi.
    - preference bisa berupa "auto", "gs", "gswin64c", "gswin32c", atau path absolut.
    - return None jika tidak ditemukan.
    """
    if not preference or preference == "auto":
        system = platform.system().lower()
        candidates = ["gs"]
        if system.startswith("win"):
            candidates = ["gswin64c", "gs"]  # urutan yang umum di Windows
        for cand in candidates:
            path = _which(cand)
            if path:
                return path
        return None

    # Jika user memberi nama exe spesifik / path absolut
    if os.path.isabs(preference):
        return preference if Path(preference).exists() else None

    found = _which(preference)
    return found


def _mb_to_bytes(mb: int) -> int:
    return mb * 1024 * 1024


def _ensure_dir(d: Path) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------- Public API ----------

def compress_pdf_if_needed(
    src: Path,
    threshold_mb: int = 4,
    gs_bin: str = "auto",
    gs_pdfsettings: str = "/ebook",
    workdir: Optional[Path] = None,
    timeout_sec: int = 180,
) -> Path:
    """
    Kompres PDF menggunakan Ghostscript jika ukuran file > threshold_mb.
    - Jika Ghostscript tidak tersedia / kompresi gagal / hasil lebih besar,
      fungsi ini akan MENGEMBALIKAN file sumber apa adanya (tidak error).
    - Output disimpan di workdir (jika diberikan) atau temp dir OS.

    Args:
        src: Path ke PDF sumber.
        threshold_mb: Batas ukuran (MB) untuk memicu kompresi.
        gs_bin: "auto" | "gs" | "gswin64c" | "gswin32c" | path absolut.
        gs_pdfsettings: Preset Ghostscript (/screen | /ebook | /printer | /prepress).
        workdir: Direktori output (opsional).
        timeout_sec: Batas waktu eksekusi Ghostscript.

    Returns:
        Path ke file hasil (bisa sama dengan src jika tidak dikompres).
    """
    src = Path(src).resolve()
    if not src.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {src}")

    size_bytes = src.stat().st_size
    if size_bytes <= _mb_to_bytes(threshold_mb):
        log.debug("Skip kompresi: ukuran <= threshold (%d MB).", threshold_mb)
        return src

    gs_exec = _select_ghostscript_bin(gs_bin)
    if not gs_exec:
        log.warning("Ghostscript tidak ditemukan (pref=%s). Melewati kompresi.", gs_bin)
        return src

    if workdir:
        _ensure_dir(workdir)
        out_dir = workdir
    else:
        out_dir = Path(tempfile.mkdtemp(prefix="gs_pdf_"))

    out_path = out_dir / f"{src.stem}.compressed.pdf"

    cmd = [
        gs_exec,
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        f"-dPDFSETTINGS={gs_pdfsettings}",
        "-dNOPAUSE",
        "-dQUIET",
        "-dBATCH",
        f"-sOutputFile={str(out_path)}",
        str(src),
    ]

    log.info("Menjalankan Ghostscript: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, timeout=timeout_sec)
    except subprocess.CalledProcessError as e:
        log.warning("Ghostscript gagal (exit=%s). Melewati kompresi.", e.returncode)
        return src
    except subprocess.TimeoutExpired:
        log.warning("Ghostscript timeout %ss. Melewati kompresi.", timeout_sec)
        return src
    except Exception as e:
        log.warning("Ghostscript error: %s. Melewati kompresi.", e)
        return src

    # Validasi hasil
    if not out_path.exists():
        log.warning("Output kompresi tidak ditemukan. Melewati kompresi.")
        return src

    out_size = out_path.stat().st_size
    if out_size >= size_bytes:
        log.info("Hasil kompresi >= ukuran asli (%.2fMB vs %.2fMB). Pakai file asli.",
                 out_size / (1024 * 1024), size_bytes / (1024 * 1024))
        # Hapus hasil yang tidak bermanfaat
        try:
            out_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        return src

    log.info("Kompresi sukses: %.2fMB -> %.2fMB",
             size_bytes / (1024 * 1024), out_size / (1024 * 1024))
    return out_path


def count_pages(src: Path) -> int:
    """
    Hitung jumlah halaman PDF.
    Raise exception jika file rusak/tidak dapat dibaca oleh pypdf.
    """
    src = Path(src).resolve()
    if not src.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {src}")
    reader = PdfReader(str(src))
    return len(reader.pages)


def split_pdf_two_pages(
    src: Path,
    outdir: Optional[Path] = None,
    prefix: Optional[str] = None,
) -> List[Path]:
    """
    Pecah PDF menjadi potongan file per 2 halaman (fisik), untuk kompatibel dengan tier F0 v2.1.

    Args:
        src: PDF sumber.
        outdir: Direktori output. Jika None → gunakan temp dir OS.
        prefix: Prefix nama file keluaran (default: <src.stem>).

    Returns:
        Daftar Path file potongan (urut sesuai halaman).

    Catatan:
        - Fungsi ini tidak memodifikasi file sumber.
        - Nama file output: {prefix or src.stem}.p{start}-{end}.pdf
    """
    src = Path(src).resolve()
    if not src.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {src}")

    reader = PdfReader(str(src))
    total = len(reader.pages)
    if total == 0:
        raise ValueError("PDF tidak memiliki halaman.")

    if outdir is None:
        outdir = Path(tempfile.mkdtemp(prefix="pdf_chunks_"))
    else:
        _ensure_dir(outdir)

    name_prefix = prefix or src.stem
    chunk_paths: List[Path] = []

    # Ambil 2 halaman per chunk
    for start in range(0, total, 2):
        end = min(start + 2, total)
        writer = PdfWriter()
        for i in range(start, end):
            writer.add_page(reader.pages[i])
        out_path = outdir / f"{name_prefix}.p{start + 1}-{end}.pdf"
        with open(out_path, "wb") as f:
            writer.write(f)
        chunk_paths.append(out_path)

    return chunk_paths
