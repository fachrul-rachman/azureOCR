# app/pipeline.py
from __future__ import annotations

import logging
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .config import load_config
from .pdf_ops import compress_pdf_if_needed, split_pdf_two_pages
from .azure_client import get_client
from .analyze import analyze_chunks_parallel
from .transform import (
    extract_pages_text_conf,
    extract_tables_from_pages,
    build_output_schema,
)

log = logging.getLogger("azure-ocr.pipeline")


def process_pdf(input_pdf: Path, cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Proses satu PDF:
      1) Kompres jika > threshold (default 4MB, Ghostscript)
      2) Pecah per 2 halaman (F0 v2.1 hanya proses 2 halaman/request)
      3) Kirim tiap chunk paralel ke Azure Form Recognizer v2.1 (layout)
      4) Gabung teks + tabel → JSON final sesuai skema user

    Args:
        input_pdf: path ke PDF sumber
        cfg: hasil load_config(); jika None akan load default

    Returns:
        dict: JSON final
    """
    if cfg is None:
        cfg = load_config()

    input_pdf = Path(input_pdf).resolve()
    if not input_pdf.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {input_pdf}")

    # ------- Direktori kerja sementara -------
    tmp_root = Path(cfg.get("TMP_DIR") or tempfile.gettempdir())
    workdir = Path(tempfile.mkdtemp(prefix="pipeline_", dir=str(tmp_root)))
    try:
        # ------- Kompres (> SIZE_THRESHOLD_MB) -------
        threshold_mb = int(cfg.get("SIZE_THRESHOLD_MB", 4))
        gs_bin = cfg.get("GHOSTSCRIPT_BIN", "auto")
        gs_preset = cfg.get("GS_PDFSETTINGS", "/ebook")
        compressed_pdf = compress_pdf_if_needed(
            input_pdf,
            threshold_mb=threshold_mb,
            gs_bin=gs_bin,
            gs_pdfsettings=gs_preset,
            workdir=workdir / "compress",
        )

        # ------- Split per 2 halaman (F0 v2.1) -------
        chunks_dir = workdir / "chunks"
        chunk_paths: List[Path] = split_pdf_two_pages(compressed_pdf, outdir=chunks_dir)

        # ------- Azure client -------
        endpoint = cfg.get("ENDPOINT") or cfg.get("FORMRECOGNIZER_ENDPOINT")
        api_key = cfg.get("API_KEY") or cfg.get("FORMRECOGNIZER_API_KEY")
        timeout_req = int(cfg.get("REQUEST_TIMEOUT_SECONDS", 300))
        client = get_client(endpoint=endpoint, api_key=api_key, timeout_seconds=timeout_req)

        # ------- Paralel analyze -------
        poll_timeout = int(cfg.get("POLL_TIMEOUT_SECONDS", 180))
        max_workers = int(cfg.get("MAX_WORKERS", 3))
        log.info("Mulai analisis paralel: %d chunk, workers=%d", len(chunk_paths), max_workers)

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            results_map = analyze_chunks_parallel(
                client,
                chunk_paths,
                poll_timeout_seconds=poll_timeout,
                executor_submit=ex.submit,
            )

        # ------- Gabungkan hasil -------
        all_pages: List[Tuple[str, float]] = []
        all_tables: List[Dict[str, Any]] = []
        global_page_offset = 0

        # Hasil dijamin diurutkan dengan mengiterasi index chunk
        for i in range(len(chunk_paths)):
            pages = results_map[i]  # List[FormPage]
            # pages -> (text, confidence)
            page_pairs = extract_pages_text_conf(
                pages,
                confidence_method=str(cfg.get("CONFIDENCE_PAGE_METHOD", "words_avg")),
            )
            all_pages.extend(page_pairs)

            # tables dari pages; page_offset untuk penomoran global
            tbls = extract_tables_from_pages(
                pages,
                page_offset=global_page_offset,
                duplicate_span_content=bool(cfg.get("TABLE_SPAN_DUPLICATE_CONTENT", True)),
            )
            all_tables.extend(tbls)

            global_page_offset += len(page_pairs)

        # ------- Bahasa (fallback, v2.1 tidak deteksi) -------
        language = str(cfg.get("LANGUAGE_DEFAULT", "id-ID"))

        # ------- JSON final -------
        result = build_output_schema(
            file_name=input_pdf.name,
            pages_text_conf=all_pages,
            tables=all_tables,
            language=language,
        )
        return result

    finally:
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception:
            pass
