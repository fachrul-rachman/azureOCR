# app/analyze.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Dict

from azure.core.exceptions import HttpResponseError, ServiceRequestError, ServiceResponseError
from azure.ai.formrecognizer import FormRecognizerClient
from azure.ai.formrecognizer._models import FormPage  # type: ignore

log = logging.getLogger("azure-ocr.analyze")


def analyze_chunk(
    client: FormRecognizerClient,
    pdf_path: Path,
    *,
    poll_timeout_seconds: int = 180,
) -> List[FormPage]:
    """
    Jalankan analisis 'layout' (v2.1) untuk satu file PDF (≤2 halaman).
    Kembalikan daftar FormPage (tiap halaman berisi line/word/table, dsb).

    Args:
        client: FormRecognizerClient (v2.1) dari app.azure_client.get_client()
        pdf_path: path ke file chunk (hasil split ≤2 halaman)
        poll_timeout_seconds: batas waktu tunggu hasil (detik)

    Returns:
        List[FormPage]

    Raises:
        HttpResponseError: error dari layanan (mis. 429 throttling, 400 input invalid)
        ServiceRequestError/ServiceResponseError: error jaringan/transport
        FileNotFoundError: jika file tidak ada
    """
    pdf_path = Path(pdf_path).resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {pdf_path}")

    log.info("Analyze chunk: %s", pdf_path.name)
    try:
        with open(pdf_path, "rb") as f:
            # v2.1 layout API di SDK 3.x
            poller = client.begin_recognize_content(form=f)
            pages: List[FormPage] = poller.result(timeout=poll_timeout_seconds)
            return pages
    except (HttpResponseError, ServiceRequestError, ServiceResponseError):
        # Biarkan pipeline yang memutuskan retry/backoff
        log.exception("Azure analyze gagal untuk %s", pdf_path.name)
        raise


def analyze_chunks_parallel(
    client: FormRecognizerClient,
    chunk_paths: List[Path],
    *,
    poll_timeout_seconds: int = 180,
    executor_submit,
) -> Dict[int, List[FormPage]]:
    """
    Helper untuk dipakai pipeline: kirim banyak chunk secara paralel menggunakan
    ThreadPoolExecutor dari luar (agar kontrol ada di pipeline).

    Args:
        client: FormRecognizerClient (v2.1)
        chunk_paths: daftar path chunk (urut)
        poll_timeout_seconds: timeout per job
        executor_submit: fungsi submit dari ThreadPoolExecutor (ex.submit)

    Returns:
        Dict[index_chunk -> List[FormPage]]
    """
    futures = {}
    for i, p in enumerate(chunk_paths):
        fut = executor_submit(analyze_chunk, client, p, poll_timeout_seconds=poll_timeout_seconds)
        futures[fut] = i

    results: Dict[int, List[FormPage]] = {}
    for fut in list(futures.keys()):
        idx = futures[fut]
        pages = fut.result()  # propagate exception ke caller
        results[idx] = pages
    return results
