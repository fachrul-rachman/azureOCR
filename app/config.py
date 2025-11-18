# app/config.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional


def _get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(key)
    if val is None or str(val).strip() == "":
        return default
    return val


def _parse_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(val: Optional[str], default: int) -> int:
    try:
        return int(val) if val is not None else default
    except Exception:
        return default


def _parse_float(val: Optional[str], default: float) -> float:
    try:
        return float(val) if val is not None else default
    except Exception:
        return default


def load_config(strict_azure: bool = False) -> Dict[str, Any]:
    """
    Membaca konfigurasi dari environment variable (sesuai .env.example).
    Mengembalikan dict agar mudah dipakai di pipeline/endpoint.

    strict_azure=True -> raise ValueError jika ENDPOINT/KEY kosong.
    """
    cfg: Dict[str, Any] = {}

    # ===== Azure Form Recognizer v2.1 =====
    # Wajib diisi
    cfg["ENDPOINT"] = _get_env("FORMRECOGNIZER_ENDPOINT", "")
    cfg["API_KEY"] = _get_env("FORMRECOGNIZER_API_KEY", "")

    # Dikunci ke v2.1 sesuai akun (F0)
    cfg["API_VERSION"] = _get_env("FR_API_VERSION", "v2.1")
    cfg["MODE"] = _get_env("FR_MODE", "layout")  # kita pakai /v2.1/layout/analyze

    # ===== Server (tidak dipakai langsung di pipeline, tapi disimpan untuk referensi) =====
    cfg["SERVER_HOST"] = _get_env("SERVER_HOST", "0.0.0.0")
    cfg["SERVER_PORT"] = _parse_int(_get_env("SERVER_PORT", "8000"), 8000)
    cfg["API_PREFIX"] = _get_env("API_PREFIX", "/api")
    cfg["OCR_ROUTE"] = _get_env("OCR_ROUTE", "/ocr")

    # ===== Pipeline limits & timeouts =====
    # F0: maksimal 2 halaman per request → wajib split fisik per 2 halaman
    cfg["CHUNK_PAGES"] = _parse_int(_get_env("CHUNK_PAGES", "2"), 2)
    cfg["MAX_WORKERS"] = _parse_int(_get_env("MAX_WORKERS", "3"), 3)

    cfg["REQUEST_TIMEOUT_SECONDS"] = _parse_int(_get_env("REQUEST_TIMEOUT_SECONDS", "300"), 300)
    cfg["POLL_INTERVAL_SECONDS"] = _parse_float(_get_env("POLL_INTERVAL_SECONDS", "2"), 2.0)
    cfg["POLL_TIMEOUT_SECONDS"] = _parse_int(_get_env("POLL_TIMEOUT_SECONDS", "180"), 180)

    # ===== PDF handling =====
    cfg["SIZE_THRESHOLD_MB"] = _parse_int(_get_env("SIZE_THRESHOLD_MB", "4"), 4)  # >4MB → compress
    cfg["GHOSTSCRIPT_BIN"] = _get_env("GHOSTSCRIPT_BIN", "auto")  # auto | gs | gswin64c | gswin32c
    cfg["GS_PDFSETTINGS"] = _get_env("GS_PDFSETTINGS", "/ebook")
    cfg["ALLOWED_EXT"] = _get_env("ALLOWED_EXT", "pdf")
    cfg["FILE_MAX_MB"] = _parse_int(_get_env("FILE_MAX_MB", "50"), 50)
    cfg["TMP_DIR"] = _get_env("TMP_DIR", "")  # kosong = pakai dir temp OS

    # ===== Output behavior =====
    # v2.1 tidak punya deteksi bahasa → pakai fallback
    cfg["LANGUAGE_DEFAULT"] = _get_env("LANGUAGE_DEFAULT", "id-ID")
    cfg["TABLE_SPAN_DUPLICATE_CONTENT"] = _parse_bool(
        _get_env("TABLE_SPAN_DUPLICATE_CONTENT", "true"), True
    )
    cfg["CONFIDENCE_PAGE_METHOD"] = _get_env("CONFIDENCE_PAGE_METHOD", "words_avg")

    # ===== Logging =====
    cfg["LOG_LEVEL"] = _get_env("LOG_LEVEL", "INFO")
    cfg["LOG_JSON"] = _parse_bool(_get_env("LOG_JSON", "false"), False)

    # ===== Validasi penting (opsional ketat) =====
    if strict_azure:
        if not cfg["ENDPOINT"] or not cfg["API_KEY"]:
            raise ValueError(
                "FORMRECOGNIZER_ENDPOINT / FORMRECOGNIZER_API_KEY belum diset di environment."
            )
        if cfg["API_VERSION"] != "v2.1":
            raise ValueError("API_VERSION harus 'v2.1' untuk akun F0.")

    return cfg
