# main.py
from __future__ import annotations

import os
import io
import sys
import json
import tempfile
import shutil
import logging
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from dotenv import load_dotenv
import uvicorn

# ===== Load .env as early as possible =====
load_dotenv(dotenv_path=Path(__file__).resolve().with_name(".env"), override=True)

# ===== Local imports (project modules) =====
try:
    from app.config import load_config  # type: ignore
    from app.pipeline import process_pdf  # type: ignore
except Exception as e:
    # Berikan pesan error yang jelas jika modul belum ada.
    print(
        "[ERROR] Modul app.config atau app.pipeline belum tersedia. "
        "Pastikan struktur folder sudah sesuai dan modul sudah dibuat.",
        file=sys.stderr,
    )
    raise

# ===== Helpers =====
def parse_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

def parse_int(val: Optional[str], default: int) -> int:
    try:
        return int(val) if val is not None else default
    except Exception:
        return default

def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(key, default)

def get_allowed_ext() -> List[str]:
    exts = get_env("ALLOWED_EXT", "pdf")
    parts = [x.strip().lower().lstrip(".") for x in exts.split(",") if x.strip()]
    return parts or ["pdf"]

def make_logger() -> logging.Logger:
    level_str = get_env("LOG_LEVEL", "INFO") or "INFO"
    level = getattr(logging, level_str.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger("azure-ocr")

log = make_logger()

# ===== App factory =====
def create_app() -> FastAPI:
    api_prefix = get_env("API_PREFIX", "/api") or "/api"
    ocr_route = get_env("OCR_ROUTE", "/ocr") or "/ocr"
    cors_origins_raw = get_env("CORS_ORIGINS", "*") or "*"

    app = FastAPI(title="Azure OCR v2.1 – Minimal API", version="1.0.0")

    # ----- CORS -----
    if cors_origins_raw.strip() == "*":
        allow_origins = ["*"]
    else:
        allow_origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ----- Health -----
    @app.get(f"{api_prefix}/health")
    def health():
        return {"status": "ok", "api_version": get_env("FR_API_VERSION", "v2.1")}

    # ----- OCR endpoint -----
    @app.post(f"{api_prefix}{ocr_route}")
    async def ocr(file: UploadFile = File(...)) -> JSONResponse:
        # Load config (gabungan env & default) untuk pipeline
        cfg = load_config()

        # Validasi dasar
        max_mb = parse_int(get_env("FILE_MAX_MB", "50"), 50)
        allowed_ext = get_allowed_ext()

        filename = file.filename or "upload.pdf"
        suffix = filename.split(".")[-1].lower() if "." in filename else ""
        if suffix not in allowed_ext:
            raise HTTPException(
                status_code=400,
                detail=f"File ekstensi .{suffix or '(unknown)'} tidak diizinkan. "
                       f"Izinkan: {', '.join(allowed_ext)}"
            )

        # Terkadang Content-Type bisa kosong/umum; kita tetap izinkan selama ekstensi benar
        if file.content_type not in (None, "", "application/pdf", "application/octet-stream"):
            # izinkan selain itu jika ekstensi valid, tapi beri peringatan di log
            log.warning(f"Non-standard content-type: {file.content_type}")

        # Simpan upload ke file sementara (streaming chunk)
        tmp_dir_env = get_env("TMP_DIR") or ""
        base_tmp = Path(tmp_dir_env) if tmp_dir_env else Path(tempfile.gettempdir())
        base_tmp.mkdir(parents=True, exist_ok=True)

        tmp_dir = Path(tempfile.mkdtemp(prefix="ocr_req_", dir=str(base_tmp)))
        tmp_pdf_path = tmp_dir / filename

        size_bytes = 0
        try:
            with open(tmp_pdf_path, "wb") as out:
                while True:
                    chunk = await file.read(1024 * 1024)  # 1MB per chunk
                    if not chunk:
                        break
                    out.write(chunk)
                    size_bytes += len(chunk)
        finally:
            await file.close()

        # Validasi ukuran
        size_mb = size_bytes / (1024 * 1024)
        if size_mb > max_mb:
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            finally:
                pass
            raise HTTPException(
                status_code=400,
                detail=f"File terlalu besar: {size_mb:.2f} MB (maks {max_mb} MB)."
            )

        # Jalankan pipeline di threadpool (sinkron) agar tidak block event loop
        try:
            log.info(f"Processing file: {filename} ({size_mb:.2f} MB)")
            result_dict = await run_in_threadpool(process_pdf, tmp_pdf_path, cfg)
        except TimeoutError as te:
            log.error(f"Azure timeout: {te}")
            # 502: Bad Gateway untuk downstream timeout
            raise HTTPException(status_code=502, detail="Timeout memproses dokumen di Azure.")
        except HTTPException:
            # propagate FastAPI HTTPException (jika ada)
            raise
        except Exception as e:
            log.exception("Gagal memproses dokumen.")
            raise HTTPException(status_code=500, detail="Gagal memproses dokumen.") from e
        finally:
            # cleanup
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception as e:
                log.warning(f"Gagal menghapus tmp dir {tmp_dir}: {e}")

        # Pastikan result_dict sesuai skema (opsional sanity check ringkas)
        if not isinstance(result_dict, dict) or "document" not in result_dict:
            log.error("Format output pipeline tidak valid.")
            raise HTTPException(status_code=500, detail="Format output tidak valid.")

        return JSONResponse(result_dict, status_code=200)

    return app


app = create_app()

if __name__ == "__main__":
    host = get_env("SERVER_HOST", "0.0.0.0") or "0.0.0.0"
    port = parse_int(get_env("SERVER_PORT", "8000"), 8000)
    # Gunakan reload=True hanya saat dev lokal, hindari di VPS produksi
    uvicorn.run("main:app", host=host, port=port)
