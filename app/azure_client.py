# app/azure_client.py
from __future__ import annotations

import logging
from typing import Optional

from azure.core.credentials import AzureKeyCredential
from azure.core.pipeline.policies import RetryPolicy
from azure.core.pipeline.transport import RequestsTransport
from azure.ai.formrecognizer import (
    FormRecognizerClient,
    FormRecognizerApiVersion,
)

log = logging.getLogger("azure-ocr.azure_client")

DEFAULT_TIMEOUT = 300  # detik (connection + read + overall retry timeout)


def _make_transport(timeout: int = DEFAULT_TIMEOUT) -> RequestsTransport:
    """
    Transport HTTP berbasis requests dengan timeout koneksi + baca.
    """
    return RequestsTransport(connection_timeout=timeout, read_timeout=timeout)


def _make_retry_policy(operation_timeout: int = DEFAULT_TIMEOUT) -> RetryPolicy:
    """
    Retry policy konservatif untuk throttling/intermiten.
    Penting: 'timeout' HARUS angka (bukan None) untuk menghindari TypeError di azure-core.
    """
    return RetryPolicy(
        retry_total=6,                         # total percobaan (1 + 5 retry)
        retry_connect=3,
        retry_read=3,
        retry_status=6,
        retry_on_status_codes={429, 500, 502, 503, 504},
        timeout=float(operation_timeout),      # <- WAJIB angka
    )


def get_client(
    endpoint: str,
    api_key: str,
    *,
    timeout_seconds: Optional[int] = None,
) -> FormRecognizerClient:
    """
    Factory FormRecognizerClient terkunci ke API v2.1 (sesuai akun F0).

    Args:
        endpoint: https://<resource>.cognitiveservices.azure.com
        api_key: kunci akses
        timeout_seconds: override timeout default (detik)

    Returns:
        FormRecognizerClient siap pakai.
    """
    if not endpoint or not api_key:
        raise ValueError("Endpoint atau API key kosong.")

    op_timeout = int(timeout_seconds or DEFAULT_TIMEOUT)

    transport = _make_transport(timeout=op_timeout)
    retry_policy = _make_retry_policy(operation_timeout=op_timeout)

    client = FormRecognizerClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(api_key),
        api_version=FormRecognizerApiVersion.V2_1,
        transport=transport,
        retry_policy=retry_policy,
    )

    log.debug("FormRecognizerClient v2.1 instantiated (timeout=%ss).", op_timeout)
    return client
