#!/usr/bin/env python3
"""Image-only PaddleOCR worker used outside the Quote Python environment.

This program deliberately accepts rendered PNG bytes, never PDF paths or PDF
bytes. It may therefore be installed in a CUDA or CPU virtual environment
without importing Paddle into the parent Quote process.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import sys
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Mapping

PROTOCOL = "quote-pdf-ocr-worker.v1"


def _distribution_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _cache_directory(value: str | None) -> str:
    directory = value or os.environ.get("PADDLE_PDX_CACHE_HOME")
    if not directory:
        raise RuntimeError("PADDLE_PDX_CACHE_HOME must be configured for the OCR worker")
    Path(directory).mkdir(parents=True, exist_ok=True)
    if not os.access(directory, os.W_OK):
        raise RuntimeError(f"PaddleOCR model cache is not writable: {directory}")
    os.environ["PADDLE_PDX_CACHE_HOME"] = directory
    return directory


def _probe(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    if payload.get("protocol") != PROTOCOL:
        raise ValueError("worker protocol mismatch")
    import paddle

    cuda_compiled = bool(paddle.is_compiled_with_cuda())
    cuda_count = int(paddle.device.cuda.device_count()) if cuda_compiled else 0
    cache_dir = payload.get("model_cache_dir") or os.environ.get("PADDLE_PDX_CACHE_HOME")
    writable = bool(cache_dir and Path(cache_dir).is_dir() and os.access(cache_dir, os.W_OK))
    return {
        "protocol": PROTOCOL,
        "healthy": _distribution_version("paddleocr") == "3.7.0",
        "runtime": payload.get("runtime"),
        "paddle_version": getattr(paddle, "__version__", None),
        "paddleocr_version": _distribution_version("paddleocr"),
        "cuda_available": cuda_compiled and cuda_count > 0,
        "cuda_device_count": cuda_count,
        "device": str(paddle.device.get_device()),
        "model_cache_dir": cache_dir,
        "model_cache_writable": writable,
        "inference_config": {"enable_mkldnn": False, "ir_optim": "approved-runtime-default"},
    }


def _normalise(value: Any) -> tuple[str, float | None]:
    texts: list[str] = []
    scores: list[float] = []

    def visit(item: Any) -> None:
        if isinstance(item, str):
            if item.strip():
                texts.append(item.strip())
        elif isinstance(item, Mapping):
            values = item.get("rec_texts")
            if isinstance(values, (list, tuple)):
                texts.extend(str(entry).strip() for entry in values if str(entry).strip())
            confidence = item.get("rec_scores")
            if isinstance(confidence, (list, tuple)):
                scores.extend(float(entry) for entry in confidence if isinstance(entry, (int, float)))
            if item.get("text"):
                texts.append(str(item["text"]).strip())
            if isinstance(item.get("score"), (int, float)):
                scores.append(float(item["score"]))
        elif isinstance(item, (list, tuple)):
            if len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], (int, float)):
                texts.append(item[0].strip())
                scores.append(float(item[1]))
            else:
                for entry in item:
                    visit(entry)

    visit(value)
    return "\n".join(text for text in texts if text), (sum(scores) / len(scores) if scores else None)


def _ocr(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    if payload.get("protocol") != PROTOCOL:
        raise ValueError("worker protocol mismatch")
    _cache_directory(payload.get("model_cache_dir"))
    import numpy as np
    import paddle
    from PIL import Image
    from paddleocr import PaddleOCR

    device = str(payload.get("device") or "cpu")
    # The configuration is deliberately explicit; unrecognised overrides do
    # not silently select an unapproved accelerator optimization path.
    session = PaddleOCR(
        device=device,
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,
    )
    output = []
    for item in payload.get("pages", []):
        number = int(item["page_number"])
        image_bytes = base64.b64decode(item["image_png_base64"], validate=True)
        with Image.open(io.BytesIO(image_bytes)) as image:
            array = np.asarray(image.convert("RGB"))
        started = time.perf_counter()
        raw = list(session.predict(array)) if hasattr(session, "predict") else session.ocr(array, cls=True)
        text, confidence = _normalise(raw)
        output.append({
            "page_number": number,
            "text": text,
            "confidence": confidence,
            "elapsed_seconds": time.perf_counter() - started,
            "image_sha256": item.get("image_sha256"),
            "paddle_version": getattr(paddle, "__version__", None),
            "paddleocr_version": _distribution_version("paddleocr"),
            "model": "PP-OCRv6",
            "model_version": None,
            "diagnostics": [] if text else [{"code": "ocr_empty", "message": "PaddleOCR returned no text", "page_number": number, "severity": "error"}],
        })
    return {
        "protocol": PROTOCOL,
        "runtime": payload.get("runtime"),
        "device": str(paddle.device.get_device()),
        "inference_config": {"enable_mkldnn": False, "ir_optim": "approved-runtime-default"},
        "pages": output,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--probe", action="store_true")
    operation.add_argument("--ocr", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = json.load(sys.stdin)
        result = _probe(payload) if args.probe else _ocr(payload)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
