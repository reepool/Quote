#!/usr/bin/env bash

# Install the isolated PDF OCR workers and their persistent model cache.
# This script never installs the CUDA wheel into the Quote environment.

set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKER_ROOT="${QUOTE_PDF_WORKER_ROOT:-/opt/quote-pdf-workers}"
GPU_ENV="${QUOTE_PDF_GPU_ENV:-${WORKER_ROOT}/gpu-3.3.1}"
CPU_ENV="${QUOTE_PDF_CPU_ENV:-${WORKER_ROOT}/cpu-3.3.1}"
CACHE_DIR="${QUOTE_PDF_OCR_CACHE_DIR:-/var/cache/quote/paddlex}"
ENV_DIR="${QUOTE_PDF_ENV_DIR:-/etc/quote-system}"
ENV_FILE="${QUOTE_PDF_WORKER_ENV_FILE:-${ENV_DIR}/pdf-ocr-workers.env}"
DROPIN_DIR="${QUOTE_PDF_SYSTEMD_DROPIN_DIR:-/etc/systemd/system/quote-system.service.d}"
DROPIN_FILE="${DROPIN_DIR}/pdf-ocr-workers.conf"
SERVICE_NAME="${QUOTE_PDF_SERVICE_NAME:-quote-system}"
SERVICE_USER="${QUOTE_PDF_SERVICE_USER:-python}"
SERVICE_GROUP="${QUOTE_PDF_SERVICE_GROUP:-${SERVICE_USER}}"
PYTHON_BIN="${QUOTE_PDF_DEPLOY_PYTHON:-python3.11}"
GPU_INDEX_URL="${QUOTE_PDF_GPU_INDEX_URL:-https://www.paddlepaddle.org.cn/packages/stable/cu118/}"
GPU_APPROVAL="${QUOTE_PDF_GPU_CANARY_REPORT:-${PROJECT_ROOT}/docs/development/pdfium_paddleocr_gpu_canary_approval_20260826.json}"
INSTALL_GPU=1
INSTALL_CPU=1
INSTALL_SYSTEMD=1
RESTART_SERVICE=0

log() { printf '[pdf-ocr-deploy] %s\n' "$*"; }
die() { printf '[pdf-ocr-deploy] ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Usage: sudo scripts/deploy_pdf_ocr_workers.sh [options]

Install persistent isolated CPU/GPU OCR environments and configure Quote.

Options:
  --python PATH       Python 3.11 interpreter used to create both venvs.
  --gpu-only          Install/configure only the GPU worker.
  --cpu-only          Install/configure only the CPU worker.
  --no-systemd        Do not create the quote-system service drop-in.
  --restart           Restart quote-system after installation.
  --help              Show this help.

Paths can also be overridden with QUOTE_PDF_* environment variables; see the
deployment runbook for the complete list.
EOF
}

require_root() {
    [[ "${EUID}" -eq 0 ]] || die "run this script with sudo/root privileges"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --python)
                [[ $# -ge 2 ]] || die "--python requires a path"
                PYTHON_BIN="$2"
                shift 2
                ;;
            --gpu-only)
                INSTALL_CPU=0
                shift
                ;;
            --cpu-only)
                INSTALL_GPU=0
                shift
                ;;
            --no-systemd)
                INSTALL_SYSTEMD=0
                shift
                ;;
            --restart)
                RESTART_SERVICE=1
                shift
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                die "unknown option: $1"
                ;;
        esac
    done
}

check_inputs() {
    [[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="$(command -v "$PYTHON_BIN" || true)"
    [[ -n "$PYTHON_BIN" && -x "$PYTHON_BIN" ]] || die "Python 3.11 interpreter not found: ${PYTHON_BIN:-unset}"
    [[ -f "$PROJECT_ROOT/docs/development/pdf_ocr_worker_requirements_gpu.txt" ]] || die "GPU requirements file is missing"
    [[ -f "$PROJECT_ROOT/docs/development/pdf_ocr_worker_requirements_cpu.txt" ]] || die "CPU requirements file is missing"
    [[ -f "$PROJECT_ROOT/research/document_processing/pdf/ocr_worker.py" ]] || die "worker source is missing"
    if [[ "$INSTALL_GPU" -eq 1 ]]; then
        [[ -f "$GPU_APPROVAL" ]] || die "GPU approval report is missing: $GPU_APPROVAL"
        command -v nvidia-smi >/dev/null 2>&1 || die "nvidia-smi is required for GPU deployment"
        nvidia-smi >/dev/null 2>&1 || die "nvidia-smi cannot see a usable GPU; expose /dev/nvidia* before deployment"
    fi
}

ensure_user() {
    getent passwd "$SERVICE_USER" >/dev/null 2>&1 || die "service user does not exist: $SERVICE_USER"
    getent group "$SERVICE_GROUP" >/dev/null 2>&1 || die "service group does not exist: $SERVICE_GROUP"
}

create_worker_env() {
    local target="$1"
    local requirements="$2"
    local extra_index="${3:-}"

    if [[ ! -x "$target/bin/python" ]]; then
        log "creating venv: $target"
        install -d -m 0755 -o root -g root "$target"
        "$PYTHON_BIN" -m venv "$target"
    fi
    local pip_args=(--disable-pip-version-check -r "$requirements")
    if [[ -n "$extra_index" ]]; then
        pip_args+=(--extra-index-url "$extra_index")
    fi
    "$target/bin/python" -m pip install "${pip_args[@]}"
    chmod -R a+rX "$target"
}

probe_worker() {
    local python_path="$1"
    local runtime="$2"
    local expected_cuda="$3"
    local payload
    payload=$(printf '{"protocol":"quote-pdf-ocr-worker.v1","runtime":"%s","model_cache_dir":"%s"}\n' "$runtime" "$CACHE_DIR")
    local output
    output=$(PADDLE_PDX_CACHE_HOME="$CACHE_DIR" "$python_path" "$PROJECT_ROOT/research/document_processing/pdf/ocr_worker.py" --probe <<<"$payload")
    "$PYTHON_BIN" -c 'import json, sys; expected = sys.argv[1] == "true"; result = json.load(sys.stdin); assert result.get("protocol") == "quote-pdf-ocr-worker.v1" and result.get("healthy"), "worker probe is unhealthy"; assert bool(result.get("cuda_available")) == expected, f"unexpected CUDA state: {result}"; assert result.get("model_cache_writable"), f"model cache is not writable: {result}"; print(json.dumps(result, ensure_ascii=False, sort_keys=True))' "$expected_cuda" <<<"$output"
    # The Python one-liner above validates protocol, health, CUDA state, and
    # cache writability while keeping the worker's stdout as its stdin.
}

write_runtime_config() {
    local profile="pdfium_paddleocr_cpu"
    local gpu_config=""
    local cpu_config=""
    if [[ "$INSTALL_CPU" -eq 1 ]]; then
        cpu_config="QUOTE_PDF_CPU_OCR_WORKER=$CPU_ENV/bin/python $PROJECT_ROOT/research/document_processing/pdf/ocr_worker.py"
    fi
    if [[ "$INSTALL_GPU" -eq 1 ]]; then
        local corpus_hash
        corpus_hash=$("$PYTHON_BIN" - "$GPU_APPROVAL" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["corpus_hash"])
PY
        )
        profile="pdfium_paddleocr_gpu"
        gpu_config=$(cat <<EOF
QUOTE_PDF_GPU_OCR_WORKER=$GPU_ENV/bin/python $PROJECT_ROOT/research/document_processing/pdf/ocr_worker.py
QUOTE_PDF_GPU_CANARY_APPROVED=1
QUOTE_PDF_GPU_CANARY_REPORT=$GPU_APPROVAL
QUOTE_PDF_GPU_CANARY_CORPUS_HASH=$corpus_hash
EOF
)
    fi
    install -d -m 0750 -o root -g "$SERVICE_GROUP" "$ENV_DIR"
    local temp_file
    temp_file="$(mktemp "${ENV_FILE}.XXXXXX")"
    cat >"$temp_file" <<EOF
# Generated by deploy_pdf_ocr_workers.sh. Do not put secrets here.
PADDLE_PDX_CACHE_HOME=$CACHE_DIR
QUOTE_PDF_OCR_CACHE_DIR=$CACHE_DIR
QUOTE_PDF_ENGINE_PROFILE=$profile
$cpu_config
$gpu_config
EOF
    install -m 0640 -o root -g "$SERVICE_GROUP" "$temp_file" "$ENV_FILE"
    rm -f "$temp_file"
}

write_systemd_dropin() {
    [[ "$INSTALL_SYSTEMD" -eq 1 ]] || return 0
    command -v systemctl >/dev/null 2>&1 || die "systemctl is required unless --no-systemd is used"
    install -d -m 0755 -o root -g root "$DROPIN_DIR"
    cat >"$DROPIN_FILE" <<EOF
[Service]
EnvironmentFile=$ENV_FILE
# The Quote service otherwise hides /dev/nvidia* from its worker child process.
PrivateDevices=false
ReadWritePaths=$CACHE_DIR
EOF
    chmod 0644 "$DROPIN_FILE"
    systemctl daemon-reload
}

main() {
    parse_args "$@"
    require_root
    check_inputs
    ensure_user
    install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$CACHE_DIR"
    install -d -m 0755 -o root -g root "$WORKER_ROOT"

    if [[ "$INSTALL_GPU" -eq 1 ]]; then
        create_worker_env "$GPU_ENV" "$PROJECT_ROOT/docs/development/pdf_ocr_worker_requirements_gpu.txt" "$GPU_INDEX_URL"
    fi
    if [[ "$INSTALL_CPU" -eq 1 ]]; then
        create_worker_env "$CPU_ENV" "$PROJECT_ROOT/docs/development/pdf_ocr_worker_requirements_cpu.txt"
    fi

    if [[ "$INSTALL_GPU" -eq 1 ]]; then
        probe_worker "$GPU_ENV/bin/python" "isolated-gpu-paddle-3.3.1" true
    fi
    if [[ "$INSTALL_CPU" -eq 1 ]]; then
        probe_worker "$CPU_ENV/bin/python" "isolated-cpu-paddle-3.3.1" false
    fi

    write_runtime_config
    write_systemd_dropin
    log "persistent worker deployment completed"
    log "environment file: $ENV_FILE"
    log "GPU worker: $GPU_ENV/bin/python"
    log "CPU worker: $CPU_ENV/bin/python"
    log "model cache: $CACHE_DIR"

    if [[ "$RESTART_SERVICE" -eq 1 ]]; then
        systemctl restart "$SERVICE_NAME"
        systemctl --no-pager --full status "$SERVICE_NAME"
    else
        log "service was not restarted; run: sudo systemctl restart $SERVICE_NAME"
    fi
}

main "$@"
