# PDF OCR Worker 本机部署

本部署将 PaddleOCR 放在 Quote 进程之外的两个独立 Python 虚拟环境中：

- GPU worker：`paddlepaddle-gpu==3.3.1` + `paddleocr==3.7.0`
- CPU fallback worker：`paddlepaddle==3.3.1` + `paddleocr==3.7.0`

Quote 主进程只安装 CPU 版依赖，并使用 PDFium 将目标页渲染成 PNG，再通过
子进程协议发送给 worker。worker 不接收 PDF 路径或 PDF 字节，也不加载 Quote
的完整 `research` 包。这样可以避免 CUDA wheel 与 Quote conda 的二进制依赖冲突。

部署脚本为：

```bash
sudo /home/python/Quote/scripts/deploy_pdf_ocr_workers.sh \
  --python /home/python/miniconda3/envs/Quote/bin/python
```

脚本默认安装到：

```text
/opt/quote-pdf-workers/gpu-3.3.1/
/opt/quote-pdf-workers/cpu-3.3.1/
/var/cache/quote/paddlex/
/etc/quote-system/pdf-ocr-workers.env
/etc/systemd/system/quote-system.service.d/pdf-ocr-workers.conf
```

脚本会创建虚拟环境、安装两个 requirements 文件、检查 NVIDIA、运行两个
worker 的 `--probe`、生成持久模型缓存配置，并 reload systemd。默认不重启
Quote；确认 probe 和配置无误后再执行：

```bash
sudo systemctl restart quote-system
sudo systemctl status quote-system --no-pager
```

## 前置条件

宿主机必须能看到 GPU：

```bash
nvidia-smi
```

如果重启后设备节点不存在，由主机启动流程执行：

```bash
sudo nvidia-modprobe -u -c=0
```

应用代码和 worker 不负责创建 NVIDIA 设备节点。GPU worker 使用 CUDA 11.8
Paddle wheel，宿主驱动 535.x / CUDA 上限 12.2 满足本机 P4 的已验收组合。

## 服务隔离要求

现有 `quote-system.service` 使用 `PrivateDevices=true`。如果保持该设置，
worker 子进程会看不到 `/dev/nvidia0`。部署脚本生成的 drop-in 会设置：

```ini
[Service]
EnvironmentFile=/etc/quote-system/pdf-ocr-workers.env
PrivateDevices=false
ReadWritePaths=/var/cache/quote/paddlex
```

这是 GPU worker 能够工作的必要条件。该设置会让 Quote 服务及其子进程看到
宿主设备，服务仍保持 `NoNewPrivileges` 等其他安全限制。

## 目录和权限

虚拟环境由 root 安装并保持只读执行；模型缓存必须由运行 Quote 的用户可写。
默认用户是 `python`，可用环境变量覆盖：

```bash
QUOTE_PDF_SERVICE_USER=quote \
QUOTE_PDF_SERVICE_GROUP=quote \
sudo -E /home/python/Quote/scripts/deploy_pdf_ocr_workers.sh
```

不要把模型缓存放到 `/tmp`。首次 worker 启动会下载 PP-OCRv6 模型，持久缓存
可以避免服务重启后重复下载。

## 配置和 approval

脚本将以下配置写入 `/etc/quote-system/pdf-ocr-workers.env`：

```text
QUOTE_PDF_ENGINE_PROFILE=pdfium_paddleocr_gpu
QUOTE_PDF_GPU_OCR_WORKER=...
QUOTE_PDF_CPU_OCR_WORKER=...
QUOTE_PDF_GPU_CANARY_APPROVED=1
QUOTE_PDF_GPU_CANARY_REPORT=...
QUOTE_PDF_GPU_CANARY_CORPUS_HASH=...
QUOTE_PDF_GPU_PROBE_TIMEOUT_SEC=60
PADDLE_PDX_CACHE_HOME=/var/cache/quote/paddlex
```

GPU profile 仍受 approval、worker probe 和 cache 可写性约束。任一条件失败
都会 fail closed，不会隐式改为全文 OCR。进程内成功的 GPU `--probe` 会复用，
避免 `resolve_profile` 和 `build_router` 各探一次；失败不缓存，下次 PDF 可重试。
首次探活默认 60 秒（`QUOTE_PDF_GPU_PROBE_TIMEOUT_SEC`），失败必须把
`diagnostic` 写进日志和异常。GPU worker 运行失败时，共享 adapter
只在同一页白名单和原始预算内尝试 CPU fallback。

## 验证

单独验证 worker：

```bash
sudo -u python env \
  PADDLE_PDX_CACHE_HOME=/var/cache/quote/paddlex \
  /opt/quote-pdf-workers/gpu-3.3.1/bin/python \
  /home/python/Quote/research/document_processing/pdf/ocr_worker.py --probe \
  <<< '{"protocol":"quote-pdf-ocr-worker.v1","runtime":"isolated-gpu-paddle-3.3.1","model_cache_dir":"/var/cache/quote/paddlex"}'
```

成功结果应包含 `healthy: true`、`cuda_available: true`、
`paddle_version: 3.3.1`、`paddleocr_version: 3.7.0` 和
`model_cache_writable: true`。CPU worker 的 `cuda_available` 应为 `false`。

验证 systemd drop-in：

```bash
systemctl cat quote-system
systemctl show quote-system -p PrivateDevices -p EnvironmentFiles
```

## 选项和回滚

只部署 CPU fallback：

```bash
sudo /home/python/Quote/scripts/deploy_pdf_ocr_workers.sh \
  --python /home/python/miniconda3/envs/Quote/bin/python --cpu-only
```

只安装 worker、不修改 systemd：

```bash
sudo /home/python/Quote/scripts/deploy_pdf_ocr_workers.sh --no-systemd
```

回滚到原生 PDFium：

```bash
sudo sed -i 's/^QUOTE_PDF_ENGINE_PROFILE=.*/QUOTE_PDF_ENGINE_PROFILE=pdfium_native/' \
  /etc/quote-system/pdf-ocr-workers.env
sudo systemctl restart quote-system
```

回滚不会删除原始 PDF、模型缓存或虚拟环境；恢复 GPU 时只需重新设置
`QUOTE_PDF_ENGINE_PROFILE=pdfium_paddleocr_gpu` 并重启服务。

## 当前验收环境与正式部署的区别

本次开发验收使用过 `/tmp/quote-pdf-gpu-331`、`/tmp/quote-pdf-cpu-331` 和
`/tmp/quote-paddlex-*`。这些目录不是正式部署位置，重启后可能消失。正式环境
必须运行本脚本安装到持久路径，并通过 systemd EnvironmentFile 注入配置。
