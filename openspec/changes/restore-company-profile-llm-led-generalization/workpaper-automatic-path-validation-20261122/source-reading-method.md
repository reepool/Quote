# A独立读源接点（不送模型）

`source-manifest.json`逐股列原owner实际返回的report、official_pdf_path、native_artifact_path及directory_index_path。所有正文取舍/业务参考由A自行读取；B只做身份/完整PDF/hash/缓存及书签元数据接线。目录索引为原PDF书签标题与物理页，不是生产选章规则，也不是答案。

使用项目解释器`/home/python/miniconda3/envs/Quote/bin/python`。先读对应`directory-index/<代码>.json`定位；再直接读取正式PDF（清单中完整路径），如需保表格对齐：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
pdftotext -layout -f 1 -l 12 /absolute/path/from/source-manifest.pdf -
# A按本报告目录自主确定经营/收入章节后，以其物理页替换-f/-l。
```

缓存为原owner的gzip JSON，若只查看指定页原生文本，可在本地只读：

```python
import gzip, json
from pathlib import Path
manifest=json.loads(Path('/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-path-validation-20261122/source-manifest.json').read_text())
entry=next(c for c in manifest['companies'] if c['instrument_id']=='000049.SZ')
artifact=json.loads(gzip.decompress(Path(entry['native_artifact_path']).read_bytes()))
for page in artifact['pages']:
    if 1 <= page['page_number'] <= 12:
        print('PHYSICAL PAGE',page['page_number'])
        print(page['text'])
```

原生文本可能压缩表格空白；核金额列/跨页表时优先PDF原页或-layout输出。A冻结的source-reference文件不进入模型payload，也不替代模型自己从目录选章。当前首四顺序000049/601006/600015/600348，其后按原四层相对次序轮转，末四补剩制造/服务；模型只按同一正式源hash处理。
