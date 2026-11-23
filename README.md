# 熔炉工坊 · 实时数据融合与宽表构建平台

面向多源异构数据的实时接入融合、宽表映射构建、维度关联编排、数据质量门禁与分区快照管理的实时数据融合平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源定义 | `source_def` | 数据源 | 源编号、源名称、源类型 |
| Schema映射 | `schema_map` | 映射方案 | 方案编号、源表名称、目标宽表 |
| 维度表 | `dim_table` | 维度定义 | 维度编号、维度名称、主键字段 |
| 融合作业 | `fusion_job` | 融合任务 | 作业编号、目标宽表、参与数据源 |
| 冲突仲裁 | `merge_conflict` | 冲突规则 | 规则编号、目标宽表、冲突字段 |
| 质量门禁 | `quality_gate` | 门禁规则 | 规则编号、目标宽表、检查类型 |
| 分区快照 | `snapshot_mgr` | 快照分区 | 分区编号、目标宽表、分区键值 |
| 数据回填 | `backfill_task` | 回填任务 | 任务编号、目标宽表、回填区间 |
| 数据血缘 | `lineage_edge` | 血缘边 | 边编号、上游节点、下游节点 |
| 重放队列 | `replay_queue` | 重放记录 | 记录编号、融合作业、失败原因 |
| 水位追踪 | `watermark_track` | 水位记录 | 记录编号、数据源、当前水位 |
| 分区策略 | `partition_rule` | 分区定义 | 策略编号、目标宽表、分区类型 |
| 宽表定义 | `wide_table_def` | 宽表元数据 | 表编号、宽表名称、字段列表 |
| 增量归并 | `delta_merge` | 增量批次 | 批次编号、融合作业、归并键 |
| 冷热分层 | `cold_store` | 分层策略 | 策略编号、目标宽表、热数据天数 |
| 快照导出 | `export_snapshot` | 导出计划 | 计划编号、目标宽表、导出格式 |
| 指标聚合 | `metric_aggr` | 聚合规则 | 规则编号、目标宽表、聚合函数 |
| 交叉校验 | `cross_validate` | 校验方案 | 方案编号、主体宽表、参照宽表 |
| 数据漂移 | `data_drift` | 漂移记录 | 记录编号、宽表名称、漂移类型 |
| 融合监控 | `fusion_monitor` | 监控指标 | 指标编号、融合作业、监控维度 |
| 变更审计 | `audit_trail` | 审计记录 | 记录编号、变更目标、操作人 |
| 目录检索 | `catalog_search` | 数据目录 | 目录编号、宽表名称、标签列表 |

## 来源批次文件接入

数据源定义里的登记只是接入声明，真正参与融合的是**来源批次文件**。批次接入页
（`/batch_ingest`）为已登记来源上传 CSV，融合作业只从“来源批次回执”读取有效数据行。

- 上传校验：必须含标识字段（默认 `id`），标识为空/重复、`amount` 非数值、列数超出表头
  都会让**整份文件**不予受理，不会出现半份明细被融合消费。
- 重传去重：同一来源 + 同一批次名称下字节完全一致时只确认一次有效批次，
  页面显示重传次数；内容变化则生成新版本（v2、v3…），旧版本标记为“已被新版本替代”。
- 批次回执：作业开始取数时按来源固定一个已受理批次。此后供应方再上传新版本，
  本次作业仍读完原版本的完整明细，回执的“已取行数”等于该批有效行数。
- 输出追溯：`GET /api/fusion/jobs/{id}/run` 与继续作业的返回里，每条记录都带
  `_batch_code`、`_file_name`、`_content_sha256`、`_row_number`，可追到来源文件与原数据行。
- 缺字段来源：未受理的来源进入 `blocked_sources`，只需对该来源重新上传合格文件，
  再次继续作业即补齐；先前已融合的行不会重复计入。
- 已完成的取数再次继续会返回 `already_completed`，不会重复消费。

接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/fusion/sources/{source_id}/batches?batch_name=&file_name=` | 上传原始字节（`text/csv`） |
| GET | `/api/fusion/sources/{source_id}/batches` | 列出来源的批次版本 |
| GET | `/api/fusion/batches/{batch_id}` | 查看校验错误与完整可消费行 |
| POST | `/api/fusion/jobs/{job_id}/start` | 固定批次回执 |
| POST | `/api/fusion/jobs/{job_id}/read?rows_per_source=` | 先读取一部分明细 |
| POST | `/api/fusion/jobs/{job_id}/continue` | 继续取数并完成融合 |
| GET | `/api/fusion/jobs/{job_id}/run` | 取数状态、回执与逐行来源 |

上传使用原始请求体而不是 multipart，因此不需要额外的 `python-multipart` 依赖。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
