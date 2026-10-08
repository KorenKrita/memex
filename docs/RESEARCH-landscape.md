# 附录：开源记忆项目横向调研（2026-10-08）

筛选标准：只保留能补 memex 现有设计真实弱点的东西；“确认已有设计”与“暂不采用”同样是结论。

## 1. 开源范围（固定 commit）

| 项目 | commit | 许可 | 核心记忆逻辑是否在仓库内 | 依据 |
|---|---|---|---|---|
| Mem0 | `b7ad69a` | Apache-2.0 | 是 | `mem0/memory/main.py` `add()` → `_add_to_vector_store()` 使用 `ADDITIVE_EXTRACTION_PROMPT` |
| Hindsight | `10d8b1b` | MIT | 是（服务端完整） | `hindsight-api-slim/hindsight_api/engine/{retain,search,consolidation}` |
| Holographic | hermes-agent HEAD | MIT | 是（Hermes 内置 provider，~50KB） | `plugins/memory/holographic/{holographic,retrieval,store}.py` |
| RetainDB | GitHub HEAD | Local/SDK/MCP Apache-2.0；**server 为 BSL 1.1** | 部分 | README 第 520 行 |
| ByteRover CLI | `1052ac1` | **Elastic License 2.0**（源码可见，非 OSI） | CLI 在 | LICENSE |
| Supermemory | `203fb6e` | MIT（仓库内容） | **否**：仓库是 web/MCP/SDK/插件；本地版是 `npx supermemory local` 分发的二进制 | README 第 94、118、316 行；apps/packages 目录 |
| Memori | `574b1ea` | Apache-2.0 全文 | **部分**：Rust 引擎 + SDK 在；默认把 augmentation 发到 `*.memorilabs.ai/sdk/augmentation` | `memori/_network.py:44-62` |
| 额外：Graphiti | `86f1c94` | Apache-2.0 | 是 | `graphiti_core/edges.py`、`prompts/dedupe_edges.py` |
| 额外：memsearch | `2a4652f` | MIT | 是 | `src/memsearch/*`、`evaluation/reranking-evaluation.md` |
| 额外：claude-mem | `71ddd11` | Apache-2.0 | 是 | `src/servers/mcp-server.ts` |

未深读：RetainDB server、ByteRover、Supermemory 的引擎（不在仓库）、Letta。claude-mem 有 `pi/` 目录但只见许可证文件，**未验证其 Pi 集成**。

## 2. 对 memex 的结论

### 2.1 修订现有契约（3 条）

1. **相关性 = 能提供答案证据，而不是同主题**（memsearch rerank 提示词）。
   “True：段落陈述了回答所需的具体事实、决定、步骤或事件；False：只是同项目或相似话题。”
   → 写进评测标注规则和 rerank 输入说明，正对 OV“同主题弱相关”的噪音。不新增判定层，不换 reranker。
   注：其实验固定 10 个候选（Recall@10 三方相同），只衡量重排，不能说明召回；中英文是同一数据的翻译。

2. **“事情何时生效”≠“系统何时得知”**（Graphiti `valid_at / invalid_at / expired_at / reference_time`，`resolve_edge_contradictions()`）。
   → L0/L1 记录 `occurred_at`（来源时间戳）与 `ingested_at`；L3 结论记 `effective_from`（来源未提供就留空，不让 LLM 猜）和 `superseded_at`。导入上个月的记录不会被当成今天的新变化；冲突判断按 `occurred_at` 排序。不引入图谱。

3. **证据在进入 reranker 前就可能被挤掉**（Hindsight `search/fusion.py::cap_per_source`、`search/recall_boost.py` 注释）。
   → trace 记录每一阶段的存活：各路候选 → 融合后 → 截断进 rerank → 最终。评测除最终指标外，单独报告“正确证据在哪一阶段丢失”。不加它的 boost 参数体系（注释里 recall@20 0.97→0.40 是维护者报告，未复现）。

### 2.2 确认已有设计（不改）

- 索引可重建、来源独立保存：memsearch “Markdown 为真源 + Milvus shadow index”；我们是 L0 + 派生索引。
- 渐进读取：claude-mem `search → timeline → get_observations → get_tool_uses`；与 `mem_search → mem_zoom → 原文` 同向。可参考它的 `timeline(anchor, depth_before, depth_after)`，作为 `mem_zoom` 的前后文参数；但不强制逐层，明确要原文时允许直接读。
- 预计算开局卡：Hindsight mental models（固定问题的常驻答案，读取无 LLM）佐证 session 卡 / user card 预计算。
- 结论只增不覆盖：Hindsight observations（带原文引用与 proof count，refine 不覆盖）、Graphiti 失效不删除，与 L3 的 `superseded / contradicted` 一致。
- 四路并行 + RRF + cross-encoder：Hindsight 与我们的三通道 + rerank 同构。

### 2.3 值得注意的信号

- **Mem0 当前 OSS 的 `add()` 只走 ADD**（`ADDITIVE_EXTRACTION_PROMPT`），旧的 `ADD/UPDATE/DELETE/NONE` 提示词仍在 `prompts.py` 但不在主写入路径。和我们“先只追加、冲突用状态标记”的方向一致。（未查其迁移原因。）
- **中文全文候选**：Hindsight 支持 ParadeDB `pg_search` 的 `jieba / lindera(chinese) / chinese_compatible / source_code` tokenizer（`_pg_search.py`）。加入 §11 第 3 项的候选对照（与 `pg_trgm` 比命中率，并权衡扩展部署成本），不预先替换。
- **脱敏**：Hindsight Memory Defense 用 45 条模式。可作为我们客户端脱敏规则的参考来源。

### 2.4 暂不采用（无真实失败案例证明需要）

HRR 组合检索与 trust 打分（Holographic：FTS5 候选 → Jaccard + HRR 重排 → 乘 trust → 可选时间衰减）；知识图谱 / 图遍历 / 社区摘要；Honcho dreamer；自动技能生成（memsearch、Codex v1）；用户画像自动维护（Supermemory、Memobase）；按使用次数强化或遗忘（RetainDB、Holographic 的 `fact_feedback`）。

## 3. 来源

- https://github.com/mem0ai/mem0 · https://github.com/vectorize-io/hindsight · https://github.com/NousResearch/hermes-agent/tree/main/plugins/memory/holographic
- https://github.com/RetainDB/RetainDB · https://github.com/campfirein/byterover-cli · https://github.com/supermemoryai/supermemory · https://github.com/MemoriLabs/Memori
- https://github.com/getzep/graphiti · https://github.com/zilliztech/memsearch · https://github.com/thedotmack/claude-mem
