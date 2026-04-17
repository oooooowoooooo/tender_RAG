# Coverage Report

生成时间：`2026-04-16 11:33:48`

## 交付物状态

1. `data/raw/` 已生成三类原始层目录
2. `data/manifests/raw_manifest.jsonl` 已生成
3. `data/contracts/data_targets.json` 已生成
4. `docs/retrieval_strategy.md` 已生成
5. `reports/coverage_report.md` 已生成

## 招标数据

- 目标：`1000` 个唯一项目主键，计数单位为 `project`
- 当前仓库可用上游：`56035` 条页面记录，其中 `project_id` 非空后可聚合成 `8,803` 个唯一项目
- 当前打包：`1000` 个项目 bundle，底层共包含 `1,400` 条生命周期页面记录
- 去重收益：`44690` 条空 `project_id` 页面记录被排除，不计入项目覆盖
- 生命周期完整性：
  - 多页面项目：`110` / `1000`
  - 多链接项目：`24` / `1000`
  - 含采购正文：`645` / `1000`
  - 含结果正文：`1000` / `1000`
  - 含附件线索：`1000` / `1000`
- 官方历史源补采：
  - 时间窗口：`2023-11-01` 至 `2024-03-28`
  - 国家平台 API 原始页：`92` 页
  - 当前可用缓存页：`92` 页；本轮频控未拉全的页数：`645`
  - 官方 raw 记录：`1,840` 条，去重后 `920` 条
  - 已匹配到当前 1000 项目的项目数：`6` / `1000`
  - 已匹配的官方记录数：`11`
  - 匹配方法分布：`contained_normalized_title`: 6
  - 官方信息类型分布：`招标/资审文件澄清`: 81；`招标资审公告`: 81；`采购/资审公告`: 136；`采购合同`: 89；`开标记录`: 116；`挂牌披露`: 50；`中标公告`: 116；`交易结果公示`: 211；`交易结果`: 40
- 必填字段预覆盖：`project_code`: 1000；`project_name`: 525；`business_type`: 1000；`info_type`: 988；`publish_time`: 1000；`region`: 1000；`tenderer_or_purchaser`: 1000；`agency`: 1000；`budget_or_bid_amount`: 1000；`opening_time`: 0；`source_platform`: 1000；`source_url`: 1000；`attachment_paths`: 0
- 信息类型分布：`中标`: 891；`终止`: 106
- 备注：合肥公共资源交易站点的原始 HTML / PDF 仍存在脚本直连 `403` 问题；本轮补齐路径改为“项目级 bundle + 国家平台历史 API 官方原始 JSON 页”。国家平台历史 API 当前存在频控，已先固化可抓到的缓存页，详情页 HTML 仍待后续浏览器态采集。

## 政策数据

- 目标：`1000` 个文档，计数单位为 `document`
- 当前仓库本地已有：`180` 个文档级原始记录
- 官方政策库 feed 候选：`18,853` 条
- 本轮官方补采：`820` 个文档，写入 `820` 份原始 HTML
- 当前打包：`1000` 个政策文档 bundle，外加 `3` 个历史 HTML 样本
- 官方抓取过程：尝试 `960` 条，失败 `0` 条，因去重跳过 `0` 条
- 距离目标缺口：`0` 个文档
- 层级分布：`national`: 989；`province`: 9；`city`: 2
- 来源分布：`gov_cn_gwywj`: 100；`gov_cn_bmwj`: 720；`official`: 9；`mirror`: 2；`curated_existing`: 169
- 官方补采来源分布：`gov_cn_gwywj`: 100；`gov_cn_bmwj`: 720
- 必填字段预覆盖：`title`: 1000；`issuer`: 1000；`index_no`: 100；`subject_category`: 1000；`doc_no`: 912；`publish_date`: 1000；`validity_status`: 169；`policy_level`: 1000；`source_url`: 831；`attachment_paths`: 303
- 备注：政策库已按“document”口径补到目标规模；本地原始底表仍保留 `180` 份，缺口主要由中国政府网官方政策库补齐，后续再继续补安徽 / 合肥官方源占比。

## 企业数据

- 目标：`1000` 个实体，计数单位为 `entity`
- 主体来源：先从已选招标项目抽主体，再对全量招标主体做回填
- 当前可用招标主体名：
  - 已选 1000 项目内：`1519` 个唯一主体名
  - 全量招标语料内：`9811` 个唯一主体名
- 当前可匹配企业画像：`1936` 个唯一实体候选
- 当前打包：`1000` 个企业实体 bundle
- 选择策略：
  - 直接来自已选 1000 项目：`590` 个
  - 为补足到 1000 从全量招标主体回填：`410` 个
  - 本地企业库精确命中：`873` 个
  - 全国企业库精确命中：`127` 个
- 必填字段预覆盖：`unified_social_credit_code`: 1000；`enterprise_name`: 1000；`legal_representative`: 1000；`entity_type`: 1000；`established_date`: 1000；`registration_authority`: 0；`registered_capital`: 1000；`business_status`: 1000；`registered_address`: 1000；`business_scope`: 1000；`source_url`: 81
- 备注：企业库不再随机抽取，而是从招标主体反查生成，优先保证后续 `project -> entity -> policy` 可以关联。

## Raw Manifest

- `raw_manifest.jsonl` 总行数：`3917`
- 按数据集：`tender`: 1094；`policy`: 1823；`enterprise`: 1000
- 按文件类型：`json`: 3093；`jsonl`: 1；`html`: 823

## 当前结论

- 招标口径已改成“唯一项目主键”，后续不会再因为多阶段页面重复而返工。
- 政策口径已改成“文档”，并把 `issuer / doc_no / validity_status` 定义成强制字段，即便当前覆盖不足，也不会再混口径。
- 企业口径已改成“招标主体反查”，不再随机抽企业。
- 当前阶段的重点不是伪造 1000 文档政策覆盖，而是先把主键、目录、manifest、字段契约、覆盖报告全部钉死。
