# 产品目录采集与版本发布系统（pointwise-delivery）

从空目录构建的完整演示：模拟来源服务、递归采集器、不可变版本存储和真实网页控制台，
**仅使用 Python 3.11 标准库，无第三方依赖**。

## 业务模型与约束

来源侧四类对象各自独立更新，**没有跨对象事务**：

```
catalog ──products──▶ product ──dataset──▶ dataset ──fields──▶ field
                                            └────rules──────▶ rule
                                            └──depends_on──▶ dataset (可成环)
```

- 每个对象带单调递增修订号 `_rev` 和不透明 `ETag`。
- 条件读取：`If-None-Match` 未变返回 `304`（省字节）；`If-Match` 已变返回 `412`。
- 删除是墓碑：`404` 带 `x-tombstone: 1`，区分“从未存在”和“遍历中被删”。
- 发布的目录版本必须**引用闭合**：不能拼旧产品与新数据集，也不能因请求失败删除引用。

## 两种闭合策略（可在页面选择）

| 策略 | 做法 | 失败判定 |
|---|---|---|
| `pin` 递归钉版本 | 每个对象首次访问即钉住 rev/ETag；共享对象每次再访问用 `If-Match`/`If-None-Match` 条件复核；末尾两轮闭合扫描（追赶扫描 + 无条件稳定性扫描） | 本次 attempt 内任何对象在闭合前变化 → `conflict`；缺失 → `missing`；成环 → `cycle`；来源故障 → `source` |
| `recheck` 反复核对到稳定 | 整图多轮，首轮全量、后续轮全走条件读取；某一轮全部 `304` 才算稳定 | 轮数有上限（默认 6），来源持续变化无法收敛 → `unstable` |

不能闭合时给出**具体原因**（冲突的对象、钉住 rev 与当前 rev、循环路径、墓碑标记等），
并展示**请求代价**：总请求数、条件请求数、304 省字节数、412 冲突数、重试数、接收字节、状态码分布。

## 并发与发布围栏

- 每个任务创建时获得单调递增 `seq`；版本存储维护发布围栏 `epoch`（已发布的最大 seq）。
- 发布在全局互斥锁内**重新读取任务记录**并校验 `seq > epoch`，关闭 check-then-set 竞态。
- 两个任务同时发现共享依赖时：发生同次 attempt 漂移的任务闭合失败；闭合更晚的旧 seq
  任务发布被围栏拒绝（HTTP 409），**迟到任务无法回退已发布版本**，`latest` 指针只前进。

## 断点续采（恢复进度）

- 请求失败不删除任何已钉引用，任务落盘 `visited_pins`。
- 续采从磁盘 pin 重建采集器，恢复的旧 pin 允许追赶来源当前修订；新 pin 在稳定性扫描中
  仍受严格保护。缓存对象以条件读取校验，仅漂移对象重新全量拉取。

## 目录结构

```
catalog_app/
  source.py      模拟来源：修订号/ETag/条件读取/墓碑/故障注入/调度更新/原始请求日志
  seed.py        确定性种子数据（含未引用对象、断链产品、自引用数据集）
  client.py      LocalClient(进程内) 与 HttpClient(真实 HTTP)，统一代价统计与重试
  collector.py   pin / recheck 两种策略、循环检测、闭合扫描、恢复
  store.py       原子写版本存储、seq/epoch 围栏、任务持久化
  web.py         单进程 HTTP：/api 控制面 + /source 来源 + 静态网页
  web/           index.html / app.js / styles.css（原生 JS 控制台）
tests/           34 个 unittest（含真实 HTTP 端到端与并发）
```

## 运行

```bash
python3 -m catalog_app --port 8000 --data data
# 打开 http://127.0.0.1:8000
```

测试：

```bash
python3 -m unittest discover -s tests -v
```

## 网页控制台五个页签

1. **来源对象**：查看所有对象的 rev/ETag/正文；PUT 更新（支持条件写入）、DELETE 墓碑删除、
   故障注入（指定对象/状态码/次数/持续）、调度并发更新、一键场景（循环、断链、条件冲突、抖动故障）。
2. **采集与闭合**：选目录与策略发起任务；失败任务展示保留的 pin 并可一键断点续采。
3. **任务**：状态、不能闭合的原因、每次尝试的轮次与遍历路径、请求代价、引用边、恢复进度、发布按钮。
4. **版本存储**：围栏 epoch 与最新版本、不可变快照（每对象 rev/ETag）、发布历史与代价。
5. **原始请求**：来源返回的逐字节原始响应（状态码、条件头、ETag、x-rev、墓碑头、响应体），可按任务过滤。

## 关键 API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/PUT/DELETE | `/source/<type>/<id>` | 来源对象（支持条件头） |
| GET | `/api/state` | 来源全量状态 |
| POST | `/api/tasks` | 创建并运行采集任务 `{catalog_id, strategy, async}` |
| POST | `/api/tasks/<id>/resume` | 断点续采 |
| POST | `/api/tasks/<id>/publish` | 发布（围栏拒绝返回 409） |
| GET | `/api/versions`、`/api/versions/<v>` | 版本列表与不可变快照 |
| POST | `/api/faults`、`/api/schedule` | 故障注入 / 调度并发更新 |
| GET/DELETE | `/api/requests` | 原始请求日志 |
