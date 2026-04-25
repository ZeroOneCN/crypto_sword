# Hermes Trader（Crypto Sword）

面向 Binance 合约的自动化交易系统，当前已采用 `core + services + adapters` 分层架构，主程序职责回归调度。

## 本轮改造结果

- 已下线未使用且重复的 `data_fetcher.py`。
- 已移除 `accumulation_radar` 的系统入口（不再作为运行依赖）。
- 已新增正式启动入口 `run_live.py`，不再依赖 `python -c`。
- 已统一三类结构化输出（策略、执行、监控），便于后续回测对齐与统计。
- 已增加 `feature_store/` 与交易复盘记录（每笔平仓都会生成 review）。
- 已修复 Binance 条件单精度序列化问题，降低 `-1111 Precision` 报错概率。

## 运行入口

```bash
python run_live.py --mode live
```

常用参数：

```bash
python run_live.py \
  --mode live \
  --leverage 5 \
  --risk 2.0 \
  --max-positions 5 \
  --scan-top-n 30 \
  --scan-interval 300 \
  --fast-interval 60
```

可关闭 OI/Funding 增强：

```bash
python run_live.py --mode live --disable-oi-funding
```

## 当前架构

```text
run_live.py                    # 正式启动入口
crypto_sword.py                # 运行时编排器（CryptoSword）
core/
  bootstrap_service.py
  cycle_mixin.py
  scanner_mixin.py
  execution_mixin.py
  sync_mixin.py
  market_mixin.py
  confirmation_mixin.py
  models.py
  monitoring.py                # 监控/结构化输出工具
feature_store/
  store.py                     # 事件与复盘 JSONL 持久化
  reviewer.py                  # 每笔交易复盘摘要构建
services/
  signal_service.py
  risk_service.py
  execution_service.py
  order_service.py
  oi_funding_service.py
adapters/
  rest_gateway.py
  ws_gateway.py
```

## 结构化输出规范（已接线）

统一通过 `core/monitoring.py` 构建：

- `strategy_event`：扫描信号结果（分数、置信度、入场状态、OI/Funding bonus）
- `execution_event`：开仓/平仓执行结果（价格、数量、PnL、原因）
- `monitor_event`：周期监控快照（持仓数、未实现/已实现盈亏、已平仓数）

这些输出会进入运行日志，便于后续做统计聚合和回放。

同时会落盘到特征仓：

- `~/.hermes/logs/feature_store/<YYYY-MM-DD>/events.ndjson`
- `~/.hermes/logs/feature_store/<YYYY-MM-DD>/reviews.ndjson`

其中 `reviews.ndjson` 为每笔平仓复盘记录，包含：

- 入场原因（阶段、策略线、评分、OI/Funding加分）
- 出场原因（止盈/止损/手动、持仓时长、PnL）
- 训练标签（profit/loss、stop_loss_exit/take_profit_exit、oi_funding_enhanced）

## 已清理的冗余

- 删除：`data_fetcher.py`
- 停用：`jobs/radar_jobs.py`（对应 `accumulation_radar` 入口）
- `signal_service` 已改为纯主评分链路，不再叠加外部雷达增强

## 配置说明

路径由 `hermes_paths.py` 管理：

- `HERMES_HOME`（默认 `~/.hermes`）
- `~/.hermes/config`
- `~/.hermes/logs`

Telegram 配置优先级：

1. 环境变量 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`
2. `config/telegram.json`
3. `~/.hermes/config/telegram.json`

## 日志与数据

- 运行日志：`~/.hermes/logs/crypto_sword.log`
- 交易日志库：`~/.hermes/logs/trade_log.db`
- 日报统计：`trade_logger.py::get_daily_report`

## 后续路线图

### 短期（已完成）

- 下线 `data_fetcher.py`，清除重复抓取逻辑。
- 明确 `accumulation_radar` 策略为“移除系统入口”。

### 中期（进行中）

- 新增正式启动入口（已完成 `run_live.py`）。
- 持续扩展“策略评分 / 执行统计 / 监控通知”的统一 schema。

### 长期（规划）

- 建立统一特征仓（feature store）与回放框架，实现实盘-回测同源数据闭环。

## 风险提示

实盘交易存在亏损风险，请严格控制杠杆、单笔风险和最大回撤阈值。
