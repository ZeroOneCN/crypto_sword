# 🗡️ Crypto Sword - 诸神黄昏之剑

> Binance 合约自动化交易系统 · 分层架构 · 实时通知

## 运行入口

### 主入口：crypto_sword.py（完整参数控制）

实际生产环境使用此入口，支持全部 30+ 参数：

```bash
python3 crypto_sword.py --live \
  --leverage 5 \
  --risk 2 \
  --stop-loss 5 \
  --take-profit 10 \
  --trailing 3 \
  --max-positions 5 \
  --top 30 \
  --interval 300
```

**后台运行：**

```bash
nohup python3 crypto_sword.py --live \
  --leverage 5 --risk 2 --stop-loss 5 --take-profit 10 \
  --trailing 3 --max-positions 5 --top 30 --interval 300 \
  --max-abs-funding-rate 0.005 --max-range-position 92 --max-chase-change 40 \
  > /root/.hermes/logs/crypto_sword.log 2>&1 &
```

**停止：**

```bash
pkill -f "crypto_sword.py"
```

### 简化入口：run_live.py（快速启动）

提供合理默认值，适合快速测试：

```bash
python3 run_live.py --mode live --leverage 5 --risk 2
```

## CLI 参数

### 基础参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--live` | - | 实盘模式（当前唯一支持的模式） |
| `--leverage` / `-l` | 5 | 杠杆倍数 (1-10x) |
| `--risk` / `-r` | 2.0 | 每笔风险百分比 |
| `--stop-loss` / `-s` | 8.0 | 止损基础百分比 |
| `--take-profit` / `-t` | 20.0 | 止盈基础百分比 (ROI口径) |
| `--trailing` | 5.0 | 追踪止损百分比 |
| `--max-positions` / `-m` | 5 | 最大持仓数 |
| `--max-daily-loss` | 5.0 | 每日最大亏损百分比 |

### 扫描参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--top` | 30 | 扫描前 N 个币种 |
| `--interval` / `-i` | 300 | 扫描间隔秒数 |
| `--scan-workers` | 6 | 深度扫描并发数 |
| `--min-change` | 2.0 | 最小涨跌幅百分比 |
| `--min-pullback` | 2.0 | 最小回踩百分比 |
| `--reclaim-volume` | 1.05 | 回踩后 5m 量能回归倍数 |
| `--by-volume` | - | 按成交量排序（默认按涨幅） |
| `--max-chase-change` | 35.0 | 最大追涨 24h 涨幅% |
| `--max-range-position` | 92.0 | 最大 24h 区间位置% |

### 入场确认

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--no-entry-confirm` | - | 禁用回踩确认入场 |
| `--entry-confirm-timeout` | 1800 | 候选观察超时秒数 |
| `--no-momentum-entry` | - | 禁用强趋势动量入场 |
| `--momentum-score` | 60 | 动量入场最低评分 |
| `--accumulation-score` | 50 | 暗流最低评分 |
| `--accumulation-min-oi` | 10 | 暗流最小 OI 变化% |
| `--accumulation-max-change` | 15 | 暗流最大涨跌幅% |

### 风控

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--max-abs-funding-rate` | 0.005 | 最大绝对资金费率 (0.5%) |
| `--max-consecutive-losses` | 3 | 连续亏损熔断笔数 |
| `--loss-pause-mins` | 30 | 连续亏损后暂停分钟 |
| `--no-trailing` | - | 禁用追踪止损 |
| `--no-daily-report` | - | 禁用每日复盘通知 |

## 止盈止损机制

### 基础值 vs 实际值

CLI 参数指定的是**基础值**，实际开仓时会根据策略线应用乘数：

| 策略线 | SL 乘数 | TP 乘数 |
|--------|---------|---------|
| 趋势突破线 | 0.85 | 1.1 |
| 回踩确认线 | 1.0 | 0.95 |

例：`--stop-loss 5 --take-profit 10`
- 突破线开仓：SL = 5% × 0.85 = **4.25%**，TP = 10% × 1.1 = **11% ROI**
- 回踩线开仓：SL = 5% × 1.0 = **5%**，TP = 10% × 0.95 = **9.5% ROI**

### 分批止盈

每笔交易自动分 3 档止盈：

| 档位 | ROI 目标 | 价格目标 | 比例 |
|------|---------|---------|------|
| TP1 | 基础 × 0.5 | 基础价格 × 0.5 | 55% (回踩) / 40% (突破) |
| TP2 | 基础 × 1.0 | 基础价格 × 1.0 | 30% (回踩) / 35% (突破) |
| TP3 | 基础 × 1.5 | 基础价格 × 1.5 | 15% (回踩) / 25% (突破) |

### 追踪止损

启用后（`--trailing 3`），价格向有利方向移动时止损自动跟随，锁定利润。

### 保本止损

首次止盈成交后，止损自动移动到保本位附近，确保盈利单不会变亏损。

## 当前架构

```text
crypto_sword.py                # 主编排器（完整 CLI 入口）
run_live.py                    # 简化入口（快速启动）
core/
  models.py                    # TradingConfig + Position 数据模型
  bootstrap_service.py         # 启动/关闭流程编排
  cycle_mixin.py               # 主循环调度
  scanner_mixin.py             # 币种扫描与信号生成
  execution_mixin.py           # 开仓/平仓/保护单管理
  sync_mixin.py                # 持仓同步与恢复
  market_mixin.py              # 市场状态监控
  confirmation_mixin.py        # 入场确认（回踩/动量/暗流）
  monitoring.py                # 监控/结构化输出工具
services/
  signal_service.py            # 信号评分服务
  risk_service.py              # 风控计算服务
  execution_service.py         # 交易执行服务
  order_service.py             # 订单管理服务
  oi_funding_service.py        # OI/Funding 数据服务
adapters/
  rest_gateway.py              # Binance REST API 适配
  ws_gateway.py                # Binance WebSocket 适配
feature_store/
  store.py                     # 事件与复盘 JSONL 持久化
  reviewer.py                  # 每笔交易复盘摘要构建
binance_api_client.py          # Binance 原生 API 客户端
binance_trading_executor.py    # Binance 交易执行器
trade_logger.py                # SQLite 交易日志
telegram_notifier.py           # Telegram 通知（🟢🔴 emoji）
```

## 通知系统

所有交易通知通过 Telegram 实时推送，使用 emoji 标识：

- 🟢 盈利 / 做多
- 🔴 亏损 / 做空
- 🛑 系统停止
- 🚀 系统启动
- 🎯 分批止盈
- 🛡️ 保护单确认

启动消息显示基础 TP/SL 值，并标注"实际值因策略线乘数不同而浮动"。

## 日志与数据

- 运行日志：`~/.hermes/logs/crypto_sword.log`
- 交易日志库：`~/.hermes/logs/trade_log.db`
- 特征仓事件：`~/.hermes/logs/feature_store/<YYYY-MM-DD>/events.ndjson`
- 交易复盘：`~/.hermes/logs/feature_store/<YYYY-MM-DD>/reviews.ndjson`

## 配置说明

路径由 `hermes_paths.py` 管理：

- `HERMES_HOME`（默认 `~/.hermes`）
- `~/.hermes/config`
- `~/.hermes/logs`

Telegram 配置优先级：

1. 环境变量 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`
2. `config/telegram.json`
3. `~/.hermes/config/telegram.json`

Binance API 配置：使用 `binance-cli` 的 `main` profile（`~/.config/binance-cli/`）

## 信号评分系统

每笔候选交易通过多维度评分决定是否入场：

1. **阶段评分** - 价格在 24h 区间的位置（突破前/突破中/狂热/衰竭）
2. **OI/Funding 增强** - 持仓量变化 + 资金费率趋势
3. **动量入场** - 强趋势直接入场（评分 ≥ 60）
4. **暗流入场** - 横盘蓄势 + OI 增长入场（评分 ≥ 50）
5. **回踩确认** - 价格回踩 + 量能回归后入场

## 风控规则

- 单笔风险 ≤ 总余额的 2%
- 最大持仓 5 个
- 每日最大亏损 5%
- 连续亏损 3 笔 → 暂停 30 分钟
- 币种冷却 24 小时（亏损后）
- 裸仓保护失败 → 暂停新开仓
- 拒绝高波动市场（ATR > 10%、总敞口 > 50%）

## 风险提示

实盘交易存在亏损风险，请严格控制杠杆、单笔风险和最大回撤阈值。
