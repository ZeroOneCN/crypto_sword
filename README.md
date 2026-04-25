# 🗡️ Crypto Sword - 诸神黄昏之剑

> 统一 Binance 实盘自动交易系统 — 1-10x 杠杆，山寨/meme 币专项扫描与执行，庄家收筹雷达

## 架构

```
crypto_sword.py              # 主入口 - 诸神黄昏之剑（统一调度器）
├── binance_breakout_scanner.py  # 突破扫描器 - 发现机会
├── binance_trading_executor.py  # 交易执行器 - 执行订单
├── signal_enhancer.py           # 信号增强 - 多维度评分
├── risk_manager.py              # 风控系统 - 仓位管理
├── surf_enhancer.py             # Surf 数据增强 - 链上/舆情
├── accumulation_radar.py        # 🏦 庄家收筹雷达 - OI异动/暗流检测
├── trade_logger.py              # 交易日志 - SQLite 持久化
├── telegram_notifier.py         # Telegram 通知
├── speed_executor.py            # 高速执行器
└── backtester.py                # 回测引擎
```

## 🏦 庄家雷达功能（新增）

整合自 [accumulation-radar](https://github.com/connectfarm1/accumulation-radar)：

### 核心功能

| 功能 | 说明 | 使用频率 |
|------|------|----------|
| 🏦 **收筹池扫描** | 检测横盘≥45天的低调收筹标的 | 每日1次 |
| ⚡ **OI异动监控** | 实时检测OI变化≥3%的标的 | 每小时1次 |
| 🎯 **暗流信号** | OI变但价没动 = 最佳埋伏时机 | 实时推送 |
| 🔥 **空头燃料** | 负费率+价格上涨 = 轧空潜力 | 每小时1次 |

### 三策略评分体系

**🔥 追多策略（短线轧空）**
- 资金费率越负 = 做空人越多 = 空头燃料
- 前提：涨>3% + 费率<-0.005% + Vol>$1M

**📊 综合策略（四维均衡，各25分）**
| 维度 | 权重 | 逻辑 |
|------|------|------|
| 🧊 费率 | 25分 | 越负越好 |
| 💎 市值 | 25分 | <$50M满分 |
| 💤 横盘 | 25分 | ≥120天满分 |
| ⚡ OI | 25分 | ≥15%变化满分 |

**🎯 埋伏策略（中长线布局）**
- 市值(35%) + OI(30%) + 横盘(20%) + 费率(15%)
- 前提：在收筹池内 + 涨幅<50%

### 使用方式

```bash
# 更新收筹池（每日）
python3 accumulation_radar.py pool

# OI异动监控（每小时）
python3 accumulation_radar.py oi

# 全量扫描
python3 accumulation_radar.py full

# 主程序自动集成雷达评分
python3 crypto_sword.py --live
```

## 快速开始

```bash
# 实盘模式（⚠️ 真实资金）
python3 crypto_sword.py --live

# 自定义参数
python3 crypto_sword.py --live --leverage 10 --risk 0.5 --stop-loss 8 --take-profit 20 --top 30 --interval 300 --trailing 5 --max-positions 5
```

> ⚠️ **警告**：本系统仅支持实盘交易。首次使用请确保已配置好 `binance-cli` 实盘 profile，并从小杠杆（3-5x）和低风险（0.5-1%）开始测试。

## 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--live` | - | 实盘模式（必须） |
| `--leverage` | 5 | 杠杆倍数 (1-10x) |
| `--risk` | 1.0 | 每笔风险 (%) |
| `--stop-loss` | 5 | 止损 (%) |
| `--take-profit` | 10 | 止盈 (%) |
| `--trailing` | 3 | 追踪止损 (%) |
| `--max-positions` | 3 | 最大持仓数 |
| `--top` | 30 | 扫描 Top N 币种 |
| `--interval` | 300 | 扫描间隔 (秒) |

## 目录结构

```
scripts/
├── 🗡️ crypto_sword.py              # ✅ 主入口（唯一交易程序，集成所有核心功能）
├── 📡 binance_breakout_scanner.py   # 突破扫描器（被主程序调用）
├── ⚔️ binance_trading_executor.py   # 交易执行器（被主程序调用）
├── 🎯 signal_enhancer.py            # 信号增强（被主程序调用）
├── 🛡️ risk_manager.py               # 风控系统（被主程序调用）
├── 🌊 surf_enhancer.py              # Surf 数据增强（被主程序调用）
├── 🏦 accumulation_radar.py         # ✨ 庄家收筹雷达（新增强力模块）
├── 📜 trade_logger.py               # 交易日志（被主程序调用）
├── 📱 telegram_notifier.py          # Telegram 通知（被主程序调用）
├── 📊 backtester.py                 # 回测引擎（独立工具）
├── 📈 backtest_analyzer.py          # 回测分析（独立工具）
├── ⚡ speed_executor.py             # ⚠️ 可选：高速执行器（WebSocket，勿与主程序同时运行）
├── 📡 token_anomaly_radar.py        # 异常检测（被主程序调用）
├── 🗑️ binance_monitor.py            # ❌ 已废弃（功能已集成到主程序）
├── 🗑️ trailing_stop.py              # ❌ 已废弃（功能已集成到主程序）
├── 🗑️ binance_auto_trader.py.bak    # 📦 旧版备份
└── 🧪 tests/                        # 测试目录
```

> ⚠️ **重要**：本系统**仅使用 `crypto_sword.py` 作为主程序**。其他模块均为工具库或已废弃脚本，请勿独立运行（除 `accumulation_radar.py` 和 `speed_executor.py` 外，但也请勿与主程序同时运行）。

## 配置

### Binance API

```bash
# 使用 binance-cli 配置实盘 profile
binance-cli profile create --name main --env mainnet --api-key "YOUR_KEY" --api-secret "YOUR_SECRET" -f
binance-cli profile select --name main
```

### Telegram 通知

编辑 `config/telegram.json`:
```json
{
  "bot_token": "YOUR_BOT_TOKEN",
  "chat_id": "YOUR_CHAT_ID"
}
```

## 通知功能

`crypto_sword.py` 集成了完整的 Telegram 通知系统：

| 通知类型 | 触发条件 | 内容 |
|---------|---------|------|
| 🚀 启动通知 | 程序启动 | 杠杆、风险、止损、止盈、扫描参数 |
| 🟢 开仓通知 | 成功开仓 | 币种、方向、入场价、仓位、止损、止盈、风险、雷达评分 |
| 🔴 平仓通知 | 触发止损/止盈 | 币种、原因、入场/出场价、PnL |
| ❌ 错误通知 | 开仓失败/API 错误 | 错误详情、当前状态 |
| 📊 持仓汇总 | 每 6 小时 | 当前持仓、未实现 PnL、已平仓 PnL |
| 🛑 停止通知 | 程序退出 | 总交易数、总盈亏、未实现 PnL |
| 🎯 暗流信号 | OI异动但价不动 | 庄家收筹最佳埋伏时机 |
| 🏦 收筹池报告 | 每日更新 | 横盘≥45天的低调收筹标的 |

> 💡 确保已配置 `config/telegram.json` 或环境变量 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID`。

## 策略说明

**突破交易策略**：
- 扫描 24h 成交量和涨跌幅 Top 币种
- 检测价格突破近期高低点
- 结合资金费率、多空比、舆情评分
- 🏦 新增：OI异动、横盘天数、市值三策略评分
- 多维度信号评分后执行交易
- 自动追踪止损止盈

**庄家雷达核心理论**：
> 庄家拉盘前必须先收筹 → 长期横盘+低量 = 收筹中 → OI暴涨 = 大资金进场 = 即将拉盘

## 日志

```
logs/
├── crypto_sword.log     # 主程序日志
└── trade_log.db         # SQLite 交易日志
```

## 风险警告

⚠️ **实盘交易涉及真实资金，请谨慎使用！**
- 设置合理的风控参数（建议低杠杆 3-5x 起步）
- 严格控制仓位和风险（每笔风险 0.5-1%）
- 监控 Telegram 通知，实时掌握交易动态
- 定期查看交易日志和回测报告优化策略

## 致谢

- [accumulation-radar](https://github.com/connectfarm1/accumulation-radar) - 庄家收筹雷达核心逻辑

## License

MIT