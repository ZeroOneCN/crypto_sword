# Crypto Sword - 宙斯交易中枢

> Binance 合约自动化交易系统，包含扫描、执行、风控与 Telegram 通知。

## 运行入口

### 主入口：`crypto_sword.py`

默认参数直接启动：

```bash
python3 crypto_sword.py
```

自定义参数启动示例：

```bash
python3 crypto_sword.py \
  --leverage 5 \
  --risk 2.5 \
  --stop-loss 8 \
  --take-profit 20 \
  --max-positions 6 \
  --max-position-pct 30 \
  --top 30 \
  --interval 180
```

后台运行示例：

```bash
nohup python3 crypto_sword.py > /root/.hermes/logs/crypto_sword.log 2>&1 &
```

停止：

```bash
pkill -f "crypto_sword.py"
```

### 简化入口：`run_live.py`

```bash
python3 run_live.py --mode live --leverage 5 --risk 2.5
```

## 常用参数

- `--leverage`：杠杆倍数（1-10）
- `--risk`：单笔风险百分比
- `--stop-loss`：止损百分比
- `--take-profit`：止盈百分比
- `--max-positions`：最大持仓数
- `--max-position-pct`：单仓最大资金占比
- `--max-daily-loss`：每日最大亏损百分比
- `--top`：扫描币种数量
- `--interval`：扫描间隔（秒）
- `--no-daily-report`：关闭每日复盘

查看完整参数：

```bash
python3 crypto_sword.py --help
```

## 通知与数据

- 运行日志：`~/.hermes/logs/crypto_sword.log`
- 交易日志库：`~/.hermes/logs/trade_log.db`
- Telegram 配置：`config/telegram.json` 或环境变量
- Binance 配置：`config/binance.json`（或 binance-cli profile）

## 服务器更新流程

```bash
cd /your/path/crypto_sword
git fetch origin
git checkout master
git pull --ff-only origin master
pkill -f "crypto_sword.py"
nohup python3 crypto_sword.py > /root/.hermes/logs/crypto_sword.log 2>&1 &
```

## 风险提示

实盘交易存在亏损风险，请严格控制杠杆与回撤。
