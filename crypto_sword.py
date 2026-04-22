#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║              🗡️  CRYPTO SWORD - 诸神黄昏之剑 🗡️               ║
║                                                               ║
║    统一 Binance 自动交易系统 — 实盘专用，1-10x 杠杆，          ║
║    山寨/meme 币专项扫描与执行                                  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

用法:
    crypto-sword --live        # 实盘模式（⚠️ 真实资金）
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

# Add scripts to path
sys.path.insert(0, str(Path("/root/.hermes/scripts")))

# ═══════════════════════════════════════════════════════════════
# 导入模块
# ═══════════════════════════════════════════════════════════════

try:
    from binance_breakout_scanner import (
        get_top_symbols_by_volume,
        get_top_symbols_by_change,
        scan_symbols,
        classify_and_direction,
        fetch_ticker_24hr,
    )
    from binance_trading_executor import (
        TradingSignal,
        cancel_protective_order,
        cancel_stop_loss_order,
        ensure_profile_selected,
        execute_trade,
        get_account_balance,
        place_market_order,
        place_stop_loss_order,
        should_trade,
        OrderResult,
    )
    from telegram_notifier import (
        format_close_position_msg,
        format_error_msg,
        format_shutdown_msg,
        format_summary_msg,
        format_startup_msg,
        get_telegram_config,
        send_telegram_message,
    )
    from trade_logger import TradeDatabase, TradeRecord  # 📜 神圣交易日志
    from surf_enhancer import (  # 🌊 Surf 数据增强
        get_market_overview,
        enhance_symbol_data,
        get_social_mindshare,
    )
    from signal_enhancer import (  # 🎯 信号增强
        score_signal,
        SignalScore,
    )
    from risk_manager import (  # 🛡️ 风控系统
        assess_trade_risk,
        RiskConfig,
        calculate_position_size,
    )
except ImportError as e:
    print(f"❌ 导入失败：{e}")
    print("请确保所有脚本位于 /root/.hermes/scripts/")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# 日志配置
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/root/.hermes/logs/crypto_sword.log"),
    ],
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# 配置类 - 雷神之锤的参数
# ═══════════════════════════════════════════════════════════════

class TradingConfig:
    """交易配置 - 诸神黄昏之剑的灵魂"""

    def __init__(
        self,
        # 模式（强制实盘）
        mode: str = "live",
        # 杠杆 - 奥丁的长矛
        leverage: int = 5,
        # 风控 - 英灵殿的盾牌
        risk_per_trade_pct: float = 0.5,
        stop_loss_pct: float = 8.0,
        take_profit_pct: float = 20.0,
        max_position_pct: float = 20.0,
        max_daily_loss_pct: float = 5.0,
        max_open_positions: int = 5,
        # 追踪止损 - 海姆达尔的守望
        trailing_stop_pct: float = 5.0,
        trailing_stop_enabled: bool = True,
        # 扫描 - 弗丽嘉的鹰眼
        scan_top_n: int = 50,
        scan_interval_sec: int = 300,
        min_stage: str = "pre_break",
        scan_by_change: bool = True,
        min_change_pct: float = 3.0,
        # 目标 - 矮人锻造的利刃
        target_altcoins: bool = True,
        target_memes: bool = True,
    ):
        self.mode = mode
        self.leverage = leverage
        self.risk_per_trade_pct = risk_per_trade_pct
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.max_position_pct = max_position_pct
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_open_positions = max_open_positions
        self.trailing_stop_pct = trailing_stop_pct
        self.trailing_stop_enabled = trailing_stop_enabled
        self.scan_top_n = scan_top_n
        self.scan_interval_sec = scan_interval_sec
        self.min_stage = min_stage
        self.scan_by_change = scan_by_change
        self.min_change_pct = min_change_pct
        self.target_altcoins = target_altcoins
        self.target_memes = target_memes

    @property
    def mode_emoji(self) -> str:
        return "💰"

    @property
    def mode_name(self) -> str:
        return "实盘"


# ═══════════════════════════════════════════════════════════════
# 持仓跟踪 - 瓦尔基里的记录
# ═══════════════════════════════════════════════════════════════

class Position:
    """持仓信息 - 英灵战士的荣耀"""

    def __init__(
        self,
        symbol: str,
        side: str,  # BUY (long) or SELL (short)
        entry_price: float,
        quantity: float,
        order_id: int,
        stop_loss_price: float,
        take_profit_price: float,
        entry_time: datetime,
        stage_at_entry: str,
        stop_loss_order_id: int = 0,
        session_id: str = "",
        target_roi_pct: float = 0.0,
        take_profit_targets: Optional[List[dict[str, Any]]] = None,
        take_profit_order_ids: Optional[List[int]] = None,
    ):
        self.symbol = symbol
        self.side = side
        self.entry_price = entry_price
        self.quantity = quantity
        self.order_id = order_id
        self.stop_loss_price = stop_loss_price
        self.take_profit_price = take_profit_price
        self.entry_time = entry_time
        self.stage_at_entry = stage_at_entry
        self.stop_loss_order_id = stop_loss_order_id
        self.session_id = session_id
        self.target_roi_pct = target_roi_pct
        self.take_profit_targets = take_profit_targets or []
        self.take_profit_order_ids = take_profit_order_ids or []
        self.initial_quantity = quantity

        # 动态跟踪
        self.highest_price: float = entry_price
        self.lowest_price: float = entry_price
        self.current_stop: float = stop_loss_price
        self.exit_price: Optional[float] = None
        self.exit_time: Optional[datetime] = None
        self.exit_reason: Optional[str] = None
        self.pnl: float = 0.0
        self.pnl_pct: float = 0.0

    def update_price(self, current_price: float, trailing_stop_pct: float):
        """更新价格并计算追踪止损"""
        if self.side == "BUY":  # Long
            if current_price > self.highest_price:
                self.highest_price = current_price
                if self.current_stop < self.highest_price * (1 - trailing_stop_pct / 100):
                    self.current_stop = self.highest_price * (1 - trailing_stop_pct / 100)
        else:  # Short
            if current_price < self.lowest_price:
                self.lowest_price = current_price
                if self.current_stop > self.lowest_price * (1 + trailing_stop_pct / 100):
                    self.current_stop = self.lowest_price * (1 + trailing_stop_pct / 100)

        # 计算未实现盈亏
        if self.side == "BUY":
            self.pnl = (current_price - self.entry_price) * self.quantity
            self.pnl_pct = (current_price - self.entry_price) / self.entry_price * 100
        else:
            self.pnl = (self.entry_price - current_price) * self.quantity
            self.pnl_pct = (self.entry_price - current_price) / self.entry_price * 100

    def check_exit_conditions(self, current_price: float) -> Optional[str]:
        """检查平仓条件"""
        if self.side == "BUY":
            if current_price <= self.current_stop:
                return "STOP_LOSS"
            if not self.take_profit_order_ids and current_price >= self.take_profit_price:
                return "TAKE_PROFIT"
        else:
            if current_price >= self.current_stop:
                return "STOP_LOSS"
            if not self.take_profit_order_ids and current_price <= self.take_profit_price:
                return "TAKE_PROFIT"
        return None

    def _format_take_profit_targets_text(self) -> str:
        if not self.take_profit_targets:
            return f"${self.take_profit_price:,.4f}"

        parts = []
        for target in self.take_profit_targets:
            roi_pct = float(target.get("target_roi_pct", 0) or 0)
            price = float(target.get("price", 0) or 0)
            parts.append(f"{roi_pct:.0f}%→${price:,.4f}")
        return " | ".join(parts)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "side": "LONG" if self.side == "BUY" else "SHORT",
            "entry_price": self.entry_price,
            "current_price": round(self.entry_price * (1 + self.pnl_pct / 100), 4) if self.side == "BUY" else round(self.entry_price * (1 - self.pnl_pct / 100), 4),
            "quantity": self.quantity,
            "entry_time": self.entry_time.isoformat(),
            "stop_loss": round(self.current_stop, 4),
            "take_profit": round(self.take_profit_price, 4),
            "target_roi_pct": round(self.target_roi_pct, 2),
            "take_profit_targets": self.take_profit_targets,
            "take_profit_targets_text": self._format_take_profit_targets_text(),
            "highest": round(self.highest_price, 4),
            "lowest": round(self.lowest_price, 4),
            "unrealized_pnl": round(self.pnl, 2),
            "unrealized_pnl_pct": round(self.pnl_pct, 2),
            "session_id": self.session_id,
        }


class PositionTracker:
    """持仓跟踪器 - 海姆达尔的守望"""

    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Position] = []

    def add_position(self, position: Position):
        self.positions[position.symbol] = position
        logger.info(f"📊 开仓：{position.symbol} {position.side} @ ${position.entry_price}")

    def remove_position(self, symbol: str):
        if symbol in self.positions:
            pos = self.positions.pop(symbol)
            self.closed_positions.append(pos)
            logger.info(f"✅ 平仓：{pos.symbol} | PnL: ${pos.pnl:.2f} ({pos.pnl_pct:.2f}%) | 原因：{pos.exit_reason}")

    def get_position(self, symbol: str) -> Optional[Position]:
        return self.positions.get(symbol)

    def get_open_count(self) -> int:
        return len(self.positions)

    def update_all_prices(self, prices: Dict[str, float], trailing_stop_pct: float):
        for symbol, position in self.positions.items():
            if symbol in prices:
                position.update_price(prices[symbol], trailing_stop_pct)

    def check_all_exits(self, prices: Dict[str, float]) -> Dict[str, str]:
        exits = {}
        for symbol, position in self.positions.items():
            if symbol in prices:
                reason = position.check_exit_conditions(prices[symbol])
                if reason:
                    exits[symbol] = reason
        return exits

    def get_summary(self) -> dict:
        total_pnl = sum(p.pnl for p in self.positions.values())
        return {
            "open_positions": len(self.positions),
            "total_unrealized_pnl": round(total_pnl, 2),
            "positions": [p.to_dict() for p in self.positions.values()],
            "closed_today": len(self.closed_positions),
            "realized_pnl": round(sum(p.pnl for p in self.closed_positions), 2),
        }

    def reset_daily_summary(self):
        self.closed_positions = []


# ═══════════════════════════════════════════════════════════════
# 主交易引擎 - 诸神黄昏之剑
# ═══════════════════════════════════════════════════════════════

class CryptoSword:
    """
    🗡️ CRYPTO SWORD - 诸神黄昏之剑

    监控与交易的化身，于测试之荒原与实战的腥风血雨间切换
    捕捉山寨与 meme 的血腥气息，执行雷霆般的杀伐
    """

    def __init__(self, config: TradingConfig):
        self.config = config
        self.tracker = PositionTracker()
        self.db = TradeDatabase()  # 📜 神圣交易日志
        self.daily_pnl = 0.0
        self.day_start_balance: float = 0.0
        self._daily_marker = datetime.now().date().isoformat()
        self._daily_loss_alert_sent = False
        self.traded_symbols_today: set = set()
        self.running = True
        
        # 通知控制
        self._last_summary_time: float = 0
        self._summary_interval: int = 6 * 3600  # 每 6 小时发送一次持仓汇总

    def _new_session_id(self, symbol: str) -> str:
        return f"{symbol}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    def _build_take_profit_plan(self) -> tuple[list[float], list[float]]:
        """Build default staged take-profit plan around the configured target ROI."""
        base_roi = max(float(self.config.take_profit_pct), 0.0)
        if base_roi <= 0:
            return [0.0], [1.0]

        staged_levels = []
        for multiplier in (0.5, 1.0, 1.5):
            roi = round(base_roi * multiplier, 2)
            if roi > 0 and roi not in staged_levels:
                staged_levels.append(roi)

        ratios = [0.5, 0.3, 0.2][:len(staged_levels)]
        ratio_total = sum(ratios) or 1.0
        ratios = [ratio / ratio_total for ratio in ratios]
        return staged_levels, ratios

    def _parse_trade_notes(self, notes: str) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for note in (notes or "").split(";"):
            if "=" not in note:
                continue
            key, value = note.split("=", 1)
            parsed[key] = value
        return parsed

    def _cancel_position_protection(self, position: Position):
        if position.stop_loss_order_id:
            if cancel_stop_loss_order(position.symbol, position.stop_loss_order_id):
                logger.info(f"🔕 已撤销 {position.symbol} 保护止损单：{position.stop_loss_order_id}")
            else:
                logger.warning(f"⚠️ {position.symbol} 保护止损单撤销失败：{position.stop_loss_order_id}")
                send_telegram_message(
                    format_error_msg(
                        error_type="止损单撤销失败",
                        message=f"order_id={position.stop_loss_order_id}",
                        symbol=position.symbol,
                        session_id=position.session_id,
                        component="stop_loss_cleanup",
                    )
                )

        for order_id in position.take_profit_order_ids:
            if not order_id:
                continue
            if cancel_protective_order(position.symbol, order_id):
                logger.info(f"🔕 已撤销 {position.symbol} 止盈委托：{order_id}")
            else:
                logger.warning(f"⚠️ {position.symbol} 止盈委托撤销失败：{order_id}")

    def _sync_positions_with_exchange(self):
        """Sync local tracked positions with real exchange positions for staged TP fills."""
        if not self.tracker.positions:
            return

        try:
            account_info = get_account_balance()
        except Exception as e:
            logger.warning(f"同步交易所持仓失败：{e}")
            return

        live_positions = {
            (item["symbol"], item["side"]): item
            for item in self._extract_live_positions(account_info if isinstance(account_info, dict) else {})
        }

        for symbol, position in list(self.tracker.positions.items()):
            side_key = "LONG" if position.side == "BUY" else "SHORT"
            live_pos = live_positions.get((symbol, side_key))

            if not live_pos:
                logger.warning(f"♻️ {symbol} 本地有仓位但交易所已无持仓，按交易所状态移除")
                current_price = self.get_current_prices([symbol]).get(symbol, position.take_profit_price or position.current_stop or position.entry_price)
                if position.side == "BUY":
                    inferred_reason = "STOP_LOSS" if current_price <= position.current_stop else "TAKE_PROFIT"
                    pnl = (current_price - position.entry_price) * position.quantity
                else:
                    inferred_reason = "STOP_LOSS" if current_price >= position.current_stop else "TAKE_PROFIT"
                    pnl = (position.entry_price - current_price) * position.quantity

                position.exit_price = current_price
                position.exit_time = datetime.now()
                position.exit_reason = f"{inferred_reason}_EXCHANGE"
                position.pnl = pnl
                position.pnl_pct = pnl / (position.entry_price * position.quantity) * 100 if position.entry_price and position.quantity else 0.0
                self.daily_pnl += pnl

                self._cancel_position_protection(position)
                self.tracker.remove_position(symbol)
                send_telegram_message(
                    format_close_position_msg(
                        symbol=symbol,
                        direction="LONG" if position.side == "BUY" else "SHORT",
                        entry_price=position.entry_price,
                        exit_price=current_price,
                        quantity=position.quantity,
                        pnl=pnl,
                        pnl_pct=position.pnl_pct,
                        reason=position.exit_reason,
                        duration_hours=(position.exit_time - position.entry_time).total_seconds() / 3600,
                        session_id=position.session_id,
                    )
                )

                open_trades = self.db.get_open_trades(mode=self.config.mode)
                for trade in open_trades:
                    if trade.symbol == symbol:
                        self.db.update_exit(
                            trade_id=trade.id,
                            exit_price=current_price,
                            exit_reason=position.exit_reason,
                            pnl=pnl,
                            pnl_pct=position.pnl_pct,
                            realized_pnl=pnl,
                        )
                        break
                continue

            live_qty = float(live_pos.get("quantity", 0) or 0)
            if live_qty <= 0:
                continue

            if live_qty + 1e-9 < position.quantity:
                reduced_qty = position.quantity - live_qty
                logger.info(f"🎯 {symbol} 交易所已部分止盈：减少 {reduced_qty:.6f}，剩余 {live_qty:.6f}")
            position.quantity = live_qty

    def _check_new_day(self):
        today = datetime.now().date().isoformat()
        if today != self._daily_marker:
            self._daily_marker = today
            self.daily_pnl = 0.0
            self.traded_symbols_today.clear()
            self.tracker.reset_daily_summary()
            self._daily_loss_alert_sent = False
            try:
                balance_info = get_account_balance()
                if isinstance(balance_info, dict):
                    self.day_start_balance = float(balance_info.get("availableBalance", self.day_start_balance or 0))
            except Exception:
                pass

    def _is_daily_loss_limit_hit(self) -> bool:
        if self.day_start_balance <= 0:
            return False
        limit_amount = self.day_start_balance * (self.config.max_daily_loss_pct / 100.0)
        return self.daily_pnl <= -limit_amount

    def _passes_liquidity_filter(self, symbol: str, desired_position_value: float) -> bool:
        try:
            ticker = fetch_ticker_24hr(symbol)
            quote_volume = float(ticker.get("quoteVolume", 0) or 0)
            if quote_volume < 250000:
                logger.warning(f"⚠️ {symbol} 24h 成交额过低，跳过：{quote_volume:.2f} USDT")
                return False

            position_to_volume_ratio = desired_position_value / quote_volume if quote_volume > 0 else 1.0
            if position_to_volume_ratio > 0.002:
                logger.warning(
                    f"⚠️ {symbol} 流动性不足，仓位/成交额占比过高：{position_to_volume_ratio:.4%}"
                )
                return False
            return True
        except Exception as e:
            logger.warning(f"⚠️ {symbol} 流动性检查失败：{e}")
            return False

    def _extract_live_positions(self, account_info: dict[str, Any]) -> list[dict[str, Any]]:
        live_positions = []
        for pos in account_info.get("positions", []) or []:
            position_amt = float(pos.get("positionAmt", 0) or 0)
            if abs(position_amt) <= 0:
                continue

            side = pos.get("positionSide")
            if not side or side == "BOTH":
                side = "LONG" if position_amt > 0 else "SHORT"

            live_positions.append({
                "symbol": pos.get("symbol", ""),
                "side": side,
                "quantity": abs(position_amt),
                "entry_price": float(pos.get("entryPrice", 0) or 0),
                "unrealized_pnl": float(pos.get("unRealizedProfit", 0) or 0),
            })
        return [p for p in live_positions if p["symbol"]]

    def _restore_positions(self, account_info: dict[str, Any]):
        live_positions = self._extract_live_positions(account_info)
        if not live_positions:
            return

        open_trades = self.db.get_open_trades(mode=self.config.mode)
        trades_by_symbol = {t.symbol: t for t in open_trades}

        for live_pos in live_positions:
            if live_pos["symbol"] in self.tracker.positions:
                continue

            side = "BUY" if live_pos["side"] == "LONG" else "SELL"
            trade = trades_by_symbol.get(live_pos["symbol"])
            entry_price = trade.entry_price if trade else live_pos["entry_price"]
            stop_loss_price = trade.stop_loss if trade else (
                entry_price * (1 - self.config.stop_loss_pct / 100) if side == "BUY"
                else entry_price * (1 + self.config.stop_loss_pct / 100)
            )
            notes_map = self._parse_trade_notes(trade.notes if trade else "")
            take_profit_targets: list[dict[str, Any]] = []
            take_profit_order_ids: list[int] = []
            target_roi_pct = float(notes_map.get("target_roi_pct", self.config.take_profit_pct) or self.config.take_profit_pct)
            if notes_map.get("tp_plan"):
                try:
                    take_profit_targets = json.loads(notes_map["tp_plan"])
                except Exception:
                    take_profit_targets = []
            if notes_map.get("tp_order_ids"):
                try:
                    take_profit_order_ids = [int(x) for x in notes_map["tp_order_ids"].split(",") if x.strip()]
                except Exception:
                    take_profit_order_ids = []

            take_profit_price = trade.take_profit if trade else (
                entry_price * (1 + (target_roi_pct / max(self.config.leverage, 1)) / 100) if side == "BUY"
                else entry_price * (1 - (target_roi_pct / max(self.config.leverage, 1)) / 100)
            )
            entry_time = datetime.fromisoformat(trade.entry_time) if trade and trade.entry_time else datetime.now()
            session_id = notes_map.get("session_id", "")
            if not session_id:
                session_id = self._new_session_id(live_pos["symbol"])

            restored = Position(
                symbol=live_pos["symbol"],
                side=side,
                entry_price=entry_price,
                quantity=live_pos["quantity"],
                order_id=trade.id if trade and trade.id else 0,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                entry_time=entry_time,
                stage_at_entry=trade.stage if trade else "restored",
                stop_loss_order_id=0,
                session_id=session_id,
                target_roi_pct=target_roi_pct,
                take_profit_targets=take_profit_targets,
                take_profit_order_ids=take_profit_order_ids,
            )
            self.tracker.add_position(restored)
            logger.warning(f"♻️ 已恢复持仓：{restored.symbol} {restored.side} session={session_id}")

    def _run_health_checks(self) -> float:
        telegram_config = get_telegram_config()
        if not telegram_config.get("bot_token") or not telegram_config.get("chat_id"):
            raise RuntimeError("Telegram 未配置 bot_token/chat_id")

        if not shutil.which("binance-cli"):
            raise RuntimeError("未找到 binance-cli")

        ensure_profile_selected("main")

        account_info = get_account_balance()
        if not isinstance(account_info, dict):
            raise RuntimeError("账户信息返回格式异常")

        balance = float(account_info.get("availableBalance", 0) or 0)
        if balance <= 0:
            raise RuntimeError("账户可用余额为 0")

        log_dir = Path("/root/.hermes/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(log_dir, os.W_OK):
            raise RuntimeError(f"日志目录不可写: {log_dir}")

        self._restore_positions(account_info)
        return balance

    def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        """获取当前价格"""
        prices = {}
        for symbol in symbols:
            try:
                ticker = fetch_ticker_24hr(symbol)
                prices[symbol] = float(ticker.get("lastPrice", 0))
            except Exception as e:
                logger.warning(f"获取 {symbol} 价格失败：{e}")
        return prices

    def scan_for_signals(self) -> List[dict]:
        """扫描交易信号 - 弗丽嘉的鹰眼"""
        if self.config.scan_by_change:
            symbols = get_top_symbols_by_change(
                self.config.scan_top_n,
                min_change=self.config.min_change_pct
            )
            logger.info(f"🔥 妖币模式 - 扫描 {len(symbols)} 个异动币种：{symbols[:5]}...")
        else:
            symbols = get_top_symbols_by_volume(self.config.scan_top_n)
            logger.info(f"📊 成交量模式 - 扫描 {len(symbols)} 个币种：{symbols[:5]}...")

        results = scan_symbols(symbols, min_stage=self.config.min_stage)

        signals = []
        for r in results:
            if r.stage in {"neutral", "error"}:
                continue
            if r.direction not in {"LONG", "SHORT"}:
                continue
            if r.symbol in self.tracker.positions:
                continue
            if r.symbol in self.traded_symbols_today:
                continue

            # 🎯 信号质量评分
            try:
                signal_score = score_signal(
                    symbol=r.symbol,
                    stage=r.stage,
                    direction=r.direction,
                    metrics=r.metrics,
                )
                
                # 过滤低质量信号（降低阈值：允许中等以上）
                if signal_score.confidence in {"低"}:
                    logger.info(f"🎯 {r.symbol} 信号质量过低 ({signal_score.total_score:.1f})，跳过")
                    continue
                
                # 记录评分
                logger.info(f"🎯 {r.symbol} 信号评分：{signal_score.total_score:.1f}/100 ({signal_score.confidence})")
                
            except Exception as e:
                logger.warning(f"信号评分失败 {r.symbol}: {e}")
                signal_score = None

            signals.append({
                "symbol": r.symbol,
                "stage": r.stage,
                "direction": r.direction,
                "price": r.metrics.get("last_price", 0),
                "metrics": r.metrics,
                "trigger": r.trigger,
                "risk": r.risk,
                "score": signal_score.to_dict() if signal_score else None,
            })

        # 按评分排序
        signals.sort(key=lambda s: s.get("score", {}).get("total_score", 0), reverse=True)
        
        logger.info(f"📡 发现 {len(signals)} 个有效信号（已按质量排序）")

        return signals

    def execute_entry(self, signal: dict) -> Optional[Position]:
        """执行开仓 - 奥丁的长矛"""
        symbol = signal["symbol"]
        direction = signal["direction"]
        price = signal["price"]

        try:
            trading_signal = TradingSignal(
                symbol=symbol,
                stage=signal["stage"],
                direction=direction,
                entry_price=price,
                metrics=signal["metrics"],
            )
            session_id = self._new_session_id(symbol)
            risk_level = "UNKNOWN"

            if not should_trade(trading_signal):
                return None

            # 获取账户余额
            try:
                balance_info = get_account_balance()
                if isinstance(balance_info, dict):
                    balance = float(balance_info.get("availableBalance", 10000))
                else:
                    balance = 10000
            except Exception:
                balance = 10000

            quantity = None
            stop_loss = None

            # 🛡️ 风控评估
            try:
                existing_positions = []
                for pos_symbol, pos in self.tracker.positions.items():
                    existing_positions.append({
                        "symbol": pos_symbol,
                        "side": "LONG" if pos.side == "BUY" else "SHORT",
                        "position_value": pos.entry_price * pos.quantity,
                    })

                risk_config = RiskConfig(
                    risk_per_trade_pct=self.config.risk_per_trade_pct,
                    base_stop_loss_pct=self.config.stop_loss_pct,
                    base_take_profit_pct=self.config.take_profit_pct,
                    max_position_pct=self.config.max_position_pct,
                    max_total_exposure=50.0,
                    max_correlated_positions=3,
                )

                risk_result = assess_trade_risk(
                    symbol=symbol,
                    side="LONG" if direction == "LONG" else "SHORT",
                    entry_price=price,
                    account_balance=balance,
                    existing_positions=existing_positions,
                    config=risk_config,
                )

                if not risk_result.get("can_open", False):
                    logger.warning(f"🛡️ {symbol} 风控拒绝：{risk_result.get('warnings', [])}")
                    return None

                logger.info(f"🛡️ {symbol} 风控评分：{risk_result.get('risk_score', 0)}/100 ({risk_result.get('risk_level', 'UNKNOWN')})")
                risk_level = risk_result.get("risk_level", "UNKNOWN")

                position_size = risk_result.get("position_size", {})
                quantity = position_size.get("quantity")
                stop_loss = risk_result.get("stop_loss", {}).get("stop_loss")
                position_value = float(position_size.get("position_value", 0) or 0)

                if quantity is not None and quantity <= 0:
                    logger.warning(f"🛡️ {symbol} 仓位计算失败")
                    return None

                if position_value > 0 and not self._passes_liquidity_filter(symbol, position_value):
                    return None

                logger.info(
                    f"🔍 {symbol} 风控参数: 余额=${balance:.2f}, 杠杆={self.config.leverage}x, "
                    f"名义仓位=${position_size.get('position_value', 0):.2f}, "
                    f"数量={quantity}, 止损=${(stop_loss or 0):.4f}"
                )

            except Exception as e:
                logger.warning(f"🛡️ 风控评估失败 {symbol}: {e}，回退到执行器默认计算")

            # 执行交易（优先使用风控计算的仓位和止损）
            take_profit_roi_pcts, take_profit_ratios = self._build_take_profit_plan()
            result = execute_trade(
                signal=trading_signal,
                account_balance=balance,
                risk_per_trade_pct=self.config.risk_per_trade_pct,
                stop_loss_pct=self.config.stop_loss_pct,
                max_position_pct=self.config.max_position_pct,
                leverage=self.config.leverage,
                quantity=quantity,
                stop_loss_price=stop_loss,
                take_profit_roi_pcts=take_profit_roi_pcts,
                take_profit_ratios=take_profit_ratios,
            )

            if result.get("action") != "EXECUTED":
                logger.warning(f"❌ {symbol} 开仓失败：{result.get('reason', 'Unknown')}")
                return None

            entry_order = result.get("entry_order", {})
            executed_entry_price = float(entry_order.get("executed_price", price) or price)
            take_profit_targets = result.get("take_profit_orders", [])
            take_profit_prices = result.get("take_profit_prices", [])
            target_roi_pcts = result.get("take_profit_roi_pcts", take_profit_roi_pcts)
            primary_target_roi_pct = float(self.config.take_profit_pct)
            tp_price = float(take_profit_prices[0] if take_profit_prices else executed_entry_price)
            if direction == "LONG":
                side = "BUY"
            else:
                side = "SELL"

            stop_loss_order = result.get("stop_loss_order", {})
            position = Position(
                symbol=symbol,
                side=side,
                entry_price=executed_entry_price,
                quantity=result.get("quantity", 0),
                order_id=result.get("order_id", 0),
                stop_loss_price=result.get("stop_loss_price", 0),
                take_profit_price=tp_price,
                entry_time=datetime.now(),
                stage_at_entry=signal["stage"],
                stop_loss_order_id=stop_loss_order.get("order_id", 0),
                session_id=session_id,
                target_roi_pct=primary_target_roi_pct,
                take_profit_targets=take_profit_targets,
                take_profit_order_ids=[int(item.get("order_id", 0) or 0) for item in take_profit_targets if item.get("order_id")],
            )

            from telegram_notifier import format_open_position_msg

            score = signal.get("score", {}).get("total_score", 0) if signal.get("score") else 0

            msg = format_open_position_msg(
                symbol=symbol,
                direction=direction,
                entry_price=executed_entry_price,
                quantity=position.quantity,
                leverage=self.config.leverage,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                risk_amount=result.get('risk_amount_usdt', 0),
                risk_pct=self.config.risk_per_trade_pct,
                score=score,
                risk_level=risk_level,
                session_id=session_id,
                target_roi_pct=primary_target_roi_pct,
                take_profit_targets=take_profit_targets,
            )
            send_telegram_message(msg)

            notes_parts = [
                f"session_id={session_id}",
                f"risk_level={risk_level}",
                f"target_roi_pct={primary_target_roi_pct}",
                f"tp_plan={json.dumps(take_profit_targets, separators=(',', ':'))}",
                f"tp_order_ids={','.join(str(int(item.get('order_id', 0))) for item in take_profit_targets if item.get('order_id'))}",
            ]

            trade = TradeRecord(
                symbol=symbol,
                side=direction,
                direction=side,
                stage=signal["stage"],
                entry_price=executed_entry_price,
                quantity=position.quantity,
                leverage=self.config.leverage,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                entry_time=position.entry_time.isoformat(),
                mode=self.config.mode,
                market_snapshot=signal.get("metrics", {}),
                notes=";".join(notes_parts),
            )
            trade_id = self.db.add_trade(trade)
            logger.info(f"📜 交易已记录 (ID: {trade_id})")

            return position

        except Exception as e:
            # 捕获所有异常，防止单币种失败阻塞主循环
            logger.error(f"❌ {symbol} 开仓流程异常：{e}", exc_info=True)
            send_telegram_message(
                format_error_msg(
                    error_type="开仓流程异常",
                    message=str(e),
                    symbol=symbol,
                    session_id=session_id if 'session_id' in locals() else "",
                    component="execute_entry",
                )
            )
            return None

    def execute_exit(self, symbol: str, reason: str) -> bool:
        """执行平仓 - 托尔的雷霆"""
        position = self.tracker.get_position(symbol)
        if not position:
            return False

        try:
            prices = self.get_current_prices([symbol])
            current_price = prices.get(symbol, 0)
        except Exception:
            current_price = 0

        close_side = "SELL" if position.side == "BUY" else "BUY"
        position_side = "LONG" if position.side == "BUY" else "SHORT"

        # 实盘平仓
        try:
            result = place_market_order(
                symbol,
                close_side,
                position.quantity,
                position_side=position_side,
                reduce_only=True,
            )

            if result.status == "FILLED":
                if position.side == "BUY":
                    pnl = (result.executed_price - position.entry_price) * position.quantity
                else:
                    pnl = (position.entry_price - result.executed_price) * position.quantity

                position.exit_price = result.executed_price
                position.exit_time = datetime.now()
                position.exit_reason = reason
                position.pnl = pnl
                position.pnl_pct = pnl / (position.entry_price * position.quantity) * 100

                self.tracker.remove_position(symbol)
                self.daily_pnl += pnl
                self._cancel_position_protection(position)

                duration_hours = (position.exit_time - position.entry_time).total_seconds() / 3600
                send_telegram_message(
                    format_close_position_msg(
                        symbol=symbol,
                        direction="LONG" if position.side == "BUY" else "SHORT",
                        entry_price=position.entry_price,
                        exit_price=result.executed_price,
                        quantity=position.quantity,
                        pnl=pnl,
                        pnl_pct=position.pnl_pct,
                        reason=reason,
                        duration_hours=duration_hours,
                        session_id=position.session_id,
                    )
                )

                # 📜 更新交易日志
                open_trades = self.db.get_open_trades(mode=self.config.mode)
                for t in open_trades:
                    if t.symbol == symbol:
                        self.db.update_exit(
                            trade_id=t.id,
                            exit_price=result.executed_price,
                            exit_reason=reason,
                            pnl=pnl,
                            pnl_pct=position.pnl_pct,
                            realized_pnl=pnl,
                        )
                        logger.info(f"📜 交易已更新 (ID: {t.id})")
                        break

                return True

        except Exception as e:
            logger.error(f"平仓失败 {symbol}: {e}")
            send_telegram_message(
                format_error_msg(
                    error_type="平仓失败",
                    message=str(e),
                    symbol=symbol,
                    session_id=position.session_id,
                    component="execute_exit",
                )
            )

        return False

    def run_scan_cycle(self):
        """运行一次完整的扫描 - 交易循环"""
        self._check_new_day()
        self._sync_positions_with_exchange()
        logger.info("=" * 60)
        logger.info(f"🔄 扫描周期开始 | 持仓：{self.tracker.get_open_count()}/{self.config.max_open_positions}")

        # 1. 更新持仓价格并检查平仓
        open_symbols = list(self.tracker.positions.keys())
        if open_symbols:
            prices = self.get_current_prices(open_symbols)
            self.tracker.update_all_prices(prices, self.config.trailing_stop_pct)

            exits = self.tracker.check_all_exits(prices)
            for symbol, reason in exits.items():
                logger.info(f"🚨 {symbol} 触发 {reason}")
                self.execute_exit(symbol, reason)

        # 2. 扫描新信号
        signals = self.scan_for_signals()
        logger.info(f"📡 发现 {len(signals)} 个交易信号")

        if self._is_daily_loss_limit_hit():
            if not self._daily_loss_alert_sent:
                limit_amount = self.day_start_balance * (self.config.max_daily_loss_pct / 100.0)
                msg = format_error_msg(
                    error_type="日内熔断",
                    message=f"当日亏损已达到 {self.daily_pnl:.2f} USDT，超过限制 {-limit_amount:.2f} USDT，暂停新开仓",
                    component="risk_guard",
                )
                send_telegram_message(msg)
                self._daily_loss_alert_sent = True
            signals = []

        # 3. 执行开仓
        for signal in signals:
            if self.tracker.get_open_count() >= self.config.max_open_positions:
                logger.info(f"⏸️ 已达最大持仓数 ({self.config.max_open_positions})")
                break

            position = self.execute_entry(signal)
            if position:
                self.tracker.add_position(position)
                self.traded_symbols_today.add(signal["symbol"])

        # 4. 发送持仓摘要（日志 + 定期 Telegram 通知）
        summary = self.tracker.get_summary()
        logger.info(f"📊 持仓摘要：{summary['open_positions']} 个 | 未实现 PnL: ${summary['total_unrealized_pnl']:.2f}")

        # 定期发送 Telegram 持仓汇总（每 6 小时）
        import time
        current_time = time.time()
        if current_time - self._last_summary_time >= self._summary_interval:
            self._send_position_summary(summary)
            self._last_summary_time = current_time

        self.last_scan_time = datetime.now()

    def _send_position_summary(self, summary: dict):
        """发送持仓汇总通知"""
        msg = format_summary_msg(
            positions=summary["positions"],
            total_pnl=summary["total_unrealized_pnl"],
            realized_pnl=summary["realized_pnl"],
        )
        msg += f"\n\n<b>已平仓</b>  <code>{summary['closed_today']}</code> 笔"
        send_telegram_message(msg)

    def run(self):
        """主循环 - 诸神黄昏的永恒之战"""
        mode_text = f"{self.config.mode_emoji} {self.config.mode_name} 模式"
        logger.info("=" * 60)
        logger.info(f"⚔️  {mode_text} 启动")
        logger.info(f"🔧 杠杆：{self.config.leverage}x | 风险：{self.config.risk_per_trade_pct}%")
        logger.info(f"🛡️  止损：{self.config.stop_loss_pct}% | 止盈：{self.config.take_profit_pct}%")
        logger.info(f"👁️  扫描：前{self.config.scan_top_n}币种 | 间隔：{self.config.scan_interval_sec}s")
        logger.info(f"📈 最大持仓：{self.config.max_open_positions} 个")
        logger.info("=" * 60)

        try:
            self.day_start_balance = self._run_health_checks()
            logger.info(f"🩺 启动健康检查通过 | 可用余额: ${self.day_start_balance:.2f}")
        except Exception as e:
            logger.error(f"❌ 启动健康检查失败：{e}")
            send_telegram_message(
                format_error_msg(
                    error_type="启动健康检查失败",
                    message=str(e),
                    component="startup_checks",
                )
            )
            raise

        # 发送启动通知
        send_telegram_message(
            format_startup_msg(
                mode_name=mode_text,
                leverage=self.config.leverage,
                risk_pct=self.config.risk_per_trade_pct,
                stop_loss_pct=self.config.stop_loss_pct,
                take_profit_pct=self.config.take_profit_pct,
                scan_top_n=self.config.scan_top_n,
                scan_interval_sec=self.config.scan_interval_sec,
                max_positions=self.config.max_open_positions,
            )
        )

        # 🌊 获取市场概览（Surf 数据增强）
        try:
            market_overview = get_market_overview()
            logger.info(f"🌊 市场环境：{market_overview.get('market_sentiment', 'NEUTRAL')}")
            logger.info(f"🌊 恐慌贪婪：{market_overview.get('fear_greed', {})}")
            logger.info(f"🌊 清算风险：{market_overview.get('liquidation_risk', 'LOW')}")
        except Exception as e:
            logger.warning(f"🌊 获取市场概览失败：{e}")

        # 主循环
        while self.running:
            try:
                self.run_scan_cycle()
                logger.info(f"⏳ 等待 {self.config.scan_interval_sec}s 后下次扫描...")
                time.sleep(self.config.scan_interval_sec)
            except KeyboardInterrupt:
                logger.info("\n🛑 用户中断，正在停止...")
                self.running = False
            except Exception as e:
                logger.error(f"❌ 循环错误：{e}")
                send_telegram_message(
                    format_error_msg(
                        error_type="主循环异常",
                        message=str(e),
                    )
                )
                time.sleep(10)

        # 停止通知
        summary = self.tracker.get_summary()
        send_telegram_message(
            format_shutdown_msg(
                mode_name=mode_text,
                closed_trades=summary["closed_today"],
                realized_pnl=summary["realized_pnl"],
                unrealized_pnl=summary["total_unrealized_pnl"],
            )
        )


# ═══════════════════════════════════════════════════════════════
# CLI 入口 - 英灵殿的大门
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="🗡️  CRYPTO SWORD - 诸神黄昏之剑",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  crypto-sword --live                 # 实盘模式（⚠️ 真实资金）
  crypto-sword --live --leverage 10   # 实盘 + 10x 杠杆
        """,
    )

    # 实盘模式（强制）
    parser.add_argument("--live", action="store_true", required=True, help="实盘模式（⚠️ 真实资金）")

    # 杠杆 - 奥丁的长矛
    parser.add_argument("--leverage", "-l", type=int, default=5, choices=range(1, 11),
                        metavar="1-10", help="杠杆倍数 (1-10x, 默认：5x)")

    # 风控 - 英灵殿的盾牌
    parser.add_argument("--risk", "-r", type=float, default=0.5, help="每笔风险 %% (默认：0.5%%)")
    parser.add_argument("--stop-loss", "-s", type=float, default=8.0, help="止损 %% (默认：8%%)")
    parser.add_argument("--take-profit", "-t", type=float, default=20.0, help="止盈 %% (默认：20%%)")
    parser.add_argument("--max-positions", "-m", type=int, default=5, help="最大持仓数 (默认：5)")
    parser.add_argument("--max-daily-loss", type=float, default=5.0, help="每日最大亏损 %% (默认：5%%)")

    # 扫描 - 弗丽嘉的鹰眼
    parser.add_argument("--top", type=int, default=50, help="扫描前 N 个币种 (默认：50)")
    parser.add_argument("--interval", "-i", type=int, default=300, help="扫描间隔秒数 (默认：300)")
    parser.add_argument("--min-change", type=float, default=3.0, help="最小涨幅 %% (默认：3%%)")
    parser.add_argument("--by-volume", action="store_true", help="按成交量排序（默认按涨幅）")

    # 追踪止损 - 海姆达尔的守望
    parser.add_argument("--trailing", type=float, default=5.0, help="追踪止损 %% (默认：5%%)")
    parser.add_argument("--no-trailing", action="store_true", help="禁用追踪止损")

    args = parser.parse_args()

    # 实盘模式确认
    mode = "live"
    print("\n" + "=" * 50)
    print("⚠️  ⚠️  ⚠️  实盘交易警告  ⚠️  ⚠️  ⚠️")
    print("=" * 50)
    print("\n即将使用 REAL MONEY 进行交易！")
    print(f"杠杆：{args.leverage}x | 风险：{args.risk}% | 止损：{args.stop_loss}%")
    print("\n确认继续？输入 'y' 继续，其他键取消：")
    if not sys.stdin.isatty():
        print("ℹ️ 后台模式，跳过确认")
        confirm = "y"
    else:
        confirm = input("> ").strip().lower()
    if confirm != "y":
        print("❌ 已取消")
        sys.exit(0)

    # 创建配置
    config = TradingConfig(
        mode=mode,
        leverage=args.leverage,
        risk_per_trade_pct=args.risk,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        max_position_pct=20.0,
        max_daily_loss_pct=args.max_daily_loss,
        max_open_positions=args.max_positions,
        trailing_stop_pct=args.trailing,
        trailing_stop_enabled=not args.no_trailing,
        scan_top_n=args.top,
        scan_interval_sec=args.interval,
        min_stage="pre_break",
        scan_by_change=not args.by_volume,
        min_change_pct=args.min_change,
    )

    # 启动交易引擎
    trader = CryptoSword(config)
    trader.run()


if __name__ == "__main__":
    main()
