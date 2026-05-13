"""Execution and protection logic mixin for the trading engine."""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from typing import Optional

from feature_store import feature_store
from telegram_notifier import (
    format_direction_label,
    format_entry_failure_detail,
    format_error_msg,
    format_protection_failure_detail,
    send_telegram_message,
)
from services.capital_allocator import capital_allocator
from services.execution_service import execution_service
from services.order_service import order_service
from services.risk_service import risk_service
from services.exit_service import ExitServiceMixin
from services.position_lifecycle import PositionLifecycleMixin
from services.protection_service import ProtectionServiceMixin

from .monitoring import build_execution_event, message_signature
from .models import Position

logger = logging.getLogger(__name__)

class ExecutionMixin(ProtectionServiceMixin, PositionLifecycleMixin, ExitServiceMixin):
    """Open/close execution and protective order lifecycle."""

    @staticmethod
    def _signal_float(signal: dict, key: str, default: float = 0.0) -> float:
        metrics = signal.get("metrics", {}) or {}
        try:
            return float(metrics.get(key, default) or default)
        except (TypeError, ValueError):
            return default

    def _allow_high_reversal_short_spike_bypass(self, signal: dict, spike_reason: str) -> bool:
        """Allow qualified blow-off-top SHORT fast lanes through the generic spike guard."""
        if not getattr(self.config, "fast_lane_spike_guard_bypass_enabled", True):
            return False
        if str(signal.get("direction", "") or "").upper() != "SHORT":
            return False

        marker = " ".join(
            str(signal.get(key, "") or "")
            for key in ("entry_status_text", "watch_stage", "entry_note", "strategy_line")
        )
        if "高位转弱快线" not in marker and "冲高回落做空" not in marker:
            return False

        score_data = signal.get("score") or {}
        try:
            score = float(score_data.get("total_score", score_data.get("total", 0)) if isinstance(score_data, dict) else score_data or 0)
        except (TypeError, ValueError):
            score = 0.0
        change_24h = self._signal_float(signal, "change_24h_pct")
        drawdown = self._signal_float(signal, "drawdown_from_24h_high_pct")
        oi_change = abs(self._signal_float(signal, "oi_24h_pct", self._signal_float(signal, "oi_change_pct")))
        rebound_match = re.search(r"反弹\s*([0-9.]+)%", spike_reason or "")
        rebound_pct = float(rebound_match.group(1)) if rebound_match else 0.0

        return (
            score >= 80.0
            and change_24h >= 8.0
            and drawdown >= 3.0
            and oi_change >= 20.0
            and rebound_pct <= 6.0
        )

    def execute_entry(self, signal: dict) -> Optional[Position]:
        """执行开仓 - 奥丁的长矛"""
        symbol = signal["symbol"]
        direction = signal["direction"]
        price = float(signal.get("price", 0) or 0)
        position: Optional[Position] = None
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []
        entry_status = str(signal.get("entry_status", "") or "")
        status_text = str(signal.get("entry_status_text", "") or "")
        watch_stage = str(signal.get("watch_stage", "") or "")
        entry_note = str(signal.get("entry_note", "") or "")
        score_conf = str((signal.get("score") or {}).get("confidence", "") or "")
        guard_text = f"{status_text}|{watch_stage}|{entry_note}|{score_conf}"

        if entry_status != "ready":
            logger.warning(f"entry guard reject {symbol}: entry_status={entry_status}")
            return None
        if price <= 0:
            logger.warning(f"entry guard reject {symbol}: invalid price={price}")
            return None
        if any(token in guard_text for token in ("失效", "淘汰", "移出监控", "状态变更")):
            logger.warning(f"entry guard reject {symbol}: blocked by monitor state [{guard_text}]")
            return None

        if self._new_entries_suspended:
            logger.warning(f"🛡️ {symbol} 新开仓暂停：存在保护单不完整的持仓")
            if not self._new_entries_suspended_alert_sent:
                send_telegram_message(
                    format_error_msg(
                        error_type="新开仓已暂停",
                        message="存在未完整受保护的持仓，请先确认止损/止盈保护单。",
                        symbol=symbol,
                        component="protection_guard",
                    )
                )
                self._new_entries_suspended_alert_sent = True
            return None

        try:
            trading_signal = execution_service.build_trading_signal(
                symbol=symbol,
                stage=signal["stage"],
                direction=direction,
                entry_price=price,
                metrics=signal["metrics"],
            )
            session_id = self._new_session_id(symbol)
            risk_level = "未评估"
            strategy_line = str(signal.get("strategy_line", "回踩确认线") or "回踩确认线")
            strategy_profile = self._strategy_profile(strategy_line)
            exit_profile = self._exit_profile_for_signal(signal)
            exit_profile_name = str(exit_profile.get("name", "默认策略") or "默认策略")
            take_profit_mode_for_trade = str(exit_profile.get("take_profit_mode", self.config.take_profit_mode) or self.config.take_profit_mode)
            stop_loss_pct = float(exit_profile.get("stop_loss_pct", self._strategy_stop_loss_pct(strategy_line)) or self._strategy_stop_loss_pct(strategy_line))
            stop_trigger_buffer_pct = self._strategy_stop_trigger_buffer_pct(strategy_line)
            score_data = signal.get("score") or {}
            score = float(score_data.get("total_score", score_data.get("total", 0)) if isinstance(score_data, dict) else score_data or 0)
            direction_label = format_direction_label(direction)
            notify_reject = entry_status == "ready"

            if (
                str(signal.get("stage", "") or "") == "mania"
                and direction == "LONG"
                and not getattr(self.config, "allow_mania_long_entries", False)
            ):
                logger.warning(f"entry guard reject {symbol}: mania LONG is disabled")
                if notify_reject:
                    send_telegram_message(
                        format_error_msg(
                            error_type="过热追多拦截",
                            message=(
                                "阶段：开仓前阶段过滤\n"
                                "原因：mania 过热阶段不再允许做多直通，等待回踩后二次确认\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="execute_entry",
                        )
                    )
                return None

            if not execution_service.should_trade(trading_signal):
                if notify_reject:
                    send_telegram_message(
                        format_error_msg(
                            error_type="执行服务拒绝开仓",
                            message=(
                                "阶段：开仓前执行过滤\n"
                                "原因：信号未满足执行服务的最终下单条件\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="execute_entry",
                        )
                    )
                return None

            if symbol not in self.tracker.positions:
                step_started = time.perf_counter()
                self._cancel_symbol_stale_protection(symbol, session_id=session_id, reason="before_new_entry")
                self._record_latency_step(latency_steps, "stale_protection_cleanup", step_started)

            step_started = time.perf_counter()
            try:
                balance_hint = signal.get("_balance_hint")
                if balance_hint is not None:
                    balance = float(balance_hint)
                else:
                    balance_info = self._get_account_info_cached(ttl_sec=3.0)
                    balance = float(balance_info.get("availableBalance", 10000))
            except Exception as e:
                logger.error(f"entry guard reject {symbol}: account balance query failed: {e}")
                send_telegram_message(
                    format_error_msg(
                        error_type="账户查询失败，拒绝开仓",
                        message=(
                            "阶段：账户余额查询\n"
                            f"原因：{format_entry_failure_detail(e)}\n"
                            f"方向：{direction_label}\n"
                            f"策略：{strategy_line}｜{exit_profile_name}\n"
                            f"评分：{score:.1f}"
                        ),
                        symbol=symbol,
                        session_id=session_id,
                        component="account_query",
                    )
                )
                return None
            self._record_latency_step(latency_steps, "account_query", step_started)

            step_started = time.perf_counter()
            latest_price = self.get_current_prices([symbol]).get(symbol, price)
            if latest_price and price > 0:
                if direction == "LONG":
                    slippage_pct = (latest_price - price) / price * 100.0
                else:
                    slippage_pct = (price - latest_price) / price * 100.0
                if slippage_pct > self.config.max_entry_slippage_pct:
                    logger.warning(
                        f"🧊 {symbol} 下单前价格偏移过大，放弃开仓: signal={price:.8f}, latest={latest_price:.8f}, "
                        f"slippage={slippage_pct:.2f}%"
                    )
                    if notify_reject:
                        send_telegram_message(
                            format_error_msg(
                                error_type="价格偏移过大，拒绝开仓",
                                message=(
                                    "阶段：下单前价格复查\n"
                                    f"原因：{format_entry_failure_detail('价格偏移过大')}\n"
                                    f"方向：{direction_label}\n"
                                    f"信号价：{price:.8f}\n"
                                    f"最新价：{latest_price:.8f}\n"
                                    f"偏移：{slippage_pct:.2f}% > {self.config.max_entry_slippage_pct:.2f}%\n"
                                    f"策略：{strategy_line}｜{exit_profile_name}\n"
                                    f"评分：{score:.1f}"
                                ),
                                symbol=symbol,
                                session_id=session_id,
                                component="execute_entry",
                            )
                        )
                    return None
                if latest_price > 0:
                    price = latest_price
                    trading_signal.entry_price = latest_price
            # ready 信号已经过策略确认；短线插针只记录，不再二次拦截主链机会。
            spike_reason = self._recent_spike_reversal_reason(symbol, direction, price)
            if spike_reason:
                if (
                    getattr(self.config, "ready_signal_spike_guard_bypass", True)
                    and entry_status == "ready"
                ) or self._allow_high_reversal_short_spike_bypass(signal, spike_reason):
                    logger.info(f"✅ {symbol} ready信号忽略短线插针复查：{spike_reason}")
                    spike_reason = ""
            if spike_reason:
                logger.warning(f"🧊 {symbol} 开仓前过滤：{spike_reason}")
                if notify_reject:
                    send_telegram_message(
                        format_error_msg(
                            error_type="短线插针风险，拒绝开仓",
                            message=(
                                "阶段：下单前K线复查\n"
                                f"原因：{spike_reason}\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="execute_entry",
                        )
                    )
                return None
            self._record_latency_step(latency_steps, "price_recheck", step_started)

            quantity = None
            stop_loss = None
            capital_plan = None
            entry_scale = {"mode": "full", "ratio": 1.0, "label": "完整仓"}
            risk_balance = balance
            entry_risk_pct = self.config.risk_per_trade_pct
            entry_max_position_pct = self.config.max_position_pct
            entry_leverage = self.config.leverage

            step_started = time.perf_counter()
            try:
                existing_positions = []
                for pos_symbol, pos in self.tracker.positions.items():
                    existing_positions.append(
                        {
                            "symbol": pos_symbol,
                            "side": "LONG" if pos.side == "BUY" else "SHORT",
                            "position_value": pos.entry_price * pos.quantity,
                        }
                    )
                dynamic_limits = self._dynamic_risk_limits(signal)
                logger.info(
                    f"🛡️ {symbol} 风控预算：敞口={dynamic_limits['max_total_exposure']}% "
                    f"相关仓={dynamic_limits['max_correlated_positions']} "
                    f"模式={dynamic_limits['mode']} 原因={dynamic_limits['reason']}"
                )

                capital_plan = capital_allocator.build_plan(
                    config=self.config,
                    signal=signal,
                    exit_profile=exit_profile,
                    dynamic_limits=dynamic_limits,
                    account_balance=balance,
                    day_start_balance=self.day_start_balance,
                    daily_pnl=self.daily_pnl,
                    daily_report=self._get_daily_report_snapshot(ttl_sec=90.0),
                    market_style_mode=self._market_style_mode,
                )
                logger.info(
                    f"🏦 {symbol} 资金分配：{capital_plan.mode} | "
                    f"风险={capital_plan.risk_per_trade_pct:.2f}% 杠杆={capital_plan.leverage}x "
                    f"仓位上限={capital_plan.max_position_pct:.1f}% 敞口={capital_plan.max_total_exposure_pct:.1f}% "
                    f"EV={capital_plan.expected_rr:.2f}R 原因={capital_plan.reason}"
                )
                if not capital_plan.allowed:
                    if notify_reject:
                        send_telegram_message(
                            format_error_msg(
                                error_type="资本分配拒绝",
                                message=(
                                    "阶段：资金分配\n"
                                    f"原因：{capital_plan.reason}\n"
                                    f"方向：{direction_label}\n"
                                    f"策略：{strategy_line}｜{exit_profile_name}\n"
                                    f"评分：{score:.1f}\n"
                                    f"资金档位：{capital_plan.mode}\n"
                                    f"期望收益：{capital_plan.expected_reward_pct:.2f}%\n"
                                    f"止损：{stop_loss_pct:.2f}%"
                                ),
                                symbol=symbol,
                                session_id=session_id,
                                component="risk_assessment",
                            )
                        )
                    return None

                risk_balance = capital_plan.effective_balance if capital_plan.effective_balance > 0 else balance
                entry_risk_pct = capital_plan.risk_per_trade_pct
                entry_max_position_pct = capital_plan.max_position_pct
                entry_leverage = capital_plan.leverage
                entry_scale = self._entry_scale_profile(signal)
                scale_ratio = float(entry_scale.get("ratio", 1.0) or 1.0)
                if scale_ratio < 1.0:
                    entry_risk_pct *= scale_ratio
                    entry_max_position_pct *= scale_ratio
                    logger.info(
                        f"{symbol} 试探仓模式：首仓 {scale_ratio * 100:.0f}% "
                        f"风险={entry_risk_pct:.2f}% 仓位上限={entry_max_position_pct:.2f}% "
                        f"原因={entry_scale.get('reason', '')}"
                    )

                risk_config = risk_service.build_config(
                    risk_per_trade_pct=entry_risk_pct,
                    base_stop_loss_pct=stop_loss_pct,
                    base_take_profit_pct=self.config.take_profit_pct * strategy_profile["tp_multiplier"],
                    max_position_pct=entry_max_position_pct,
                    max_total_exposure=float(capital_plan.max_total_exposure_pct),
                    max_correlated_positions=int(capital_plan.max_correlated_positions),
                )

                risk_result = risk_service.assess(
                    symbol=symbol,
                    side="LONG" if direction == "LONG" else "SHORT",
                    entry_price=price,
                    account_balance=risk_balance,
                    existing_positions=existing_positions,
                    config=risk_config,
                )

                if not risk_result.get("can_open", False):
                    warnings = risk_result.get("warnings", [])
                    logger.warning(f"🛡️ {symbol} 风控拒绝：{warnings}")
                    send_telegram_message(
                        format_error_msg(
                            error_type="强信号风控拒绝",
                            message=(
                                "阶段：风控评估\n"
                                f"原因：{'; '.join(str(item) for item in warnings) or '风控未通过'}\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}\n"
                                f"资金档位：{capital_plan.mode}\n"
                                f"敞口上限：{capital_plan.max_total_exposure_pct}%\n"
                                f"相关仓上限：{capital_plan.max_correlated_positions}\n"
                                f"说明：{capital_plan.reason}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="risk_assessment",
                        )
                    )
                    return None

                logger.info(
                    f"🛡️ {symbol} 风控评分：{risk_result.get('risk_score', 0)}/100 ({risk_result.get('risk_level', 'UNKNOWN')})"
                )
                risk_level = risk_result.get("risk_level") or "未评估"

                position_size = risk_result.get("position_size", {})
                quantity = position_size.get("quantity")
                stop_loss = risk_result.get("stop_loss", {}).get("stop_loss")
                position_value = float(position_size.get("position_value", 0) or 0)
                scale_ratio = float(entry_scale.get("ratio", 1.0) or 1.0)
                if scale_ratio > 0 and quantity:
                    entry_scale["intended_quantity"] = float(quantity) / scale_ratio
                else:
                    entry_scale["intended_quantity"] = float(quantity or 0)

                if quantity is not None and quantity <= 0:
                    logger.warning(f"🛡️ {symbol} 仓位计算失败")
                    send_telegram_message(
                        format_error_msg(
                            error_type="仓位计算失败，拒绝开仓",
                            message=(
                                "阶段：风控仓位计算\n"
                                "原因：计算出的下单数量小于等于 0，可能是余额、精度或最小名义价值限制导致\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}\n"
                                f"余额：{balance:.2f} USDT\n"
                                f"风险余额：{risk_balance:.2f} USDT"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="risk_assessment",
                        )
                    )
                    return None

                if position_value > 0 and not self._passes_liquidity_filter(symbol, position_value):
                    if notify_reject:
                        send_telegram_message(
                            format_error_msg(
                                error_type="流动性过滤失败，拒绝开仓",
                                message=(
                                    "阶段：流动性检查\n"
                                    f"原因：{format_entry_failure_detail('流动性过滤未通过')}\n"
                                    f"方向：{direction_label}\n"
                                    f"策略：{strategy_line}｜{exit_profile_name}\n"
                                    f"评分：{score:.1f}\n"
                                    f"计划名义仓位：{position_value:.2f} USDT"
                                ),
                                symbol=symbol,
                                session_id=session_id,
                                component="risk_assessment",
                            )
                        )
                    return None

                logger.info(
                    f"🔍 {symbol} 风控参数: 余额=${balance:.2f}, 风险余额=${risk_balance:.2f}, 杠杆={capital_plan.leverage}x, "
                    f"名义仓位=${position_size.get('position_value', 0):.2f}, "
                    f"数量={quantity}, 止损=${(stop_loss or 0):.4f}"
                )

            except Exception as e:
                logger.warning(f"🛡️ 风控评估失败 {symbol}: {e}，回退到执行器默认计算")
            self._record_latency_step(latency_steps, "risk_assessment", step_started)

            take_profit_target_pcts = [float(item) for item in (exit_profile.get("take_profit_targets") or [])]
            take_profit_ratios = [float(item) for item in (exit_profile.get("take_profit_ratios") or [])]
            if not take_profit_target_pcts or not take_profit_ratios:
                take_profit_target_pcts, take_profit_ratios = self._build_take_profit_plan(strategy_line)
            step_started = time.perf_counter()
            result = execution_service.execute_entry_trade(
                signal=trading_signal,
                account_balance=risk_balance,
                risk_per_trade_pct=entry_risk_pct,
                stop_loss_pct=stop_loss_pct,
                max_position_pct=entry_max_position_pct,
                leverage=entry_leverage,
                quantity=quantity,
                stop_loss_price=stop_loss,
                take_profit_target_pcts=take_profit_target_pcts,
                take_profit_ratios=take_profit_ratios,
                take_profit_mode=take_profit_mode_for_trade,
                stop_trigger_buffer_pct=stop_trigger_buffer_pct,
                defer_protection_orders=False,
            )
            self._record_latency_step(latency_steps, "execute_trade", step_started)

            if result.get("action") != "EXECUTED":
                failure_reason = str(result.get("reason", "Unknown") or "Unknown")
                logger.warning(f"❌ {symbol} 开仓失败：{failure_reason}")
                send_telegram_message(
                    format_error_msg(
                        error_type="开仓下单失败",
                        message=(
                            "阶段：交易所下单\n"
                            f"原因：{format_entry_failure_detail(failure_reason)}\n"
                            f"方向：{direction_label}\n"
                            f"数量：{quantity}\n"
                            f"杠杆：{entry_leverage}x\n"
                            f"策略：{strategy_line}｜{exit_profile_name}\n"
                            f"评分：{score:.1f}\n"
                            f"计划入场价：{price:.8f}"
                        ),
                        symbol=symbol,
                        session_id=session_id,
                        component="execute_entry_trade",
                    )
                )
                self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                return None

            entry_order = result.get("entry_order", {})
            executed_entry_price = float(entry_order.get("executed_price", price) or price)
            order_status = entry_order.get("status", "UNKNOWN")
            actual_quantity = float(entry_order.get("quantity", 0) or result.get("quantity", 0))

            if quantity is None:
                quantity = actual_quantity if actual_quantity > 0 else 0

            if order_status == "PARTIALLY_FILLED":
                logger.warning(f"⚠️ {symbol} 部分成交！请求数量：{quantity}，实际成交：{actual_quantity}")
                if actual_quantity < quantity * 0.5:
                    logger.error(f"❌ {symbol} 部分成交比例过低，放弃持仓")
                    send_telegram_message(
                        format_error_msg(
                            error_type="开仓部分成交异常",
                            message=(
                                "阶段：成交结果确认\n"
                                "原因：部分成交比例过低，系统放弃继续持仓\n"
                                f"方向：{direction_label}\n"
                                f"请求数量：{quantity}\n"
                                f"实际成交：{actual_quantity}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="execute_entry",
                        )
                    )
                    self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                    return None

            take_profit_targets = result.get("take_profit_orders", [])
            take_profit_prices = result.get("take_profit_prices", [])
            target_roi_pcts = result.get("take_profit_roi_pcts", [])
            target_price_pcts = result.get("take_profit_price_pcts", take_profit_target_pcts)
            primary_target_roi_pct = float(
                target_roi_pcts[0] if target_roi_pcts else self.config.take_profit_pct * self.config.leverage
            )
            primary_price_move_pct = float(target_price_pcts[0] if target_price_pcts else self.config.take_profit_pct)
            tp_price = float(take_profit_prices[0] if take_profit_prices else executed_entry_price)
            if direction == "LONG":
                side = "BUY"
            else:
                side = "SELL"

            stop_loss_order = result.get("stop_loss_order", {})
            protection_deferred = bool(result.get("protection_deferred", False))
            protection_errors: list[str] = []
            if protection_deferred:
                protection_errors.append("protection_deferred=true")

            stop_loss_order_id = int(stop_loss_order.get("order_id", 0) or 0)
            stop_loss_status = str(stop_loss_order.get("status", "") or "").upper()
            if stop_loss_order_id <= 0 or stop_loss_status == "ERROR":
                sl_message = str(stop_loss_order.get("message", "") or "").strip()
                protection_errors.append(
                    f"stop_loss status={stop_loss_status or 'UNKNOWN'} id={stop_loss_order_id}"
                    + (f" msg={sl_message}" if sl_message else "")
                )

            for idx, target in enumerate(take_profit_targets, start=1):
                tp_order_id = int(target.get("order_id", 0) or 0)
                tp_status = str(target.get("status", "") or "").upper()
                if tp_order_id <= 0 or tp_status == "ERROR":
                    tp_message = str(target.get("message", "") or "").strip()
                    protection_errors.append(
                        f"tp{idx} status={tp_status or 'UNKNOWN'} id={tp_order_id}"
                        + (f" msg={tp_message}" if tp_message else "")
                    )

            if protection_errors:
                close_side = "SELL" if direction == "LONG" else "BUY"
                flat_result = order_service.quick_close(
                    symbol=symbol,
                    side=close_side,
                    quantity=actual_quantity,
                    reason="ENTRY_PROTECTION_FAILED",
                )
                detail = "; ".join(protection_errors)
                logger.error(f"entry protection hard-fail {symbol}: {detail} | flat={flat_result}")
                protection_event = build_execution_event(
                    event="entry_protection_failed",
                    symbol=symbol,
                    direction=direction,
                    session_id=session_id,
                    metrics={
                        "detail": detail,
                        "flat_success": bool(flat_result.get("success")),
                        "flat_order_id": int(flat_result.get("order_id", 0) or 0),
                        "flat_elapsed_ms": float(flat_result.get("elapsed_ms", 0) or 0),
                        "entry_order_id": int(result.get("order_id", 0) or 0),
                        "entry_price": executed_entry_price,
                        "entry_quantity": actual_quantity,
                    },
                )
                logger.info(f"execution_event {message_signature(protection_event)}")
                feature_store.append_event(protection_event)
                send_telegram_message(
                    format_error_msg(
                        error_type="开仓保护单失败已回滚",
                        message=(
                            "阶段：开仓后保护单确认\n"
                            f"原因：{format_protection_failure_detail(detail)}\n"
                            f"方向：{direction_label}\n"
                            f"成交数量：{actual_quantity}\n"
                            f"成交价：{executed_entry_price:.8f}\n"
                            f"回滚结果：{'已尝试市价平仓' if flat_result else '未获得回滚结果'}\n"
                            f"原始信息：{detail}"
                        ),
                        symbol=symbol,
                        session_id=session_id,
                        component="entry_protection",
                    )
                )
                self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                return None

            oi_funding = signal.get("oi_funding") or {}
            leverage_applied = int(result.get("leverage_applied", entry_leverage) or entry_leverage or self.config.leverage)
            position = Position(
                symbol=symbol,
                side=side,
                entry_price=executed_entry_price,
                quantity=actual_quantity,
                order_id=result.get("order_id", 0),
                stop_loss_price=result.get("stop_loss_price", 0),
                take_profit_price=tp_price,
                entry_time=datetime.now(),
                stage_at_entry=signal["stage"],
                strategy_line=strategy_line,
                stop_loss_order_id=stop_loss_order_id,
                session_id=session_id,
                oi_funding=oi_funding,
                entry_score=dict(signal.get("score") or {}),
                entry_metrics=dict(signal.get("metrics") or {}),
                target_roi_pct=primary_target_roi_pct,
                take_profit_targets=take_profit_targets,
                take_profit_order_ids=[
                    int(item.get("order_id", 0) or 0) for item in take_profit_targets if item.get("order_id")
                ],
                leverage=leverage_applied,
                entry_scale_mode=str(entry_scale.get("mode", "full") or "full"),
                entry_scale_ratio=float(entry_scale.get("ratio", 1.0) or 1.0),
                intended_quantity=float(entry_scale.get("intended_quantity", actual_quantity) or actual_quantity),
                add_on_done=bool(entry_scale.get("add_on_done", False)),
            )

            from telegram_notifier import format_open_position_msg

            msg = format_open_position_msg(
                symbol=symbol,
                direction=direction,
                entry_price=executed_entry_price,
                quantity=position.quantity,
                leverage=leverage_applied,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                risk_amount=result.get("risk_amount_usdt", 0),
                risk_pct=entry_risk_pct,
                score=score,
                risk_level=risk_level,
                session_id=session_id,
                strategy_line=f"{strategy_line}｜{exit_profile_name}"
                + (f"｜{entry_scale.get('label')}" if entry_scale.get("mode") == "probe" else ""),
                oi_funding=oi_funding,
                target_roi_pct=primary_target_roi_pct,
                price_move_pct=primary_price_move_pct,
                take_profit_targets=take_profit_targets,
                capital_plan=capital_plan.to_dict() if capital_plan else None,
            )
            notify_ok = send_telegram_message(msg)
            if not notify_ok:
                logger.error(f"entry notify failed: {symbol} session={session_id}")
                notify_event = build_execution_event(
                    event="entry_notify_failed",
                    symbol=symbol,
                    direction=direction,
                    session_id=session_id,
                    metrics={"reason": "telegram_send_failed"},
                )
                feature_store.append_event(notify_event)

            step_started = time.perf_counter()
            self._ensure_position_protection(position)
            self._send_protection_status(position, source="entry_confirm", force=True)
            self._record_latency_step(latency_steps, "protection_confirm", step_started)
            protection_ok_event = build_execution_event(
                event="entry_protection_ok",
                symbol=symbol,
                direction=direction,
                session_id=session_id,
                metrics={
                    "stop_loss_order_id": int(position.stop_loss_order_id or 0),
                    "take_profit_order_count": len(position.take_profit_order_ids or []),
                    "entry_order_id": int(position.order_id or 0),
                    "entry_price": float(position.entry_price or 0),
                    "entry_quantity": float(position.quantity or 0),
                },
            )
            feature_store.append_event(protection_ok_event)

            step_started = time.perf_counter()
            self._persist_entry_opened(
                signal=signal,
                position=position,
                direction=direction,
                side=side,
                session_id=session_id,
                strategy_line=strategy_line,
                exit_profile_name=exit_profile_name,
                risk_level=risk_level,
                primary_target_roi_pct=primary_target_roi_pct,
                primary_price_move_pct=primary_price_move_pct,
                take_profit_mode_for_trade=take_profit_mode_for_trade,
                strategy_profile=strategy_profile,
                stop_loss_pct=stop_loss_pct,
                stop_trigger_buffer_pct=stop_trigger_buffer_pct,
                leverage_applied=leverage_applied,
                capital_plan=capital_plan,
                oi_funding=oi_funding,
                take_profit_targets=take_profit_targets,
                tp_price=tp_price,
                executed_entry_price=executed_entry_price,
                entry_scale=entry_scale,
            )
            self._record_latency_step(latency_steps, "db_write", step_started)
            self._emit_latency_trace("execute_entry", trace_started, latency_steps, symbol=symbol)
            # P5: 更新最后交易时间，用于空仓超时提醒
            self._last_entry_or_signal_time = time.time()
            return position

        except Exception as e:
            logger.error(f"❌ {symbol} 开仓流程异常：{e}", exc_info=True)
            score_text = f"{score:.1f}" if "score" in locals() else "未知"
            send_telegram_message(
                format_error_msg(
                    error_type="开仓流程异常",
                    message=(
                        "阶段：开仓流程总异常\n"
                        f"原因：{format_entry_failure_detail(e)}\n"
                        f"方向：{format_direction_label(direction) if 'direction' in locals() else '未知方向'}\n"
                        f"策略：{strategy_line if 'strategy_line' in locals() else '未知策略'}\n"
                        f"评分：{score_text}"
                    ),
                    symbol=symbol,
                    session_id=session_id if "session_id" in locals() else "",
                    component="execute_entry",
                )
            )
            if position is not None:
                logger.error(f"entry post-process failed but position exists: {symbol} session={position.session_id}")
                self._emit_latency_trace("execute_entry_post_error", trace_started, latency_steps, symbol=symbol)
                return position
            self._emit_latency_trace("execute_entry_exception", trace_started, latency_steps, symbol=symbol)
            return None
    def execute_exit(self, symbol: str, reason: str) -> bool:
        """???? - ?????"""
        return self._execute_exit_impl(symbol, reason)

