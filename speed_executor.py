#!/usr/bin/env python3
"""
⚡ SPEED EXECUTOR - 高速执行系统（可选/高级功能）

⚠️  警告：此为独立的高级工具，使用 WebSocket 实时推送。
请勿与 crypto_sword.py 同时运行，否则会导致：
- 重复开仓/平仓
- 持仓跟踪冲突

适用场景：
- 需要极低延迟的短线交易
- 需要预埋条件单
- 独立测试 WebSocket 推送

普通用户请使用主程序：python3 crypto_sword.py --live
"""

# WebSocket 可选依赖
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    websocket = None

import json
import threading
import logging
import time
from typing import Dict, Any, Optional, List, Callable
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict
import subprocess

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 数据结构 - 赫尔墨斯的神靴
# ═══════════════════════════════════════════════════════════════

@dataclass
class PriceUpdate:
    """价格更新"""
    symbol: str
    price: float
    timestamp: float
    change_24h: float = 0.0


@dataclass
class ConditionOrder:
    """条件单"""
    id: str
    symbol: str
    side: str  # BUY/SELL
    direction: str  # LONG/SHORT
    trigger_price: float
    trigger_type: str  # ABOVE/BELOW
    order_type: str  # MARKET/LIMIT
    quantity: float
    leverage: int
    stop_loss: float
    take_profit: float
    
    # 状态
    is_active: bool = True
    triggered_at: Optional[float] = None
    order_id: Optional[int] = None
    
    def check_trigger(self, current_price: float) -> bool:
        """检查是否触发"""
        if not self.is_active:
            return False
        
        if self.trigger_type == "ABOVE":
            return current_price >= self.trigger_price
        else:  # BELOW
            return current_price <= self.trigger_price
    
    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side,
            "direction": self.direction,
            "trigger_price": self.trigger_price,
            "trigger_type": self.trigger_type,
            "order_type": self.order_type,
            "quantity": self.quantity,
            "leverage": self.leverage,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "is_active": self.is_active,
        }


# ═══════════════════════════════════════════════════════════════
# WebSocket 价格推送 - 波塞冬的海浪
# ═══════════════════════════════════════════════════════════════

class PriceWebSocket:
    """Binance WebSocket 价格推送"""
    
    def __init__(self, symbols: List[str], callback: Callable[[PriceUpdate], None]):
        self.symbols = [s.upper().replace("USDT", "") + "USDT" for s in symbols]
        self.callback = callback
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        
        #  streams
        self.streams = [f"{s.lower()}@trade" for s in self.symbols]
        self.ws_url = "wss://fstream.binance.com/ws/" + "+".join(self.streams)
        
        # 价格缓存
        self.prices: Dict[str, float] = {}
        self.last_update: Dict[str, float] = {}
    
    def on_message(self, ws, message):
        """处理消息"""
        try:
            data = json.loads(message)
            
            if "e" in data and data["e"] == "trade":
                symbol = data["s"]
                price = float(data["p"])
                timestamp = data["T"]
                
                self.prices[symbol] = price
                self.last_update[symbol] = time.time()
                
                # 回调
                update = PriceUpdate(
                    symbol=symbol,
                    price=price,
                    timestamp=timestamp / 1000,
                )
                self.callback(update)
                
        except Exception as e:
            logger.error(f"WebSocket 消息处理错误：{e}")
    
    def on_error(self, ws, error):
        logger.error(f"WebSocket 错误：{error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket 关闭：{close_status_code} - {close_msg}")
        if self.running:
            logger.info("5 秒后重连...")
            time.sleep(5)
            self.start()
    
    def on_open(self, ws):
        logger.info(f"WebSocket 已连接，订阅 {len(self.symbols)} 个币种")
    
    def start(self):
        """启动 WebSocket"""
        self.running = True
        
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        
        thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        thread.start()
        
        logger.info(f"WebSocket 线程已启动")
    
    def stop(self):
        """停止 WebSocket"""
        self.running = False
        if self.ws:
            self.ws.close()
    
    def get_price(self, symbol: str) -> Optional[float]:
        """获取最新价格"""
        return self.prices.get(symbol.upper())
    
    def get_all_prices(self) -> Dict[str, float]:
        """获取所有价格"""
        return self.prices.copy()


# ═══════════════════════════════════════════════════════════════
# 条件单管理器 - 雅典娜的谋略
# ═══════════════════════════════════════════════════════════════

class ConditionOrderManager:
    """条件单管理器"""
    
    def __init__(self, price_callback: Callable[[PriceUpdate], None]):
        self.orders: Dict[str, ConditionOrder] = {}
        self.price_callback = price_callback
        self.lock = threading.Lock()
        
        # 按符号索引
        self.orders_by_symbol: Dict[str, List[str]] = defaultdict(list)
    
    def add_order(self, order: ConditionOrder) -> str:
        """添加条件单"""
        with self.lock:
            self.orders[order.id] = order
            self.orders_by_symbol[order.symbol].append(order.id)
            
            logger.info(f"📋 条件单已添加：{order.id} - {order.symbol} {order.trigger_type} ${order.trigger_price}")
            
            return order.id
    
    def cancel_order(self, order_id: str) -> bool:
        """取消条件单"""
        with self.lock:
            if order_id in self.orders:
                order = self.orders[order_id]
                order.is_active = False
                
                if order.id in self.orders_by_symbol[order.symbol]:
                    self.orders_by_symbol[order.symbol].remove(order.id)
                
                del self.orders[order_id]
                logger.info(f"❌ 条件单已取消：{order_id}")
                return True
        return False
    
    def cancel_symbol_orders(self, symbol: str) -> int:
        """取消某币种的所有条件单"""
        with self.lock:
            order_ids = self.orders_by_symbol.get(symbol, []).copy()
            count = 0
            
            for order_id in order_ids:
                if order_id in self.orders:
                    self.orders[order_id].is_active = False
                    del self.orders[order_id]
                    count += 1
            
            self.orders_by_symbol[symbol] = []
            logger.info(f"❌ 取消 {symbol} 的 {count} 个条件单")
            
            return count
    
    def check_triggers(self, update: PriceUpdate):
        """检查价格触发"""
        triggered = []
        
        with self.lock:
            order_ids = self.orders_by_symbol.get(update.symbol, [])
            
            for order_id in order_ids:
                if order_id not in self.orders:
                    continue
                
                order = self.orders[order_id]
                
                if order.check_trigger(update.price):
                    triggered.append(order)
                    order.is_active = False
                    order.triggered_at = time.time()
        
        # 触发回调
        for order in triggered:
            logger.info(f"🎯 条件单触发：{order.id} - {order.symbol} @ ${update.price}")
            self.price_callback(order, update.price)
    
    def get_active_orders(self, symbol: str = None) -> List[ConditionOrder]:
        """获取活跃条件单"""
        with self.lock:
            if symbol:
                return [
                    self.orders[oid] 
                    for oid in self.orders_by_symbol.get(symbol, [])
                    if oid in self.orders
                ]
            else:
                return [o for o in self.orders.values() if o.is_active]
    
    def get_order_count(self) -> Dict[str, int]:
        """获取订单统计"""
        with self.lock:
            return {
                "total": len(self.orders),
                "by_symbol": {s: len(oids) for s, oids in self.orders_by_symbol.items() if oids},
            }


# ═══════════════════════════════════════════════════════════════
# 快速平仓通道 - 阿瑞斯的战斧
# ═══════════════════════════════════════════════════════════════

def run_binance_cli(args: List[str], timeout: int = 10) -> Optional[Any]:
    """运行 binance-cli"""
    try:
        cmd = ["binance-cli", "futures-usds"] + args
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        
        if result.returncode != 0:
            return None
        
        return json.loads(result.stdout)
        
    except Exception as e:
        logger.error(f"binance-cli 异常：{e}")
        return None


def quick_close_position(
    symbol: str,
    side: str,
    quantity: float,
    reason: str = "MANUAL",
) -> Dict[str, Any]:
    """
    快速平仓
    
    Args:
        symbol: 币种
        side: 平仓方向（与持仓相反）
        quantity: 数量
        reason: 平仓原因
    
    Returns:
        {
            "success": bool,
            "order_id": int,
            "executed_price": float,
            "pnl": float,
            "reason": str,
        }
    """
    start_time = time.time()
    
    # 市价平仓
    result = run_binance_cli([
        "new-order",
        "--symbol", symbol,
        "--side", side,
        "--type", "MARKET",
        "--quantity", str(quantity),
        "--reduce-only", "true",
    ])
    
    elapsed = time.time() - start_time
    
    if result and result.get("status") in ["FILLED", "NEW"]:
        return {
            "success": True,
            "order_id": result.get("orderId", 0),
            "executed_price": float(result.get("avgPrice", 0)),
            "quantity": float(result.get("executedQty", 0)),
            "elapsed_ms": round(elapsed * 1000, 2),
            "reason": reason,
        }
    else:
        return {
            "success": False,
            "error": result.get("msg", "Unknown error") if result else "Command failed",
            "elapsed_ms": round(elapsed * 1000, 2),
        }


def emergency_close_all(
    positions: List[Dict[str, Any]],
    reason: str = "EMERGENCY",
) -> List[Dict[str, Any]]:
    """
    紧急全平
    
    Args:
        positions: 持仓列表 [{"symbol": "BTCUSDT", "side": "LONG", "quantity": 0.001}, ...]
        reason: 平仓原因
    
    Returns:
        平仓结果列表
    """
    results = []
    
    # 并行平仓（多线程）
    threads = []
    
    def close_single(pos):
        side = "SELL" if pos["side"] == "LONG" else "BUY"
        result = quick_close_position(
            pos["symbol"],
            side,
            pos["quantity"],
            reason=reason,
        )
        result["symbol"] = pos["symbol"]
        results.append(result)
    
    for pos in positions:
        thread = threading.Thread(target=close_single, args=(pos,))
        threads.append(thread)
        thread.start()
    
    # 等待所有平仓完成
    for thread in threads:
        thread.join(timeout=5)
    
    logger.info(f"🚨 紧急平仓完成：{len(results)} 个持仓，耗时 {max(r.get('elapsed_ms', 0) for r in results):.0f}ms")
    
    return results


# ═══════════════════════════════════════════════════════════════
# 极速执行器 - 宙斯的雷霆
# ═══════════════════════════════════════════════════════════════

class SpeedExecutor:
    """极速执行器"""
    
    def __init__(
        self,
        symbols: List[str],
        on_order_triggered: Callable[[ConditionOrder, float], None],
    ):
        self.symbols = symbols
        self.on_order_triggered = on_order_triggered
        
        # WebSocket
        self.ws = PriceWebSocket(symbols, self._on_price_update)
        
        # 条件单管理
        self.order_manager = ConditionOrderManager(self._on_order_triggered)
        
        # 性能统计
        self.stats = {
            "price_updates": 0,
            "orders_triggered": 0,
            "avg_latency_ms": 0,
        }
    
    def _on_price_update(self, update: PriceUpdate):
        """价格更新回调"""
        self.stats["price_updates"] += 1
        
        # 检查条件单触发
        self.order_manager.check_triggers(update)
    
    def _on_order_triggered(self, order: ConditionOrder, price: float):
        """条件单触发回调"""
        self.stats["orders_triggered"] += 1
        
        # 通知上层执行
        self.on_order_triggered(order, price)
    
    def start(self):
        """启动执行器"""
        logger.info(f"⚡ 极速执行器启动，监控 {len(self.symbols)} 个币种")
        self.ws.start()
    
    def stop(self):
        """停止执行器"""
        logger.info(f"⚡ 极速执行器停止")
        self.ws.stop()
    
    def add_condition_order(
        self,
        symbol: str,
        side: str,
        direction: str,
        trigger_price: float,
        trigger_type: str,
        quantity: float,
        leverage: int = 5,
        stop_loss: float = 0,
        take_profit: float = 0,
        order_type: str = "MARKET",
    ) -> str:
        """
        添加条件单
        
        Returns:
            order_id: 条件单 ID
        """
        import uuid
        
        order = ConditionOrder(
            id=str(uuid.uuid4())[:8],
            symbol=symbol.upper(),
            side=side,
            direction=direction,
            trigger_price=trigger_price,
            trigger_type=trigger_type.upper(),
            order_type=order_type,
            quantity=quantity,
            leverage=leverage,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
        
        return self.order_manager.add_order(order)
    
    def cancel_condition_order(self, order_id: str) -> bool:
        """取消条件单"""
        return self.order_manager.cancel_order(order_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计"""
        return {
            **self.stats,
            "active_orders": self.order_manager.get_order_count(),
        }


# ═══════════════════════════════════════════════════════════════
# CLI 测试入口
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="⚡ 极速执行系统")
    parser.add_argument("--test-ws", action="store_true", help="测试 WebSocket")
    parser.add_argument("--symbols", type=str, default="BTCUSDT,ETHUSDT", help="币种列表")
    
    args = parser.parse_args()
    
    if args.test_ws:
        symbols = args.symbols.split(",")
        
        print("\n" + "═" * 70)
        print("⚡ 极速执行系统 - WebSocket 测试")
        print("═" * 70)
        
        def on_price(update: PriceUpdate):
            print(f"  {update.symbol}: ${update.price:.2f}")
        
        def on_order_triggered(order: ConditionOrder, price: float):
            print(f"🎯 触发：{order.id} - {order.symbol} @ ${price}")
        
        executor = SpeedExecutor(symbols, on_order_triggered)
        executor.ws.callback = on_price  # 临时覆盖用于测试
        
        print(f"\n订阅币种：{symbols}")
        print("按 Ctrl+C 停止...\n")
        
        executor.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            executor.stop()
            print("\n已停止")
    
    else:
        print("\n用法:")
        print("  speed-executor --test-ws                    # 测试 WebSocket")
        print("  speed-executor --test-ws --symbols BTC,ETH  # 指定币种")
        print("\n" + "═" * 70 + "\n")


if __name__ == "__main__":
    main()
