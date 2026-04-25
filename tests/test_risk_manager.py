#!/usr/bin/env python3
"""
风控模块单元测试

测试覆盖：
- 仓位计算（正常/边界/异常）
- 止损距离校验（除零风险）
- 名义价值限制
- 杠杆与保证金计算
"""

import sys
import os
import unittest
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from risk_manager import calculate_position_size


class TestPositionSizeCalculation(unittest.TestCase):
    """测试仓位计算逻辑"""

    def setUp(self):
        """测试用例前置条件"""
        self.account_balance = 10000.0
        self.risk_per_trade_pct = 2.0
        self.entry_price = 50000.0
        self.stop_loss_price = 46000.0  # 8% 止损
        self.max_position_pct = 20.0
        self.min_notional = 5.0

    def test_normal_position_calculation(self):
        """测试正常仓位计算"""
        result = calculate_position_size(
            account_balance=self.account_balance,
            risk_per_trade_pct=self.risk_per_trade_pct,
            entry_price=self.entry_price,
            stop_loss_price=self.stop_loss_price,
            max_position_pct=self.max_position_pct,
            min_notional=self.min_notional,
        )
        
        self.assertGreater(result["quantity"], 0)
        self.assertGreater(result["position_value"], 0)
        self.assertEqual(result.get("error"), None)
        
        # 验证风险金额计算正确
        expected_risk = self.account_balance * (self.risk_per_trade_pct / 100)
        self.assertAlmostEqual(result["risk_amount"], expected_risk, places=2)

    def test_very_small_stop_distance(self):
        """测试极小止损距离（<0.01%）应返回错误"""
        # 止损距离仅为 0.005%
        result = calculate_position_size(
            account_balance=self.account_balance,
            risk_per_trade_pct=self.risk_per_trade_pct,
            entry_price=self.entry_price,
            stop_loss_price=self.entry_price * 0.99995,  # 极小距离
            max_position_pct=self.max_position_pct,
            min_notional=self.min_notional,
        )
        
        self.assertEqual(result["quantity"], 0)
        self.assertIn("止损距离过小", result.get("error", ""))

    def test_zero_stop_distance(self):
        """测试止损距离为 0 应返回错误"""
        result = calculate_position_size(
            account_balance=self.account_balance,
            risk_per_trade_pct=self.risk_per_trade_pct,
            entry_price=self.entry_price,
            stop_loss_price=self.entry_price,  # 相同价格
            max_position_pct=self.max_position_pct,
            min_notional=self.min_notional,
        )
        
        self.assertEqual(result["quantity"], 0)
        self.assertIn("止损距离过小", result.get("error", ""))

    def test_max_position_cap(self):
        """测试最大仓位限制"""
        # 使用极小止损距离，计算出的仓位会远超限制
        result = calculate_position_size(
            account_balance=self.account_balance,
            risk_per_trade_pct=self.risk_per_trade_pct,
            entry_price=self.entry_price,
            stop_loss_price=self.entry_price * 0.99,  # 1% 止损
            max_position_pct=5.0,  # 限制最大仓位 5%
            min_notional=self.min_notional,
        )
        
        max_position_value = self.account_balance * 0.05
        self.assertLessEqual(result["position_value"], max_position_value + 0.01)
        self.assertTrue(result.get("is_capped", False))

    def test_min_notional_enforcement(self):
        """测试最小名义价值限制"""
        # 使用极小风险比例，计算出的仓位可能低于最小名义价值
        result = calculate_position_size(
            account_balance=100.0,  # 小账户
            risk_per_trade_pct=0.1,  # 极小风险
            entry_price=1000.0,
            stop_loss_price=990.0,  # 1% 止损
            max_position_pct=20.0,
            min_notional=5.0,
        )
        
        # 仓位价值应至少为 5 USDT
        self.assertGreaterEqual(result["position_value"], 5.0)


class TestTTLCache(unittest.TestCase):
    """测试 TTLCache 缓存机制"""

    def test_cache_ttl_expiration(self):
        """测试缓存 TTL 过期"""
        import time
        from signal_enhancer import TTLCache
        
        cache = TTLCache(ttl_sec=0.5, max_size=10)
        cache.set("key1", "value1")
        
        # 未过期时应命中
        self.assertEqual(cache.get("key1"), "value1")
        
        # 等待过期
        time.sleep(0.6)
        self.assertIsNone(cache.get("key1"))

    def test_cache_lru_eviction(self):
        """测试 LRU 淘汰机制"""
        from signal_enhancer import TTLCache
        
        cache = TTLCache(ttl_sec=60, max_size=3)
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        
        # 超过 max_size 应淘汰最旧的
        cache.set("key4", "value4")
        self.assertIsNone(cache.get("key1"))
        self.assertEqual(cache.get("key4"), "value4")


if __name__ == "__main__":
    unittest.main()
