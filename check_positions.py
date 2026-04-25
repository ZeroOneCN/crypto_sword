#!/usr/bin/env python3
import sys
sys.path.insert(0, '/root/.hermes/scripts')

from binance_api_client import BinanceApiClient

client = BinanceApiClient.from_environment()

# 获取账户信息
account = client.account_information()
print('=' * 60)
print('💰 账户信息')
print('=' * 60)

wallet_balance = float(account.get('totalWalletBalance', 0))
unrealized_pnl = float(account.get('totalUnrealizedProfit', 0))
margin_balance = float(account.get('totalMarginBalance', 0))

print(f'钱包余额：{wallet_balance:.2f} USDT')
print(f'未实现盈亏：{unrealized_pnl:.4f} USDT')
print(f'保证金余额：{margin_balance:.2f} USDT')

# 获取持仓
print('\n' + '=' * 60)
print('📊 持仓详情')
print('=' * 60)

positions = client.position_risk()
open_positions = [p for p in positions if float(p.get('positionAmt', 0)) != 0]

print(f'\n持仓数量：{len(open_positions)}/3\n')

for i, pos in enumerate(open_positions, 1):
    symbol = pos.get('symbol', '')
    position_amt = float(pos.get('positionAmt', 0))
    entry_price = float(pos.get('entryPrice', 0))
    mark_price = float(pos.get('markPrice', 0))
    unrealized_pnl = float(pos.get('unrealizedProfit', 0))
    leverage = pos.get('leverage', '')
    liquidation_price = pos.get('liquidationPrice', '')
    position_value = abs(position_amt * mark_price)
    
    direction = '🟢 做多' if position_amt > 0 else '🔴 做空'
    pnl_pct = (unrealized_pnl / position_value * 100) if position_value > 0 else 0
    
    print(f"--- 持仓 {i}: {symbol} {direction} ---")
    print(f'  数量：{abs(position_amt)}')
    print(f'  开仓价：{entry_price}')
    print(f'  标记价：{mark_price}')
    print(f'  仓位价值：{position_value:.2f} USDT')
    print(f'  未实现盈亏：{unrealized_pnl:.4f} USDT ({pnl_pct:.2f}%)')
    print(f'  杠杆：{leverage}x')
    print(f'  强平价：{liquidation_price}')
    print()

# 获取挂单
print('=' * 60)
print('📋 当前挂单')
print('=' * 60)

open_orders = client.open_orders()
active_orders = [o for o in open_orders if o.get('status') == 'NEW']

if active_orders:
    print(f'\n挂单数量：{len(active_orders)}\n')
    for order in active_orders:
        symbol = order.get('symbol', '')
        side = order.get('side', '')
        price = order.get('price', '')
        quantity = order.get('origQty', '')
        order_type = order.get('type', '')
        
        print(f"  {symbol} | {side} | {order_type} | 价格：{price} | 数量：{quantity}")
    print()
else:
    print('\n无挂单\n')
