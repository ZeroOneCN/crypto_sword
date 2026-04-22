#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║         📈 BACKTEST ANALYZER - 神圣回测分析系统 📈            ║
║                                                               ║
║    分析历史交易，验证策略之神威                               ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import csv

DB_PATH = Path("/root/.hermes/logs/trade_log.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def analyze_performance(days: int = 30, mode: str = "testnet") -> Dict[str, Any]:
    """
    分析交易绩效 - 雅典娜的智慧
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 基础统计
    cursor.execute("""
        SELECT 
            COUNT(*) as total_trades,
            COUNT(CASE WHEN exit_price IS NOT NULL THEN 1 END) as closed_trades,
            COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) as winners,
            COALESCE(SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END), 0) as losers,
            COALESCE(SUM(pnl), 0) as total_pnl,
            COALESCE(AVG(pnl), 0) as avg_pnl,
            COALESCE(MAX(pnl), 0) as max_win,
            COALESCE(MIN(pnl), 0) as max_loss,
            COALESCE(AVG(pnl_pct), 0) as avg_pct
        FROM trades
        WHERE mode = ?
        AND exit_price IS NOT NULL
        AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
    """, (mode, days))
    
    row = cursor.fetchone()
    
    total = row['closed_trades']
    winners = row['winners']
    losers = row['losers']
    win_rate = (winners / total * 100) if total > 0 else 0
    
    # 盈亏比
    avg_win = 0
    avg_loss = 0
    
    cursor.execute("""
        SELECT AVG(pnl) as avg_win FROM trades 
        WHERE mode = ? AND pnl > 0 
        AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
    """, (mode, days))
    r = cursor.fetchone()
    avg_win = r['avg_win'] or 0
    
    cursor.execute("""
        SELECT AVG(ABS(pnl)) as avg_loss FROM trades 
        WHERE mode = ? AND pnl < 0 
        AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
    """, (mode, days))
    r = cursor.fetchone()
    avg_loss = r['avg_loss'] or 0
    
    profit_factor = (avg_win / avg_loss) if avg_loss > 0 else float('inf')
    
    # 按币种统计
    cursor.execute("""
        SELECT 
            symbol,
            COUNT(*) as trades,
            SUM(pnl) as total_pnl,
            AVG(pnl) as avg_pnl
        FROM trades
        WHERE mode = ? 
        AND exit_price IS NOT NULL
        AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
        GROUP BY symbol
        ORDER BY total_pnl DESC
        LIMIT 10
    """, (mode, days))
    
    by_symbol = [dict(r) for r in cursor.fetchall()]
    
    # 按平仓原因统计
    cursor.execute("""
        SELECT 
            exit_reason,
            COUNT(*) as count,
            AVG(pnl) as avg_pnl,
            SUM(pnl) as total_pnl
        FROM trades
        WHERE mode = ? 
        AND exit_price IS NOT NULL
        AND exit_reason IS NOT NULL
        AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
        GROUP BY exit_reason
    """, (mode, days))
    
    by_reason = [dict(r) for r in cursor.fetchall()]
    
    # 每日盈亏
    cursor.execute("""
        SELECT 
            DATE(exit_time) as date,
            COUNT(*) as trades,
            SUM(pnl) as daily_pnl
        FROM trades
        WHERE mode = ? 
        AND exit_price IS NOT NULL
        AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
        GROUP BY DATE(exit_time)
        ORDER BY date DESC
    """, (mode, days))
    
    daily_pnl = [dict(r) for r in cursor.fetchall()]
    
    conn.close()
    
    return {
        'period_days': days,
        'mode': mode,
        'total_trades': row['total_trades'],
        'closed_trades': total,
        'winners': winners,
        'losers': losers,
        'win_rate': round(win_rate, 2),
        'total_pnl': round(row['total_pnl'], 2),
        'avg_pnl': round(row['avg_pnl'], 2),
        'avg_pct': round(row['avg_pct'], 2),
        'max_win': round(row['max_win'], 2),
        'max_loss': round(row['max_loss'], 2),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else '∞',
        'by_symbol': by_symbol,
        'by_reason': by_reason,
        'daily_pnl': daily_pnl,
    }


def print_report(stats: Dict[str, Any]):
    """打印绩效报告"""
    print("\n" + "═" * 70)
    print("📈 神圣回测分析报告")
    print("═" * 70)
    print(f"统计周期：{stats['period_days']} 天 | 模式：{stats['mode']}")
    print("─" * 70)
    
    print("\n📊 基础统计")
    print(f"  总交易数：{stats['total_trades']}")
    print(f"  已平仓：{stats['closed_trades']}")
    print(f"  盈利：{stats['winners']} | 亏损：{stats['losers']}")
    print(f"  胜率：{stats['win_rate']}%")
    
    print("\n💰 盈亏统计")
    print(f"  总盈亏：${stats['total_pnl']:.2f}")
    print(f"  平均盈亏：${stats['avg_pnl']:.2f} ({stats['avg_pct']:.2f}%)")
    print(f"  最大盈利：${stats['max_win']:.2f}")
    print(f"  最大亏损：${stats['max_loss']:.2f}")
    print(f"  平均盈利：${stats['avg_win']:.2f}")
    print(f"  平均亏损：${stats['avg_loss']:.2f}")
    print(f"  盈亏比：{stats['profit_factor']}")
    
    print("\n🎯 按币种统计 (Top 10)")
    print(f"  {'Symbol':<15} {'Trades':<8} {'Total PnL':<12} {'Avg PnL':<10}")
    print("  " + "─" * 45)
    for s in stats['by_symbol'][:10]:
        print(f"  {s['symbol']:<15} {s['trades']:<8} ${s['total_pnl']:<11.2f} ${s['avg_pnl']:<9.2f}")
    
    print("\n📋 按平仓原因统计")
    print(f"  {'Reason':<15} {'Count':<8} {'Avg PnL':<12} {'Total PnL':<12}")
    print("  " + "─" * 47)
    for r in stats['by_reason']:
        reason = r['exit_reason'] or 'Unknown'
        print(f"  {reason:<15} {r['count']:<8} ${r['avg_pnl']:<11.2f} ${r['total_pnl']:<11.2f}")
    
    print("\n📅 每日盈亏 (最近 10 天)")
    print(f"  {'Date':<12} {'Trades':<8} {'PnL':<12}")
    print("  " + "─" * 32)
    for d in stats['daily_pnl'][:10]:
        color = "🟢" if d['daily_pnl'] > 0 else "🔴" if d['daily_pnl'] < 0 else "⚪"
        print(f"  {d['date']:<12} {d['trades']:<8} {color} ${d['daily_pnl']:.2f}")
    
    print("\n" + "═" * 70 + "\n")


def export_report(output_path: str, days: int = 30, mode: str = "testnet"):
    """导出完整报告到 CSV"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM trades
        WHERE mode = ?
        AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
        ORDER BY exit_time DESC
    """, (mode, days))
    
    rows = cursor.fetchall()
    conn.close()
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'ID', 'Symbol', 'Side', 'Stage', 'Entry Price', 'Exit Price',
            'Quantity', 'Leverage', 'PnL', 'PnL %', 'Exit Reason',
            'Entry Time', 'Exit Time', 'Mode'
        ])
        
        for row in rows:
            writer.writerow([
                row['id'], row['symbol'], row['side'], row['stage'],
                row['entry_price'], row['exit_price'], row['quantity'],
                row['leverage'], row['pnl'], row['pnl_pct'], row['exit_reason'],
                row['entry_time'], row['exit_time'], row['mode']
            ])
    
    print(f"✅ 报告已导出：{output_path}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="📈 神圣回测分析系统")
    parser.add_argument("--analyze", action="store_true", help="执行绩效分析")
    parser.add_argument("--export", type=str, help="导出报告到 CSV")
    parser.add_argument("--days", type=int, default=30, help="统计天数 (默认：30)")
    parser.add_argument("--mode", type=str, default="testnet", choices=["testnet", "live", "dry_run"], help="交易模式")
    
    args = parser.parse_args()
    
    if args.analyze or not args.export:
        stats = analyze_performance(days=args.days, mode=args.mode)
        print_report(stats)
    
    if args.export:
        export_report(args.export, days=args.days, mode=args.mode)


if __name__ == "__main__":
    main()
