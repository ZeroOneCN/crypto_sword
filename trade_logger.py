#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║         📜 TRADE LOGGER - 神圣交易日志系统 📜                 ║
║                                                               ║
║    记录每一笔交易的荣耀与耻辱，为回测提供神圣的数据基石       ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict

DB_PATH = Path("/root/.hermes/logs/trade_log.db")


# ═══════════════════════════════════════════════════════════════
# 数据模型 - 雅典娜的圣典
# ═══════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    """交易记录 - 每一笔神圣的交易"""
    
    id: Optional[int] = None
    symbol: str = ""
    side: str = ""  # LONG/SHORT
    direction: str = ""  # BUY/SELL
    stage: str = ""  # pre_break/confirmed/etc
    entry_price: float = 0.0
    quantity: float = 0.0
    leverage: int = 1
    stop_loss: float = 0.0
    take_profit: float = 0.0
    entry_time: str = ""
    
    # 平仓信息
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    exit_reason: Optional[str] = None  # STOP_LOSS/TAKE_PROFIT/TRAILING/MANUAL
    pnl: float = 0.0
    pnl_pct: float = 0.0
    realized_pnl: float = 0.0
    
    # 市场数据快照
    market_snapshot: Dict[str, Any] = None
    
    # 元数据
    mode: str = "live"  # live
    notes: str = ""
    
    def __post_init__(self):
        if self.market_snapshot is None:
            self.market_snapshot = {}
        if not self.entry_time:
            self.entry_time = datetime.now().isoformat()


# ═══════════════════════════════════════════════════════════════
# 数据库管理 - 波塞冬的海底宝库
# ═══════════════════════════════════════════════════════════════

class TradeDatabase:
    """交易数据库 - 神圣的记忆宫殿"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_db(self):
        """初始化数据库表结构"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 交易记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                direction TEXT NOT NULL,
                stage TEXT NOT NULL,
                entry_price REAL NOT NULL,
                quantity REAL NOT NULL,
                leverage INTEGER NOT NULL DEFAULT 1,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                entry_time TEXT NOT NULL,
                exit_price REAL,
                exit_time TEXT,
                exit_reason TEXT,
                pnl REAL DEFAULT 0.0,
                pnl_pct REAL DEFAULT 0.0,
                realized_pnl REAL DEFAULT 0.0,
                market_snapshot TEXT,
                mode TEXT NOT NULL DEFAULT 'live',
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 索引 - 加速查询
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entry_time ON trades(entry_time)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_exit_reason ON trades(exit_reason)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_mode ON trades(mode)
        """)
        
        conn.commit()
        conn.close()
    
    def add_trade(self, trade: TradeRecord) -> int:
        """添加新交易记录"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO trades (
                symbol, side, direction, stage, entry_price, quantity,
                leverage, stop_loss, take_profit, entry_time,
                exit_price, exit_time, exit_reason, pnl, pnl_pct,
                realized_pnl, market_snapshot, mode, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade.symbol, trade.side, trade.direction, trade.stage,
            trade.entry_price, trade.quantity, trade.leverage,
            trade.stop_loss, trade.take_profit, trade.entry_time,
            trade.exit_price, trade.exit_time, trade.exit_reason,
            trade.pnl, trade.pnl_pct, trade.realized_pnl,
            json.dumps(trade.market_snapshot), trade.mode, trade.notes
        ))
        
        trade_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return trade_id
    
    def update_exit(self, trade_id: int, exit_price: float, exit_reason: str, 
                    pnl: float, pnl_pct: float, realized_pnl: float = None):
        """更新平仓信息"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE trades SET
                exit_price = ?,
                exit_time = ?,
                exit_reason = ?,
                pnl = ?,
                pnl_pct = ?,
                realized_pnl = COALESCE(?, ?)
            WHERE id = ?
        """, (
            exit_price, datetime.now().isoformat(), exit_reason,
            pnl, pnl_pct, realized_pnl, pnl, trade_id
        ))
        
        conn.commit()
        conn.close()
    
    def get_trade(self, trade_id: int) -> Optional[TradeRecord]:
        """获取单笔交易"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return self._row_to_trade(row)
        return None
    
    def get_open_trades(self, mode: str = None) -> List[TradeRecord]:
        """获取未平仓交易"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if mode:
            cursor.execute(
                "SELECT * FROM trades WHERE exit_price IS NULL AND mode = ? ORDER BY entry_time DESC",
                (mode,)
            )
        else:
            cursor.execute(
                "SELECT * FROM trades WHERE exit_price IS NULL ORDER BY entry_time DESC"
            )
        
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_trade(row) for row in rows]
    
    def get_closed_trades(self, days: int = 7, mode: str = None) -> List[TradeRecord]:
        """获取已平仓交易（最近 N 天）"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if mode:
            cursor.execute("""
                SELECT * FROM trades 
                WHERE exit_price IS NOT NULL 
                AND mode = ?
                AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
                ORDER BY exit_time DESC
            """, (mode, days))
        else:
            cursor.execute("""
                SELECT * FROM trades 
                WHERE exit_price IS NOT NULL 
                AND datetime(exit_time) >= datetime('now', '-' || ? || ' days')
                ORDER BY exit_time DESC
            """, (days,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_trade(row) for row in rows]
    
    def get_all_trades(self, limit: int = 100) -> List[TradeRecord]:
        """获取所有交易（限制数量）"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT * FROM trades ORDER BY entry_time DESC LIMIT ?",
            (limit,)
        )
        
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_trade(row) for row in rows]
    
    def get_statistics(self, days: int = 7, mode: str = None) -> Dict[str, Any]:
        """获取交易统计"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 基础统计
        if mode:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    COUNT(CASE WHEN exit_price IS NOT NULL THEN 1 END) as closed_trades,
                    COUNT(CASE WHEN exit_price IS NULL THEN 1 END) as open_trades,
                    COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) as winning_trades,
                    COALESCE(SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END), 0) as losing_trades,
                    COALESCE(SUM(pnl), 0) as total_pnl,
                    COALESCE(AVG(pnl), 0) as avg_pnl,
                    COALESCE(MAX(pnl), 0) as max_pnl,
                    COALESCE(MIN(pnl), 0) as min_pnl
                FROM trades
                WHERE mode = ?
                AND (exit_time IS NULL OR datetime(exit_time) >= datetime('now', '-' || ? || ' days'))
            """, (mode, days))
        else:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    COUNT(CASE WHEN exit_price IS NOT NULL THEN 1 END) as closed_trades,
                    COUNT(CASE WHEN exit_price IS NULL THEN 1 END) as open_trades,
                    COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) as winning_trades,
                    COALESCE(SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END), 0) as losing_trades,
                    COALESCE(SUM(pnl), 0) as total_pnl,
                    COALESCE(AVG(pnl), 0) as avg_pnl,
                    COALESCE(MAX(pnl), 0) as max_pnl,
                    COALESCE(MIN(pnl), 0) as min_pnl
                FROM trades
                WHERE (exit_time IS NULL OR datetime(exit_time) >= datetime('now', '-' || ? || ' days'))
            """, (days,))
        
        row = cursor.fetchone()
        
        # 胜率
        total_closed = row['closed_trades']
        winning = row['winning_trades']
        win_rate = (winning / total_closed * 100) if total_closed > 0 else 0
        
        stats = {
            'period_days': days,
            'mode': mode or 'all',
            'total_trades': row['total_trades'],
            'closed_trades': total_closed,
            'open_trades': row['open_trades'],
            'winning_trades': winning,
            'losing_trades': row['losing_trades'],
            'win_rate': round(win_rate, 2),
            'total_pnl': round(row['total_pnl'], 2),
            'avg_pnl': round(row['avg_pnl'], 2),
            'max_pnl': round(row['max_pnl'], 2),
            'min_pnl': round(row['min_pnl'], 2),
        }
        
        conn.close()
        return stats
    
    def export_to_csv(self, output_path: Path, days: int = 30):
        """导出交易记录到 CSV"""
        import csv
        
        trades = self.get_closed_trades(days=days)
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'ID', 'Symbol', 'Side', 'Entry Price', 'Exit Price',
                'Quantity', 'PnL', 'PnL %', 'Exit Reason', 'Entry Time', 'Exit Time'
            ])
            
            for trade in trades:
                writer.writerow([
                    trade.id, trade.symbol, trade.side, trade.entry_price,
                    trade.exit_price, trade.quantity, trade.pnl, trade.pnl_pct,
                    trade.exit_reason, trade.entry_time, trade.exit_time
                ])
    
    def _row_to_trade(self, row: sqlite3.Row) -> TradeRecord:
        """将数据库行转换为 TradeRecord"""
        return TradeRecord(
            id=row['id'],
            symbol=row['symbol'],
            side=row['side'],
            direction=row['direction'],
            stage=row['stage'],
            entry_price=row['entry_price'],
            quantity=row['quantity'],
            leverage=row['leverage'],
            stop_loss=row['stop_loss'],
            take_profit=row['take_profit'],
            entry_time=row['entry_time'],
            exit_price=row['exit_price'],
            exit_time=row['exit_time'],
            exit_reason=row['exit_reason'],
            pnl=row['pnl'],
            pnl_pct=row['pnl_pct'],
            realized_pnl=row['realized_pnl'],
            market_snapshot=json.loads(row['market_snapshot']) if row['market_snapshot'] else {},
            mode=row['mode'],
            notes=row['notes'] or ""
        )


# ═══════════════════════════════════════════════════════════════
# CLI 工具 - 赫尔墨斯的信使
# ═══════════════════════════════════════════════════════════════

def print_statistics(db: TradeDatabase, days: int = 7, mode: str = None):
    """打印交易统计"""
    stats = db.get_statistics(days=days, mode=mode)
    
    print("\n" + "═" * 60)
    print("📊 交易统计报告")
    print("═" * 60)
    print(f"统计周期：{stats['period_days']} 天 | 模式：{stats['mode']}")
    print("─" * 60)
    print(f"总交易数：{stats['total_trades']}")
    print(f"已平仓：{stats['closed_trades']} | 未平仓：{stats['open_trades']}")
    print(f"盈利：{stats['winning_trades']} | 亏损：{stats['losing_trades']}")
    print(f"胜率：{stats['win_rate']}%")
    print("─" * 60)
    print(f"总盈亏：${stats['total_pnl']:.2f}")
    print(f"平均盈亏：${stats['avg_pnl']:.2f}")
    print(f"最大盈利：${stats['max_pnl']:.2f}")
    print(f"最大亏损：${stats['min_pnl']:.2f}")
    print("═" * 60 + "\n")


def print_recent_trades(db: TradeDatabase, limit: int = 10):
    """打印最近交易"""
    trades = db.get_all_trades(limit=limit)
    
    if not trades:
        print("\n📭 暂无交易记录\n")
        return
    
    print("\n" + "═" * 80)
    print("📜 最近交易记录")
    print("═" * 80)
    print(f"{'ID':<5} {'Symbol':<12} {'Side':<6} {'Entry':<10} {'Exit':<10} {'PnL':<10} {'Reason':<12} {'Time':<20}")
    print("─" * 80)
    
    for t in trades:
        exit_price = f"${t.exit_price:.2f}" if t.exit_price else "-"
        pnl = f"${t.pnl:.2f}" if t.exit_price else "-"
        reason = t.exit_reason or "-"
        time_str = t.entry_time[:16].replace('T', ' ')
        
        print(f"{t.id:<5} {t.symbol:<12} {t.side:<6} ${t.entry_price:<9.2f} {exit_price:<10} {pnl:<10} {reason:<12} {time_str:<20}")
    
    print("═" * 80 + "\n")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="📜 神圣交易日志系统")
    parser.add_argument("--stats", action="store_true", help="显示交易统计")
    parser.add_argument("--recent", action="store_true", help="显示最近交易")
    parser.add_argument("--export", type=str, help="导出 CSV 到指定路径")
    parser.add_argument("--days", type=int, default=7, help="统计天数 (默认：7)")
    parser.add_argument("--mode", type=str, choices=["testnet", "live", "dry_run"], help="交易模式过滤")
    parser.add_argument("--limit", type=int, default=10, help="显示交易数量 (默认：10)")
    
    args = parser.parse_args()
    
    db = TradeDatabase()
    
    if args.stats:
        print_statistics(db, days=args.days, mode=args.mode)
    elif args.export:
        db.export_to_csv(Path(args.export), days=args.days)
        print(f"✅ 已导出到：{args.export}")
    else:
        print_recent_trades(db, limit=args.limit)
        print_statistics(db, days=args.days, mode=args.mode)


if __name__ == "__main__":
    main()
