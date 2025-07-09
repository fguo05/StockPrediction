import json
from datetime import datetime

import pandas as pd
import pymysql
from pymysql import Error
from pymysql.constants import CLIENT


# 配置数据库连接
config = {
    'host': 'rm-uf6q5h4a7tkthf82cno.mysql.rds.aliyuncs.com',  # 公网地址
    'port': 3306,                             # 端口
    'user': 'db_user4',                       # 数据库账号
    'password': 'Cangjie!user4',              # 数据库密码
    'database': 'db_test1',                   # 数据库名
    'charset': 'utf8mb4',                     # 字符编码
    'client_flag': CLIENT.MULTI_STATEMENTS,   # 允许执行多条SQL语句
    'cursorclass': pymysql.cursors.DictCursor # 返回字典格式的结果
}


def create_db_connection():
    try:
        connection = pymysql.connect(**config)
        print("成功连接到阿里云RDS数据库！")
        return connection
    except Error as e:
        print(f"连接数据库失败: {e}")
        return None


def parse_date(date_str):
    formats = [
        "%b %d, %Y, %H:%M",  # Oct 10, 2022, 12:00
        "%Y-%m-%d %H:%M:%S",  # 2022-10-10 12:00:00
        "%m/%d/%Y %H:%M"  # 10/10/2022 12:00
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str}")


def db_log(msg):
    with open("logs/db/transactions.log", "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {msg}\n")


def insert_backtesting_data(connection, excel_data):
    try:
        # 获取Performance
        performance_data = {
            "open_pl": excel_data['Performance']['B'][1],  # 第二行B列
            "net_profit": excel_data['Performance']['B'][2],
            "gross_profit": excel_data['Performance']['B'][3],
            "gross_loss": excel_data['Performance']['B'][4],
            "commission_paid": excel_data['Performance']['B'][5],
            "buy_hold_return": excel_data['Performance']['B'][6],
            "max_equity_runup": excel_data['Performance']['B'][7],
            "max_equity_drawdown": excel_data['Performance']['B'][8],
            "max_contracts_held": excel_data['Performance']['B'][9]
        }

        # 获取交易分析数据
        trades_analysis_data = {
            "total_trades": excel_data['Trades analysis']['B'][1],
            "total_open_trades": excel_data['Trades analysis']['B'][2],
            "winning_trades": excel_data['Trades analysis']['B'][3],
            "losing_trades": excel_data['Trades analysis']['B'][4],
            "percent_profitable": excel_data['Trades analysis']['B'][5],
            "avg_pnl": excel_data['Trades analysis']['B'][6],
            "avg_winning_trade": excel_data['Trades analysis']['B'][7],
            "avg_losing_trade": excel_data['Trades analysis']['B'][8],
            "ratio_avg_win_avg_loss": excel_data['Trades analysis']['B'][9],
            "largest_winning_trade": excel_data['Trades analysis']['B'][10],
            "largest_losing_trade": excel_data['Trades analysis']['B'][12],
            "avg_bars_in_trades": excel_data['Trades analysis']['B'][14],
            "avg_bars_in_winning_trades": excel_data['Trades analysis']['B'][15],
            "avg_bars_in_losing_trades": excel_data['Trades analysis']['B'][16]
        }

        # 获取风险指标数据
        risk_performance_data = {
            "sharpe_ratio": excel_data['Risk performance ratios']['B'][1],
            "sortino_ratio": excel_data['Risk performance ratios']['B'][2],
            "profit_factor": excel_data['Risk performance ratios']['B'][3],
            "margin_calls": excel_data['Risk performance ratios']['B'][4]
        }

        # 获取属性数据（修正字典访问方式）
        props = {excel_data['Properties']['A'][i]: excel_data['Properties']['B'][i]
                 for i in range(len(excel_data['Properties']['A']))}

        # 解析日期范围
        trading_range = props["Trading range"].split("—")
        trading_range_start = parse_date(trading_range[0])
        trading_range_end = parse_date(trading_range[1])

        backtesting_range = props["Backtesting range"].split("—")
        backtesting_range_start = parse_date(backtesting_range[0])
        backtesting_range_end = parse_date(backtesting_range[1])

        start_date = parse_date(props["Start Date"])

        with connection.cursor() as cursor:
            # 获取ticker_id
            symbol = props["Symbol"]
            exchangeticker_id = "" # 根据symbol得到exchangeticker_id！！！！！！！！！！！！！
            cursor.execute("SELECT id FROM ExchangeTicker WHERE ticker_id = %s", (exchangeticker_id,))
            result = cursor.fetchone()
            if not result:
                raise ValueError(f"Symbol {symbol} not found in database")
            ticker_id = result['id']

            # 插入回测数据
            insert_query = """
            INSERT INTO BackTesting (
                ticker_id, performance, trades_analysis, risk_performance_ratios, strategy,
                trading_range_start, trading_range_end, backtesting_range_start, backtesting_range_end,
                symbol, timeframe, point_value, chart_type, currency, tick_size, precision_setting,
                start_date, initial_capital, order_size, pyramiding, commission, slippage,
                verify_price_ticks, long_margin, short_margin, recalculate_after_order,
                recalculate_every_tick, recalculate_on_bar_close, use_bar_magnifier
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            backtesting_data = (
                ticker_id,
                json.dumps(performance_data),
                json.dumps(trades_analysis_data),
                json.dumps(risk_performance_data),
                'Custom_Signal', # 策略暂定！！！！！！！！！！！！！！！！！
                trading_range_start,
                trading_range_end,
                backtesting_range_start,
                backtesting_range_end,
                symbol,
                props["Timeframe"],
                float(props["Point value"]),
                props["Chart type"],
                props["Currency"],
                float(props["Tick size"]),
                props["Precision"],
                start_date,
                float(props["Initial capital"]),
                int(props["Order size"]),
                int(props["Pyramiding"]),
                float(str(props["Commission"]).rstrip('%')),
                int(props["Slippage"]),
                int(props["Verify price for limit orders"]),
                float(str(props["Margin for long positions"]).rstrip('%')),
                float(str(props["Margin for short positions"]).rstrip('%')),
                'On' if props["Recalculate after order is filled"] == 'On' else 'Off',
                'On' if props["Recalculate on every tick"] == 'On' else 'Off',
                'On' if props["Recalculate on bar close"] == 'On' else 'Off',
                'On' if props["Backtesting precision. Use bar magnifier"] == 'On' else 'Off'
            )

            cursor.execute(insert_query, backtesting_data)
            backtesting_id = cursor.lastrowid
            connection.commit()

            return backtesting_id

    except Error as e:
        print(f"Error inserting backtesting data: {e}")
        connection.rollback()
        return None


def insert_trade_data(connection, backtesting_id, excel_data):
    try:
        trades = excel_data['List of trades']
        trade_entries = []

        # 预处理：将字典结构转换为更易处理的列表结构
        trade_records = []
        for i in range(len(trades['A'])):
            record = {col: trades[col][i] for col in trades.keys()}
            trade_records.append(record)

        with connection.cursor() as cursor:
            insert_query = """
                    INSERT INTO Trade (
                        backtesting_id, trade_id, trade_type, signal_type, exec_time, exec_price, quantity,
                        pnl_absolute, pnl_percent, runup_absolute, runup_percent, drawdown_absolute, drawdown_percent
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

            batch_data = []
            for record in trade_records:
                try:
                    # 跳过未关闭的交易
                    if str(record['Type']).endswith('Open'):
                        continue

                    # 解析交易类型
                    trade_type = 'long' if 'long' in str(record['Type']).lower() else 'short'

                    # 处理可能的空值
                    def safe_float(val):
                        try:
                            return float(val) if val not in [None, '', 'NA'] else None
                        except:
                            return None

                    trade_data = (
                        backtesting_id,
                        int(record['Trade #']),
                        trade_type,
                        str(record['Signal']),
                        datetime.strptime(str(record['Date/Time']), "%Y-%m-%d %H:%M:%S"),
                        safe_float(record['Price USDT']),
                        int(record['Quantity']),
                        safe_float(record['P&L USDT']),
                        safe_float(record['P&L %']),
                        safe_float(record['Run-up USDT']),
                        safe_float(record['Run-up %']),
                        safe_float(record['Drawdown USDT']),
                        safe_float(record['Drawdown %'])
                    )
                    batch_data.append(trade_data)
                except Exception as e:
                    print(f"Skipping malformed trade record {record.get('Trade #', 'unknown')}: {str(e)}")
                    continue

            # 批量执行插入
            if batch_data:
                cursor.executemany(insert_query, batch_data)
                connection.commit()

            return len(batch_data) > 0  # 返回是否插入了有效数据

    except Error as e:
        print(f"Error inserting trade data: {e}")
        connection.rollback()
        return False


def parse_excel(path):
    excel_data = {}
    with pd.ExcelFile(path) as xls:
        for sheet_name in xls.sheet_names:
            # Read each sheet and convert to dictionary
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            # Convert to dictionary where keys are column letters and values are lists
            excel_data[sheet_name] = {
                chr(65 + i): df.iloc[:, i].tolist() for i in range(df.shape[1])
            }
    return excel_data


def insert_backtesting_to_db(excel_path):
    """
    将回测数据写入数据库
    :param excel_path: 回测文件路径
    """
    # 连接数据库
    connection = create_db_connection()
    if not connection:
        return

    excel_data = parse_excel(excel_path)

    try:
        # 写入backtesting表（引用ticker_id）
        backtesting_id = insert_backtesting_data(connection, excel_data)
        if not backtesting_id:
            print("Failed to insert backtesting data")
            db_log(f"Backtesting data insertion failed for file: {excel_path}")
            return

        # 写入trade表
        success = insert_trade_data(connection, backtesting_id, excel_data)
        if not success:
            print("Failed to insert trade data")
            db_log(f"Trade insertion failed for backtesting ID: {backtesting_id}, file: {excel_path}")
            return

        print("回测数据写入成功！")

    finally:
        connection.close()


if __name__ == "__main__":
    excel_path = "Custom_Signal_Strategy_OKX_BTCUSDT_2025-07-01.xlsx"
    insert_backtesting_to_db(excel_path)
