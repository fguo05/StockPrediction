"""
将回测excel数据批量写入数据库

改动：
- insert_backtesting_data和insert_trade_data的断开连接异常不再尝试重新连接。因为如果是trade时断开连接，会连同backtesting一起回滚（？），过于复杂，简化处理
- create_db_connection、insert_backtesting_data、insert_trade_data、insert_backtesting_to_db是否应该抛出异常给外部函数？遇到异常就抛出？如果内部只抛出Exception，在外部详细处理各种异常，rollback在哪写？
    考虑：不raise异常，各处理各的。如果raise异常在外层统一处理，不好判断是backtesting还是trade出现异常
         但好像可以通过内部Except块打印/写日志区分具体哪里出现异常

待处理：
- insert_backtesting_data中根据文件名确定strategy
- insert_backtesting_data中根据symbol得到exchangeticker_id
- 日志
- trade_type
"""
import json
from decimal import Decimal, getcontext
from utils import *


def insert_backtesting_data(connection, excel_data):
    """
    将excel数据写入backtesting表，返回backtesting_id
    :param connection: 数据库连接实例
    :param excel_data: dict
    :return: backtesting_id
    """
    # 1. 从excel获取数据
    # 辅助函数：安全转换字符串
    def safe_str(value):
        if isinstance(value, (int, float)):
            return str(value)
        return str(value).strip()

    # 辅助函数：构建带列标签的数据字典
    def build_sheet_data(sheet_data):
        data = {}

        # 获取A列的所有行标签
        row_labels = [cell for cell in sheet_data['A'][1:]]

        for row, label in enumerate(row_labels, 1):
            row_data = {}

            # 添加所有列的数据
            columns = {
                'B': 'All_USDT',
                'C': 'All_percent',
                'D': 'Long_USDT',
                'E': 'Long_percent',
                'F': 'Short_USDT',
                'G': 'Short_percent'
            }

            for col, col_name in columns.items():
                row_data[col_name] = sheet_data[col][row]

            # 使用A列的标签作为key
            key = label.lower().replace(' & ', '_and_').replace(' ', '_').replace('&', '_and_').replace('/', '_').replace('%', 'percent')
            data[key] = row_data

        return data

    # 获取Performance表数据
    performance_data = build_sheet_data(excel_data['Performance'])
    # 获取交易分析数据
    trades_analysis_data = build_sheet_data(excel_data['Trades analysis'])
    # 获取风险指标数据
    risk_performance_data = build_sheet_data(excel_data['Risk performance ratios'])
    # print(performance_data)
    # print(trades_analysis_data)
    # print(risk_performance_data)

    # 获取属性数据
    props = {}
    properties_sheet = excel_data['Properties']

    for i in range(len(properties_sheet['A'])):
        props[properties_sheet['A'][i]] = properties_sheet['B'][i]

    # 解析日期范围
    def parse_date_range(date_str):
        if not date_str or not isinstance(date_str, str):
            return None, None
        parts = date_str.split("—")
        if len(parts) == 2:
            return parse_date(parts[0].strip()), parse_date(parts[1].strip())
        return None, None

    trading_range_start, trading_range_end = parse_date_range(props.get("Trading range"))
    backtesting_range_start, backtesting_range_end = parse_date_range(props.get("Backtesting range"))
    start_date = parse_date(props.get("Start Date"))

    # 2.写入数据库
    try:
        with connection.cursor() as cursor:
            # 获取ticker_id
            symbol = props.get("Symbol", "")
            exchangeticker_id = ""  # 根据symbol得到exchangeticker_id
            cursor.execute("SELECT id FROM ExchangeTicker WHERE ticker_id = %s", (exchangeticker_id,))
            result = cursor.fetchone()
            if not result:
                raise ValueError(f"Symbol {symbol} not found in database @insert_backtesting_data")
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

            # 处理可能为空的数值
            def safe_float(value, default=0.0):
                try:
                    if value is None or str(value).strip() == '':
                        return default
                    if isinstance(value, str) and '%' in value:
                        return float(value.rstrip('%'))
                    return float(value)
                except (ValueError, TypeError):
                    return default

            def safe_int(value, default=0):
                try:
                    if value is None or str(value).strip() == '':
                        return default
                    return int(value)
                except (ValueError, TypeError):
                    return default

            def safe_str(value, default=''):
                if value is None:
                    return default
                return str(value).strip()

            backtesting_data = (
                ticker_id,
                json.dumps(performance_data, ensure_ascii=False),
                json.dumps(trades_analysis_data, ensure_ascii=False),
                json.dumps(risk_performance_data, ensure_ascii=False),
                'Custom_Signal', # ？？？？？？？？？？？？？？？？？？？？？？？？？？
                trading_range_start,
                trading_range_end,
                backtesting_range_start,
                backtesting_range_end,
                symbol,
                safe_str(props.get("Timeframe")),
                safe_float(props.get("Point value")),
                safe_str(props.get("Chart type")),
                safe_str(props.get("Currency")),
                safe_float(props.get("Tick size")),
                safe_str(props.get("Precision")),
                start_date,
                safe_float(props.get("Initial capital")),
                safe_int(props.get("Order size")),
                safe_int(props.get("Pyramiding")),
                safe_float(props.get("Commission")),
                safe_int(props.get("Slippage")),
                safe_int(props.get("Verify price for limit orders")),
                safe_float(props.get("Margin for long positions")),
                safe_float(props.get("Margin for short positions")),
                'On' if safe_str(props.get("Recalculate after order is filled")) == 'On' else 'Off',
                'On' if safe_str(props.get("Recalculate on every tick")) == 'On' else 'Off',
                'On' if safe_str(props.get("Recalculate on bar close")) == 'On' else 'Off',
                'On' if safe_str(props.get("Backtesting precision. Use bar magnifier")) == 'On' else 'Off'
            )

            cursor.execute(insert_query, backtesting_data)
            backtesting_id = cursor.lastrowid
            connection.commit()

            return backtesting_id

    except IntegrityError as e:
        # 处理唯一约束、外键约束等违反完整性错误
        connection.rollback()
        print(f"数据完整性错误: {e}. 已回滚事务")

    except DataError as e:
        # 处理数据格式、类型等错误
        connection.rollback()
        print(f"数据处理错误: {e}. 已回滚事务")

    except ProgrammingError as e:
        # 处理SQL语法错误、表不存在等问题
        connection.rollback()
        print(f"SQL编程错误: {e}. 已回滚事务")

    except OperationalError as e:
        # 处理数据库操作错误，如连接断开等
        connection.rollback()
        print(f"数据库操作错误: {e}. 已回滚事务")

        # 断开连接
        if 'Lost connection' in str(e):
            print("您已断开数据库连接")

    except InternalError as e:
        # 处理数据库内部错误
        connection.rollback()
        print(f"数据库内部错误: {e}. 已回滚事务")

    except NotSupportedError as e:
        # 处理不支持的数据库特性
        connection.rollback()
        print(f"不支持的数据库特性: {e}. 已回滚事务")

    except Error as e:
        # 捕获其他PyMySQL错误
        connection.rollback()
        print(f"数据库错误: {e}. 已回滚事务")


def insert_trade_data(connection, backtesting_id, excel_data):
    """
    将excel数据写入trade表
    :param connection: 数据库连接实例
    :param backtesting_id: int
    :param excel_data: dict
    :return: bool
    """
    # 1. 从excel获取数据（跳过第一行表头）
    trades = excel_data['List of trades'][1:]

    # 统计成功插入的交易记录数
    total_insert_num = trades_num = len(trades['A'])

    # 2. 批量插入数据
    try:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO Trade (
                backtesting_id, trade_id, trade_type, signal_type, exec_time, exec_price, quantity,
                pnl_absolute, pnl_percent, runup_absolute, runup_percent, 
                drawdown_absolute, drawdown_percent, cumulative_pnl_absolute, cumulative_pnl_percent
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 小数点后位数
            getcontext().prec = 8

            def parse_decimal(value):
                return Decimal(str(value).replace(',', '').strip()) # value有的是str有的是float，所以统一str()

            # 2. 准备批量插入数据
            batch_data = []
            for i in range(len(trades['A'])):
                try:
                    # 获取当前行数据
                    trade_num = trades['A'][i]
                    trade_type = trades['B'][i]

                    # 跳过未关闭的交易
                    if trade_type is None or trade_type.endswith('Open'):
                        continue

                    trade_type = 'long' if 'long' in str(trade_type).lower() else 'short' # ？？？？？？？？？？

                    # 修改数据准备部分
                    trade_data = (
                        backtesting_id,
                        int(trade_num),
                        trade_type,
                        str(trades['C'][i]),
                        datetime.strptime(str(trades['D'][i]), "%Y-%m-%d %H:%M:%S"),
                        parse_decimal(trades['E'][i]),  # 价格
                        int(trades['F'][i]),            # Quantity
                        parse_decimal(trades['G'][i]),  # P&L金额
                        parse_decimal(trades['H'][i]),  # P&L%
                        parse_decimal(trades['I'][i]),  # Run-up金额
                        parse_decimal(trades['J'][i]),  # Run-up%
                        parse_decimal(trades['K'][i]),  # Drawdown金额
                        parse_decimal(trades['L'][i]),  # Drawdown%
                        parse_decimal(trades['M'][i]),  # 累计P&L金额
                        parse_decimal(trades['N'][i])   # 累计P&L%
                    )

                    batch_data.append(trade_data)

                except Exception as e:
                    print(f"跳过格式错误的交易记录 #{trade_num}: {str(e)}")
                    total_insert_num -= 1
                    continue

            # 批量执行插入
            if batch_data:
                cursor.executemany(insert_query, batch_data)
                connection.commit()
                print(f"成功写入 {len(total_insert_num)}/{trades_num} 条交易记录")
                return True
            else:
                print("警告：没有有效的交易记录可写入")
                return False

    except IntegrityError as e:
        # 处理唯一约束、外键约束等违反完整性错误
        connection.rollback()
        print(f"数据完整性错误: {e}. 已回滚事务")

    except DataError as e:
        # 处理数据格式、类型等错误
        connection.rollback()
        print(f"数据处理错误: {e}. 已回滚事务")

    except ProgrammingError as e:
        # 处理SQL语法错误、表不存在等问题
        connection.rollback()
        print(f"SQL编程错误: {e}. 已回滚事务")

    except OperationalError as e:
        # 处理数据库操作错误，如连接断开等
        connection.rollback()
        print(f"数据库操作错误: {e}. 已回滚事务")

        # 断开连接
        if 'Lost connection' in str(e):
            print("您已断开数据库连接")

    except InternalError as e:
        # 处理数据库内部错误
        connection.rollback()
        print(f"数据库内部错误: {e}. 已回滚事务")

    except NotSupportedError as e:
        # 处理不支持的数据库特性
        connection.rollback()
        print(f"不支持的数据库特性: {e}. 已回滚事务")

    except Error as e:
        # 捕获其他PyMySQL错误
        connection.rollback()
        print(f"数据库错误: {e}. 已回滚事务")


def insert_backtesting_excel_to_db(connection, excel_path):
    """
    将回测数据写入数据库
    :param excel_path: 回测文件路径
    :return: bool
    """
    print("开始insert_backtesting_excel_to_db")
    excel_data = parse_excel(excel_path)

    # 1. 写入backtesting表（引用ticker_id）
    print("开始insert_backtesting_data")
    backtesting_id = insert_backtesting_data(connection, excel_data)
    if not backtesting_id:
        print("写入backtesting表失败！")
        db_log(f"Backtesting table insertion failed for file: {excel_path}")
        return False

    # 2. 写入trade表
    print("开始insert_backtesting_data")
    success = insert_trade_data(connection, backtesting_id, excel_data)
    if not success:
        print("写入trade表失败！")
        db_log(f"Trade table insertion failed for backtesting ID: {backtesting_id}, file: {excel_path}")
        return False

    print("回测数据写入成功！")
    return True


def insert_backtesting_to_db():
    """
    1.将待写入回测文件目录中的所有excel文件写入数据库
    2.将文件移动到processed目录
    :return: bool
    """
    print("===== 开始insert_backtesting_to_db =====")

    # 1. 判断目录是否存在
    if not (os.path.exists(BACKTESTING_PROCESSED_DIR) and os.path.exists(BACKTESTING_NEW_DIR)):
        print("警告：文件目录不存在")
        return False

    # 2. 获取所有Excel文件（支持xlsx、xls、csv）
    files = list(BACKTESTING_NEW_DIR.glob("*.[xX][lL][sS]*")) + list(BACKTESTING_NEW_DIR.glob("*.[cC][sS][vV]"))

    if not files:
        print("没有待处理回测数据")
        return True

    # 3. 写入数据库+移动删除文件
    # 创建数据库连接
    connection = create_db_connection()
    if not connection:
        return False

    # 遍历所有待写入回测文件
    current_file = None
    try:
        for file_path in files:
            current_file = file_path
            if insert_backtesting_excel_to_db(connection, file_path): # 成功写入数据库
                # 构建目标路径
                dest_path = BACKTESTING_PROCESSED_DIR / file_path.name
                # 移动并删除文件
                shutil.move(file_path, dest_path)
                print(f"成功移动并删除文件: {file_path.name}")
            else: # 写入数据库失败
                print(f"文件写入数据库失败，无法移动删除: {file_path.name}")

    except Exception as e:
        # 移动删除文件异常，此时new目录仍有待处理文件
        print(f"移动删除文件失败： {current_file.name}: {str(e)}")

    # 4.关闭数据库连接
    if connection.open: # 两种情况：①数据库连接正常，但某个文件移动删除出现异常 ②一切正常，全部文件移动删除完成
        connection.close()
        print("数据库连接已正常关闭")
    else:
        print("数据库连接异常中断！@insert_backtesting_to_db")

    if os.listdir(BACKTESTING_NEW_DIR):
        print("部分回测文件写入失败！")
        return False
    print("全部回测文件写入成功！")
    return True


if __name__ == "__main__":
    # unzip_all_and_backup()
    insert_backtesting_to_db()
