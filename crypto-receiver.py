import websocket
import json
import logging
import time
import pymysql
from db_config import DB_CONFIG
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BinanceKlineWatcher:
    def __init__(self, symbol, interval="5m"):
        self.symbol = symbol.lower()
        self.interval = interval
        self.ws = None

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            kline = data["data"]["k"]

            if kline["x"]:  # x表示这根K线是否完结
                symbol = kline["s"].lower()
                logger.info(f"\n新的{self.interval}K线数据 - {symbol}:")
                print(f"交易对: {kline['s']}")
                print(f"开盘时间: {self.format_timestamp(kline['t'])}")
                print(f"开盘价: {kline['o']}")
                print(f"最高价: {kline['h']}")
                print(f"最低价: {kline['l']}")
                print(f"收盘价: {kline['c']}")
                print(f"交易量: {kline['v']}")
                print("-" * 50)

                try:
                    conn = pymysql.connect(**DB_CONFIG)
                    cursor = conn.cursor()

                    table_name = "crypto_kline_data"
                    sql = f"""
                    INSERT INTO crypto_kline_data (
                        symbol, open_time, open_timestamp, open_price, high_price, low_price, 
                        close_price, volume, close_time, close_timestamp, quote_volume, trades_count
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """

                    # 准备数据
                    open_time = self.format_timestamp(kline["t"])
                    close_time = self.format_timestamp(kline["T"])

                    data = (
                        self.symbol,  # 添加symbol字段
                        open_time,
                        kline["t"],  # 原始开盘时间戳
                        float(kline["o"]),  # open_price
                        float(kline["h"]),  # high_price
                        float(kline["l"]),  # low_price
                        float(kline["c"]),  # close_price
                        float(kline["v"]),  # volume
                        close_time,
                        kline["T"],  # 原始收盘时间戳
                        float(kline["q"]),  # quote volume
                        int(kline["n"]),  # number of trades
                    )

                    cursor.execute(sql, data)
                    conn.commit()
                    logger.info("成功插入K线数据到数据库")

                except Exception as e:
                    logger.error(f"数据库操作失败: {e}")
                finally:
                    if "cursor" in locals():
                        cursor.close()
                    if "conn" in locals():
                        conn.close()

        except Exception as e:
            logger.error(f"处理消息时出错: {e}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket错误: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info("WebSocket连接关闭")
        logger.info("3秒后尝试重新连接...")
        time.sleep(3)
        self.start()

    def on_open(self, ws):
        logger.info(f"已连接到{self.symbol}的{self.interval}数据流")

    @staticmethod
    def format_timestamp(ts):
        from datetime import datetime

        # 修改时间戳格式化方法，返回datetime对象而不是字符串
        return datetime.fromtimestamp(ts / 1000)

    def start(self):
        while True:
            try:
                websocket.enableTrace(True)
                stream_url = f"wss://stream.binance.com/stream?streams={self.symbol}@kline_{self.interval}"

                self.ws = websocket.WebSocketApp(
                    stream_url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open,
                    on_ping=self.on_ping,
                    on_pong=self.on_pong,
                )

                self.ws.run_forever(ping_interval=30, ping_timeout=10)  # 添加心跳检测

            except Exception as e:
                logger.error(f"连接出错: {e}")
                logger.info("3秒后重试...")
                time.sleep(3)

    def on_ping(self, ws, message):
        logger.debug(f"Received ping from {self.symbol}")

    def on_pong(self, ws, message):
        logger.debug(f"Received pong from {self.symbol}")


def get_watch_symbols():
    """从数据库获取需要监控的交易对"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        sql = """
        SELECT symbol, timegap 
        FROM watch_symbols 
        WHERE ifshow = 'y' 
        AND api_vendor = 'binance'
        """

        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"获取监控交易对失败: {e}")
        return []
    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    try:
        # 获取所有需要监控的交易对
        symbols = get_watch_symbols()

        if not symbols:
            logger.error("没有找到需要监控的交易对")
            exit(1)

        # 为每个交易对创建数据表并启动监控
        watchers = []
        for symbol_info in symbols:
            symbol = symbol_info["symbol"].lower()
            interval = symbol_info["timegap"]

            # 创建并启动监控
            watcher = BinanceKlineWatcher(symbol=symbol, interval=interval)
            thread = threading.Thread(target=watcher.start)
            thread.daemon = True
            thread.start()
            watchers.append((watcher, thread))

            logger.info(f"已启动 {symbol} {interval} 监控")
            time.sleep(1)  # 稍微延迟，避免同时建立太多连接

        # 保持主线程运行
        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("程序正在停止...")
        for watcher, _ in watchers:
            if watcher.ws:
                watcher.ws.close()
        logger.info("程序已停止")
