import asyncio
import websockets
import json
import logging
from tabulate import tabulate
import aiomysql
from db_config import DB_CONFIG, SQL_QUERIES

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class StockClient:
    def __init__(self):
        self.stocks_data = {}
        self.klines = {}
        self.ws = None
        self.db_pool = None

    async def init_db(self):
        """初始化数据库连接池"""
        try:
            self.db_pool = await aiomysql.create_pool(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                db=DB_CONFIG['db'],
                charset=DB_CONFIG['charset'],
                autocommit=True
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    async def save_stock_data(self, data):
        """保存股票数据到数据库"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        SQL_QUERIES['insert_stock_data'],
                        {
                            'symbol': data['symbol'],
                            'current_price': data['current_price'],
                            'high': data['high'],
                            'low': data['low'],
                            'volume': data['volume'],
                            'amount': data['amount'],
                            'last_updated': data['last_updated']
                        }
                    )
        except Exception as e:
            logger.error(f"Error saving stock data: {e}")

    async def update_stock_data(self, data):
        """更新股票数据并打印市场摘要"""
        self.stocks_data[data["symbol"]] = data
        self.print_market_summary()
        # 保存到数据库
        await self.save_stock_data(data)

    def print_market_summary(self):
        """打印格式化的市场数据表格"""
        rows = []
        for symbol, data in self.stocks_data.items():
            rows.append(
                [
                    symbol,
                    f"${data['current_price']:.2f}",
                    f"${data['high']:.2f}",
                    f"${data['low']:.2f}",
                    f"{data['volume']:,}",
                    f"${data['amount']:,.2f}",
                    data["last_updated"].split("T")[1].split(".")[0],
                ]
            )

        headers = ["Symbol", "Price", "High", "Low", "Volume", "Amount", "Time"]
        table = tabulate(rows, headers=headers, tablefmt="pretty")

        # 清屏并打印
        print("\033[2J\033[H")  # 清屏并移动光标到顶部
        print("=== Real-time Market Data ===")
        print(table)

    async def connect(self):
        """建立 WebSocket 连接"""
        try:
            # 如果存在旧连接，尝试关闭它
            if self.ws is not None:
                try:
                    await self.ws.close()
                except:
                    pass
            
            # 创建新连接
            self.ws = await websockets.connect("ws://localhost:8765/")
            return self.ws
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            self.ws = None
            raise

    async def start(self):
        """启动客户端主循环"""
        while True:
            try:
                ws = await self.connect()
                while True:
                    message = await ws.recv()
                    data = json.loads(message)

                    if data["type"] == "initial_data":
                        logger.info("Received initial data for %d symbols", len(data['data']))
                        for _, stock_data in data["data"].items():
                            await self.update_stock_data(stock_data)

                    elif data["type"] == "stock_update":
                        await self.update_stock_data(data["data"])

            except websockets.exceptions.ConnectionClosed:
                logger.error("Connection to server closed")
                await asyncio.sleep(5)  # 等待后重试
            except Exception as e:
                logger.error(f"Error: {e}")
                await asyncio.sleep(5)  # 等待后重试

    async def fetch_klines(self, symbol, timeframe="1m"):
        """获取K线数据"""
        try:
            async with websockets.connect("ws://localhost:8765") as ws:
                # 发送K线数据请求
                request = {
                    "type": "subscribe_klines",
                    "symbol": symbol,
                    "timeframe": timeframe
                }
                await ws.send(json.dumps(request))

                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    if data["type"] == "kline_update":
                        # 处理K线数据
                        self.klines[symbol] = data["data"]
                        logger.info(f"Received kline update for {symbol}")
                        # 保存K线数据到数据库
                        await self.save_kline_data(symbol, data["data"])

        except websockets.exceptions.ConnectionClosed:
            logger.error("Connection to server closed")
        except Exception as e:
            logger.error(f"Error fetching klines: {e}")

    async def save_kline_data(self, symbol, kline_data):
        """保存K线数据到数据库"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for kline in kline_data:
                        await cur.execute(
                            SQL_QUERIES['insert_kline_data'],
                            {
                                'symbol': symbol,
                                'timeframe': kline['timeframe'],
                                'open': kline['open'],
                                'high': kline['high'],
                                'low': kline['low'],
                                'close': kline['close'],
                                'volume': kline['volume'],
                                'timestamp': kline['timestamp']
                            }
                        )
        except Exception as e:
            logger.error(f"Error saving kline data: {e}")


async def main():
    client = StockClient()
    logger.info("Starting stock data client...")
    
    # 初始化数据库连接
    await client.init_db()
    logger.info("Connecting to websocket server...")
    
    try:
        # 启动主数据流
        await client.start()
    except KeyboardInterrupt:
        logger.info("Client stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.info("Client stopped due to error")
    finally:
        # 关闭数据库连接池
        if client.db_pool:
            await client.db_pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nClient stopped by user")