from database.operations import database_operations
import asyncio

async def main():
    trading_days = await database_operations.get_trading_days(exchange='SSE', start_date='2025-10-09', end_date='2025-10-09', is_trading_day=True)
    print(trading_days)

if __name__ == "__main__":
    asyncio.run(main())