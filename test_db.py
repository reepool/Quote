import asyncio
from database.operations import DatabaseOperations

async def test():
    db = DatabaseOperations()
    await db.initialize()
    
    q1 = "SELECT instrument_id, listed_date, delisted_date, is_active FROM instruments WHERE instrument_id = '399150.SZ'"
    res1 = await db.execute_read_query(q1)
    print("Instrument info:", res1)
    
    q2 = "SELECT min(trade_date) as min_date, max(trade_date) as max_date, count(*) as cnt FROM daily_quotes WHERE instrument_id = '399150.SZ'"
    res2 = await db.execute_read_query(q2)
    print("Quotes stats:", res2)

asyncio.run(test())
