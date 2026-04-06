import asyncio
from database.operations import DatabaseOperations

async def test():
    db = DatabaseOperations()
    await db.initialize()
    
    q2 = "SELECT min(time) as min_date, max(time) as max_date, count(*) as cnt FROM daily_quotes WHERE instrument_id = '399150.SZ'"
    res2 = await db.execute_read_query(q2)
    print("Quotes stats:", res2)

asyncio.run(test())
