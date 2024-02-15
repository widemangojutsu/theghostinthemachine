import asyncio
import databento as db

client = db.Historical("db-jgN3ipuDYEAuxspR3xcF4XPmMEUNr")

# Symbols and timeframes
symbols = ["NQ.c.0"]
time_ranges = [
    ("2021-02-02T00:00:00", "2022-02-02T00:00:00"),
    ("2022-02-03T00:00:00", "2023-02-02T00:00:00"),
    ("2023-02-03T00:00:00", "2024-02-02T00:00:00")
]

# Async function to fetch data and save to CSV
async def fetch_and_save_async(symbol, start_time, end_time):
    data = await client.timeseries.get_range_async(
        dataset="GLBX.MDP3",
        symbols=[symbol],
        schema='trades',
        start=start_time,
        end=end_time
    )
    df = data.to_df()
    filename = f"{symbol}_{start_time[:4]}_{end_time[:4]}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved {filename}")

# Main async function to run all tasks
async def main():
    tasks = []
    for symbol in symbols:
        for start_time, end_time in time_ranges:
            task = asyncio.create_task(fetch_and_save_async(symbol, start_time, end_time))
            tasks.append(task)
    await asyncio.gather(*tasks)

# Run the main async function
asyncio.run(main())