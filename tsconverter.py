import pandas as pd
import threading

# Function to process OHLCV data for a given time frame
def process_ohlc(ohlc_data, time_frame, output_filename):
    ohlc_data['timestamp'] = ohlc_data['timestamp'].str.slice(0, -3)  # Truncate seconds from timestamp
    ohlc_data['timestamp'] = pd.to_datetime(ohlc_data['timestamp'])  # Convert to datetime
    ohlc_data = ohlc_data.set_index('timestamp')  # Set timestamp as index
    ohlc_data.to_csv(output_filename)
    print(f"{time_frame} OHLCV data processed and saved to {output_filename}")

# Read tick data from CSV file
tick_data = pd.read_csv('C:/Users/myrcene/Documents/qwantss/Longest Holdings Corporation/enigma/ES.c.0_2021_2022_trades.csv')
tick_data['timestamp'] = pd.to_datetime(tick_data['ts_recv'])

# Read OHLCV data without timestamps
ohlc_1m = pd.read_csv('C:/Users/myrcene/Documents/qwantss/Longest Holdings Corporation/enigma/ohlc/1m/ES.c.0_1m.csv')  # Assuming you have OHLCV data for 1 minute intervals
ohlc_1h = pd.read_csv('C:/Users/myrcene/Documents/qwantss/Longest Holdings Corporation/enigma/ohlc/1h/ES.c.0_1h.csv')  # Assuming you have OHLCV data for 1 hour intervals
ohlc_1d = pd.read_csv('C:/Users/myrcene/Documents/qwantss/Longest Holdings Corporation/enigma/ohlc/1d/ES.c.0_1d.csv')  # Assuming you have OHLCV data for 1 day intervals

# Thread each time frame processing
threads = []

# 1-minute time frame
t1 = threading.Thread(target=process_ohlc, args=(ohlc_1m, '1-minute', 'ohlc_1m_with_timestamps.csv'))
threads.append(t1)

# 1-hour time frame
t2 = threading.Thread(target=process_ohlc, args=(ohlc_1h, '1-hour', 'ohlc_1h_with_timestamps.csv'))
threads.append(t2)

# 1-day time frame
t3 = threading.Thread(target=process_ohlc, args=(ohlc_1d, '1-day', 'ohlc_1d_with_timestamps.csv'))
threads.append(t3)

# Start all threads
for thread in threads:
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All processing completed.")
