from bqdatasdk import DataSource
import pandas as pd
import gzip
import json
import os
import sys
import time
import platform

if platform.system() == "Windows":
    sys.path.append("./libs/win")
elif platform.system() == "Linux":
    sys.path.append("./libs/linux")

from pycqlib import quote, trade
from pycqlib.base import Exchange
from pycqlib.quote.feature.factors import SbqHftFeatureManager

# read aiquant config
with open("config/aiquant.json", "r") as file:
    aiquant_config = json.load(file)
    DataSource.init(aiquant_config["url"], aiquant_config["token"])

start_time = time.time()
print(f'Starts at {time.strftime("%Y-%m-%d %H:%M:%S")}')
# get last trade date
now_stamp = pd.Timestamp.now()
timestamp1 = now_stamp.replace(hour=15, minute=0, second=0, microsecond=0)
timestamp2 = now_stamp.replace(hour=22, minute=0, second=0, microsecond=0)
last_index = -2 if timestamp1 < now_stamp < timestamp2 else -1

df_tradedays = DataSource("trading_days").read(fields=["date"])
target_timestamp = df_tradedays.iloc[last_index]["date"]
TIME_SHIFT = (now_stamp - target_timestamp).days * 24 * 3600 * 1000000000
DATE_STR = target_timestamp.strftime("%Y-%m-%d")
print(f"Target Date: {DATE_STR}")


def read_file(filename, compress=True):
    print(f"read {filename}")
    if compress:
        with gzip.open(filename, "r") as file:
            return json.loads(file.read().decode("utf8"))
    else:
        with open(filename, "r") as file:
            return json.load(file)


def write_file(jData, filename, compress=True):
    print(f"write {filename}")
    if compress:
        with gzip.open(filename, "w") as file:
            file.write(json.dumps(jData).encode("utf8"))
    else:
        with open(filename, "w") as file:
            json.dump(jData, file)


# read stocklist
sh_list = read_file("config/sh_list.json", compress=False)
sz_list = read_file("config/sz_list.json", compress=False)
stock_list_sh = sh_list
stock_list = sh_list + sz_list
print(f"Finish reading sh_list, length = {len(sh_list)}")
print(f"Finish reading sz_list, length = {len(sz_list)}")

# Feature Managers
FMS = {}

# bar update
print("Downloading kl1m data")
kl1m_data = DataSource("level2_bar1m_CN_STOCK_A").read(
    start_date=f"{DATE_STR} 14:27:00",
    end_date=f"{DATE_STR} 14:57:00",
    fields=["close", "open", "high", "low", "amount", "volume", "num_trades"],
    instruments=stock_list,
)

for gp_name, gp_df in kl1m_data.groupby("instrument"):
    code, mkt = gp_name.split(".")
    if mkt == "SZA":
        market = Exchange.SZE
        cq_symbol = f"{code}.SZE"
    else:
        market = Exchange.SSE
        cq_symbol = f"{code}.SSE"

    FMS[cq_symbol] = SbqHftFeatureManager()
    for _, row in gp_df.iterrows():
        bar = quote.BarData()
        bar.datetime = (
            int(row["date"].tz_localize(tz="Asia/Shanghai").timestamp() * 1e9)
            + TIME_SHIFT
        )
        bar.exchange = market
        bar.interval = quote.BarInterval.MINUTE_1
        bar.open = round(row["open"], 2)
        bar.high = round(row["high"], 2)
        bar.low = round(row["low"], 2)
        bar.last = round(row["close"], 2)
        # bar.pre_close=
        bar.symbol = code
        bar.total_volume = row["volume"]
        bar.total_value = row["amount"]
        bar.total_num = row["num_trades"]
        FMS[cq_symbol].on_bar(bar)
    print(f"Finish kl1m of: {gp_name}")
print("Finish all kl1m")

# tick update
print("Downloading tick data")
tick_data = DataSource("level2_snapshot_CN_STOCK_A_days").read(
    start_date=f"{DATE_STR} 14:29:57",
    end_date=f"{DATE_STR} 14:56:59",
    instruments=stock_list,
)

for gp_name, gp_df in tick_data.groupby("instrument"):
    code, mkt = gp_name.split(".")
    if mkt == "SZA":
        market = Exchange.SZE
        cq_symbol = f"{code}.SZE"
    else:
        market = Exchange.SSE
        cq_symbol = f"{code}.SSE"

    for _, row in gp_df.iterrows():
        tick = quote.TickData()
        tick.datetime = (
            int(row["date"].tz_localize(tz="Asia/Shanghai").timestamp() * 1e9)
            + TIME_SHIFT
        )
        tick.exchange = market
        tick.open = round(row["open"] / 1e4, 2)
        tick.high = round(row["high"] / 1e4, 2)
        tick.low = round(row["low"] / 1e4, 2)
        tick.last = round(row["price"] / 1e4, 2)
        tick.pre_close = round(row["pre_close"] / 1e4, 2)
        tick.symbol = code
        tick.total_volume = row["volume"]
        tick.total_ask_volume = row["total_ask_volume"]
        tick.total_bid_volume = row["total_bid_volume"]
        tick.total_value = row["amount"]
        tick.total_num = row["num_trades"]
        tick.ask_volumes = row[[f"ask_volume{i+1}" for i in range(10)]].tolist()
        tick.ask_prices = row[[f"ask_price{i+1}" for i in range(10)]].tolist()
        tick.bid_volumes = row[[f"bid_volume{i+1}" for i in range(10)]].tolist()
        tick.bid_prices = row[[f"bid_price{i+1}" for i in range(10)]].tolist()
        # tick.high_limited
        # tick.low_limited
        # tick.trade_status
        # tick.avg_ask_price=round(row['ask_avg_price']/1e4, 2)
        # tick.avg_bid_price=round(row['bid_avg_price']/1e4, 2)
        # tick.ask_num_orders=row[[f'ask_num_orders{i+1}' for i in range(10)]].tolist()
        # tick.bid_num_orders=row[[f'bid_num_orders{i+1}' for i in range(10)]].tolist()
        tick.ask_one_orders = row[[f"ask_one_orders{i+1}" for i in range(50)]].tolist()
        tick.bid_one_orders = row[[f"bid_one_orders{i+1}" for i in range(50)]].tolist()
        # tick.settle
        # tick.pre_settle
        # tickk.total_position
        FMS[cq_symbol].on_tick(tick)
    print(f"Finish tick of: {gp_name}")
print("Finish all ticks")

# transaction update
print("Downloading trade data")
trade_data = DataSource("level2_trade_CN_STOCK_A_days").read(
    start_date=f"{DATE_STR} 14:30:00",
    end_date=f"{DATE_STR} 14:57:00",
    instruments=stock_list,
)
transaction_data = trade_data[trade_data.bs_flag != "C"]


for gp_name, gp_df in transaction_data.groupby("instrument"):
    code, mkt = gp_name.split(".")
    if mkt == "SZA":
        market = Exchange.SZE
        cq_symbol = f"{code}.SZE"
    else:
        market = Exchange.SSE
        cq_symbol = f"{code}.SSE"

    for _, row in gp_df.iterrows():
        t = quote.TransactionData()
        t.datetime = (
            int(row["date"].tz_localize(tz="Asia/Shanghai").timestamp() * 1e9)
            + TIME_SHIFT
        )
        # t.index
        # t.long_index
        # t.short_index
        t.price = round(row["price"] / 1e4, 2)
        t.volume = row["volume"]
        t.direction = (
            trade.OrderDirection.LONG
            if row["bs_flag"] == "B"
            else trade.OrderDirection.SHORT
        )
        t.exchange = market
        t.symbol = code
        FMS[cq_symbol].on_transaction(t)
    print(f"Finish transacton of: {gp_name}")
print("Finish all transactons")


cancel_data_sz = trade_data[trade_data.bs_flag == "C"]
print("Downloading sh order data")
order_data = DataSource("level2_order_CN_STOCK_A_days").read(
    start_date=f"{DATE_STR} 14:30:00",
    end_date=f"{DATE_STR} 14:57:00",
    instruments=stock_list_sh,
)
cancel_data_sh = order_data[order_data.order_type == "D"]
# cancel update
for gp_name, gp_df in cancel_data_sz.groupby("instrument"):
    code, mkt = gp_name.split(".")
    if mkt == "SZA":
        market = Exchange.SZE
        cq_symbol = f"{code}.SZE"
    else:
        market = Exchange.SSE
        cq_symbol = f"{code}.SSE"

    for _, row in gp_df.iterrows():
        t = quote.CancelData()
        t.datetime = (
            int(row["date"].tz_localize(tz="Asia/Shanghai").timestamp() * 1e9)
            + TIME_SHIFT
        )
        # t.index
        # t.cancel_index
        t.price = round(row["price"] / 1e4, 2)
        t.volume = row["volume"]
        t.direction = (
            trade.OrderDirection.LONG
            if row["bs_flag"] == "B"
            else trade.OrderDirection.SHORT
        )
        t.exchange = market
        t.symbol = code
        FMS[cq_symbol].on_cancel(t)
    print(f"Finish sh cancel of: {gp_name}")
print("Finish all sh cancels")

for gp_name, gp_df in cancel_data_sh.groupby("instrument"):
    code, mkt = gp_name.split(".")
    if mkt == "SZA":
        market = Exchange.SZE
        cq_symbol = f"{code}.SZE"
    else:
        market = Exchange.SSE
        cq_symbol = f"{code}.SSE"

    for _, row in gp_df.iterrows():
        t = quote.CancelData()
        t.datetime = (
            int(row["date"].tz_localize(tz="Asia/Shanghai").timestamp() * 1e9)
            + TIME_SHIFT
        )
        # t.index
        # t.cancel_index
        t.price = round(row["price"] / 1e4, 2)
        t.volume = row["volume"]
        t.direction = (
            trade.OrderDirection.LONG
            if row["bs_flag"] == "B"
            else trade.OrderDirection.SHORT
        )
        t.exchange = market
        t.symbol = code
        FMS[cq_symbol].on_cancel(t)
    print(f"Finish sz cancel of: {gp_name}")
print("Finish all sz cancels")

all_jsons = {}
for cq_symbol in FMS:
    all_jsons[cq_symbol] = FMS[cq_symbol].to_json()

OUTPUT_DIR = f"output/{DATE_STR}"
os.makedirs(OUTPUT_DIR, exist_ok=True)
write_file(all_jsons, f"{OUTPUT_DIR}/LastState.json", compress=False)
write_file(all_jsons, f"{OUTPUT_DIR}/LastState.json.gz", compress=True)

end_time = time.time()
print(f'Ends at {time.strftime("%Y-%m-%d %H:%M:%S")}')
print(f"Program costs {end_time-start_time:.2f} seconds")
