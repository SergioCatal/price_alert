import yfinance as yf
import pandas as pd
import datetime
from typing import Tuple, Dict
from enum import Enum
import requests
import time
import random

from yaml import load, dump, YAMLError

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def read_yaml_file_and_check_for_items(file_path, required_items):
    try:
        with open(file_path, "r") as f:
            yaml_contents = load(f, Loader)
            yaml_keys = yaml_contents.keys()
            for req_item in required_items:
                if req_item not in yaml_keys:
                    print(f"ERROR: Yaml file {file_path} does not contain item {req_item}")
                    exit(1)
            return yaml_contents
    except OSError as e:
        print(f"ERROR: Unable to open yaml configuration file!\n{e}")
        exit(1)
    except YAMLError as e:
        print(f"ERROR: Unable to parse yaml configuration file!\n{e}")
        exit(1)


def get_telegram_configs():
    configs = read_yaml_file_and_check_for_items("secrets.yaml", ["token", "group_id"])
    return configs["token"], configs["group_id"]


# Telegram stuff
API_TOKEN, GROUP_ID = get_telegram_configs()
BASE_URL = f"https://api.telegram.org/bot{API_TOKEN}"
SEND_MESSAGE_URL = f"{BASE_URL}/sendMessage"
GET_UPDATES_URL = f"{BASE_URL}/getUpdates"
SET_COMMANDS_URL = f"{BASE_URL}/setMyCommands"


# print(requests.get(GET_UPDATES_URL).json())
# exit(0)


class SymbolStatus(Enum):
    WITHIN_RANGE = 0
    ABOVE_RANGE = 1
    BELOW_RANGE = 2


def get_last_day_info(ticker: yf.Ticker) -> pd.DataFrame:
    return ticker.history(period="1d", interval="1d", actions=False)


def get_last_day_and_close(tickers: yf.Tickers) -> Dict[str, Tuple[datetime.date, float]]:
    update = {}
    for k in tickers.tickers.keys():
        info = get_last_day_info(tickers.tickers[k])

        last_day = info.index[0].to_pydatetime().date()
        last_close = info.iloc[0]["Close"]
        update[k] = (last_day, last_close)

    return update


def get_symbol_status(val, range):
    if val < range[0]:
        return SymbolStatus.BELOW_RANGE
    elif val < range[1]:
        return SymbolStatus.WITHIN_RANGE
    else:
        return SymbolStatus.ABOVE_RANGE


conf = read_yaml_file_and_check_for_items("config.yaml", ["alerts"])

alerts = conf["alerts"]
# TODO: Check that lower trigger is smaller than upper trigger
for alert in alerts.values():
    alert["last_update"] = None

    price_range = [float('-inf'), float('inf')]
    if "lower_trigger" in alert:
        price_range[0] = alert["lower_trigger"]
    if "upper_trigger" in alert:
        price_range[1] = alert["upper_trigger"]

    assert price_range[0] < price_range[1]
    alert["price_range"] = price_range

# important_keys = ["last_update", "price_range"]
# alerts = {k: {ik: v[ik] for ik in important_keys} for k, v in alerts.items()}
tickers = yf.Tickers(list(alerts.keys()))

continue_polling = True
min_sleep_time_s = conf["min_sleep_time_s"]
random_extra_sleep_time_s = conf["random_extra_sleep_time_s"]
while continue_polling:
    text = ""

    try:
        update = get_last_day_and_close(tickers)
        for k, v in update.items():
            alert = alerts[k]
            price_range = alert["price_range"]
            last_update = alert["last_update"]

            new_update = get_symbol_status(v[1], price_range)

            if new_update != last_update:
                text += f"**{alert['name']}**\n{last_update.name if last_update is not None else 'None'} -> {new_update.name}\nV: {v[1]:.3f} -- [{price_range[0]:.3f},{price_range[1]:.3f}]\n\n"
                alert["last_update"] = new_update
    except Exception as e:
        text = f"Failed to get data! {e}"

    if len(text) > 0:
        res = requests.get(f"{SEND_MESSAGE_URL}?chat_id={GROUP_ID}&text={text}")
        print(f"Sent update message!\n\n{text}")

    sleep_time = random.random() * random_extra_sleep_time_s + min_sleep_time_s
    print(f"Sleeping for {sleep_time / 3600}h")
    time.sleep(sleep_time)
    # continue_polling = False
