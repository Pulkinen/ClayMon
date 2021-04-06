from datetime import datetime
from datetime import timedelta
import farm_stat_API
import numpy as np
import pandas as pd
import os

def make_for_me_stat_df(d1, d2, stats):
    fstats = []
    ts1 = int(d1.timestamp())
    ts2 = int(d2.timestamp())

    for stat in stats:
        if stat is None:
            continue
        mask = (stat.dt >= ts1) & (stat.dt < ts2)
        fstat = stat[mask]
        fstats += [fstat]
    rez = pd.concat(fstats)
    return rez

dt = datetime.now()
# last_date = datetime(dt.year, dt.month, dt.day)
# first_date = last_date - timedelta(days=7)
last_date = datetime(2020, 11, 4)
first_date = datetime(2020, 10, 3)

# first_date = last_date - timedelta(days=2)
dayz = (last_date - first_date).days

stats = farm_stat_API.api_gimme_farm_stat(first_date, last_date)
for d in range(dayz):
    d1 = first_date + timedelta(days=d)
    d2 = d1 + timedelta(days=1)
    fname = rf'{farm_stat_API.cache_folder}\{d1:%Y-%m-%d}.csv'
    if os.path.exists(fname):
        continue

    daily_df = make_for_me_stat_df(d1, d2, stats)
    if daily_df is None:
        continue
    daily_df.to_csv(fname)


