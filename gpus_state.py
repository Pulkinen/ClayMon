from datetime import datetime
from datetime import timedelta
import farm_stat_API
import numpy as np
import pandas as pd
import os

time_span_width_days = 6

dt = datetime.now()
last_date = datetime(dt.year, dt.month, dt.day)
first_date = last_date - timedelta(days=time_span_width_days)
dayz = (last_date - first_date).days

for d in range(dayz):
    d1 = first_date + timedelta(days=d)
    fname = rf'{farm_stat_API.cache_folder}\{d1:%Y-%m-%d}.csv'
    if not os.path.exists(fname):
        continue

    daily_df = pd.read_csv(fname)
    print(daily_df.shape)
