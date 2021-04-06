from datetime import datetime
from datetime import timedelta
import farm_stat_API
import numpy as np
import pandas as pd
# -Читаем файлик дейли-репорт, определяем последнюю дату
# -С этой даты по 0:00 текущей даты делаем запрос хешрейта
# -читаем файлик блекаутов
# -дописываем в файл дейли-репорта несколько строчек
# -Дата, Сложность, Eth/USD, USD/Rub, Средний хешрейт, блекаут ч,

idx_ver = 0
idx_uptime = 1
idx_hashrate = 2
idx_shares = 3
idx_rejected = 4
idx_gpu_hashrates = 5
idx_gpu_t = 6
idx_gpu_fan = 7
idx_pool = 8
idx_gpu_shares = 9
idx_gpu_rej = 10
idx_gpu_inv = 11
idx_gpu_bus_indexes = 12

dr_filename = 'daily_reports.txt'
blacks_filename = 'blackouts.txt'
one_day = timedelta(days=1)

def get_new_reports_start_date():
    f = open(dr_filename, 'r')
    yesterday = datetime.now().replace(hour=0,minute=0, second=0,microsecond=0) - one_day
    dt = None
    for s in f:
        lst = s.split(';')
        try:
            dt2 = datetime.strptime(lst[0], '%Y-%m-%d %H:%M:%S').replace(hour=0,minute=0, second=0,microsecond=0)
            if dt is None or dt2 > dt:
                dt = dt2
        except ValueError as e:
            pass
        except Exception as e:
            print(type(e), e)
    f.close()
    if dt is None or dt > yesterday:
        dt = yesterday
    return dt

def get_blackouts():
    rez = []
    f = open(blacks_filename, 'r')
    for s in f:
        lst = s.split(';')
        try:
            start = datetime.strptime(lst[0], '%Y-%m-%d %H:%M:%S')
            stop = datetime.strptime(lst[0], '%Y-%m-%d %H:%M:%S')
            assert(start.date() == stop.date())
            rez.append((start, stop))
        except ValueError as e:
            pass
        except Exception as e:
            print(type(e), e)
    f.close()
    return rez

def evalute_daily_hashrate(first_date, last_date, stat, blackouts):
    dayz = (last_date - first_date).days
    recs = [dict(hrs=0, mindt=None, maxdt=None, blacks=0, snapcnt=0) for i in range(dayz)]
    files = [open('dump_%d.txt'%i, 'w') for i in range(dayz)]
    for dt, snapshot in stat.items():
        if dt < first_date or dt >= last_date:
            continue
        idx = (dt - first_date).days
        rec = recs[idx]
        if not rec['mindt'] or dt < rec['mindt']:
            rec['mindt'] = dt
        if not rec['maxdt'] or dt > rec['maxdt']:
            rec['maxdt'] = dt
        rec['snapcnt'] += 1

        hr = 0
        for mn, pitch in snapshot.items():
            if pitch[idx_hashrate]:
                hr += pitch[idx_hashrate]
                print(dt, mn, pitch[idx_hashrate], file=files[idx])
        recs[idx]['hrs'] = hr
    for start, stop in blackouts:
        if start < first_date or start >= last_date:
            continue
        idx = (start - first_date).days
        duration = (stop - start).seconds / 3600
        recs[idx]['blacks'] += duration
    rez = []
    print(recs)
    for idx in range(dayz):
        rec = recs[idx]
        # Если вдруг случится блекаут с переходом через полночь - считать будет неправильно
        if rec['maxdt'] is None or rec['mindt'] is None:
            files[idx].close()
            continue
        daylen = (rec['maxdt'] - rec['mindt']).seconds / 3600
        rec['hrs'] = rec['hrs']*24/(daylen - rec['blacks'])
        rez.append((first_date + idx*one_day, rec['hrs']/1000, rec['blacks'], rec['snapcnt']))
        files[idx].close()
    return rez


def evalute_daily_hashrate_from_pandas_stat(first_date, last_date, pandas_stat, blackouts):
    dayz = (last_date - first_date).days
    recs = [dict(hrs=0, mindt=None, maxdt=None, blacks=0, snapcnt=0) for i in range(dayz)]
    df = pandas_stat[['dt', 'hashrate']].groupby('dt').agg({'hashrate':np.sum, 'dt':np.max})
    df_date_time = df.dt.apply(datetime.fromtimestamp)
    df['dayz'] = df_date_time.apply(datetime.date)
    df = df[(df.dayz>=first_date.date()) & (df.dayz<=last_date.date())]
    df2 = df.groupby('dayz').agg({'hashrate':[np.sum, np.mean], 'dt':np.size})
    df2.columns = ["_".join(x) for x in df2.columns.ravel()]
    print(df2.columns)
    df2['avg_hr']=df2.hashrate_sum/8640/1000
    df2['avg_hr2']=df2.hashrate_mean/1000
    df2.index = range(len(df2))
    rez = []
    for idx in df2.index:
        # Если вдруг случится блекаут с переходом через полночь - считать будет неправильно
        rez.append((first_date + idx*one_day, df2.avg_hr[idx], df2.avg_hr2[idx], df2.dt_size[idx]))
    return rez

def evalute_daily_hashrate_from_pandas_stats(first_date, pandas_stats):
    if pandas_stats is None or len(pandas_stats)==0:
        return []
    last_date = first_date + timedelta(days = 1)
    ts1 = int(first_date.timestamp())
    ts2 = int(last_date.timestamp())
    fstats = []
    for stat in pandas_stats:
        if stat is None:
            continue
        mask = (stat.dt >= ts1) & (stat.dt < ts2)
        fstat = stat[['dt', 'hashrate', 'uptime_id']][mask]
        fstats += [fstat]


    df = pd.concat(fstats)
    df = df.groupby('dt').agg({'hashrate':np.sum, 'dt':np.max, 'uptime_id':np.max})
    df_date_time = df.dt.apply(datetime.fromtimestamp)
    df['dayz'] = df_date_time.apply(datetime.date)
    df = df[(df.dayz>=first_date.date()) & (df.dayz<=last_date.date())]

    df_uptimes = df.groupby('uptime_id').agg({'dt':[np.min, np.max]})
    df_uptimes.columns = ["_".join(x) for x in df_uptimes.columns.ravel()]
    changes = list(df_uptimes.sort_values(by=['dt_amin']).values.ravel())
    changes = [ts1] + changes + [ts2]
    downtimes =  [((a-ts1)/3600, (b-ts1)/3600) for a,b in zip(changes[::2], changes[1::2]) if a < b]

    df2 = df.groupby('dayz').agg({'hashrate':[np.sum, np.mean], 'dt':np.size})
    df2.columns = ["_".join(x) for x in df2.columns.ravel()]
    df2['avg_hr']=df2.hashrate_sum/8640/1000
    df2['avg_hr2']=df2.hashrate_mean/1000
    df2.index = range(len(df2))

    # df = df.groupby('dt').agg({'hashrate':np.sum, 'dt':np.max})
    # df_date_time = df.dt.apply(datetime.fromtimestamp)
    # df['dayz'] = df_date_time.apply(datetime.date)
    # df = df[(df.dayz>=first_date.date()) & (df.dayz<=last_date.date())]
    # df2 = df.groupby('dayz').agg({'hashrate':[np.sum, np.mean], 'dt':np.size})
    # df2.columns = ["_".join(x) for x in df2.columns.ravel()]
    # df2['avg_hr']=df2.hashrate_sum/8640/1000
    # df2['avg_hr2']=df2.hashrate_mean/1000
    # df2.index = range(len(df2))

    sl = [f'{a:.4f}-{b:.4f}' for a,b in downtimes]
    dwn_str = '[' + '; '.join(sl) + ']'

    rez = []
    for idx in df2.index:
        # Если вдруг случится блекаут с переходом через полночь - считать будет неправильно
        rez.append((first_date + idx*one_day, df2.avg_hr[idx], df2.avg_hr2[idx], df2.dt_size[idx], dwn_str))
    print(rez)
    return rez


first_date = get_new_reports_start_date()
dt = datetime.now()
last_date = datetime(dt.year, dt.month, dt.day)
# hashrate_stat.to_csv('tmp.csv')
# hashrate_stat = pd.read_csv('tmp.csv')
blackouts = get_blackouts()
dayz = (last_date - first_date).days
daily_hashrates = []

stats = farm_stat_API.api_gimme_farm_stat(first_date, last_date)

# for d in range(0, dayz, 2):
#     print(f'{d}/{dayz}')
#     d1 = first_date + timedelta(days=d)
#     d2 = first_date + timedelta(days=d+2)
#     stat = farm_stat_API.api_gimme_farm_stat(d1, d2)
#     if stat is not None:
#         stats += stat

for d in range(dayz):
    d1 = first_date + timedelta(days=d)
    daily_hashrates += evalute_daily_hashrate_from_pandas_stats(d1, stats)

f = open(dr_filename, 'a')
lines = []
def any_to_str(x):
    if isinstance(x, float): return f'{x:.3f}'
    return str(x)

for dh in daily_hashrates:
    lines += ';'.join(map(any_to_str, dh))+'\n'
f.writelines(lines)
f.close()
