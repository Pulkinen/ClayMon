import zipstat
import datetime
from datetime import timedelta
import farm_stat_API
import numpy as np
import pandas as pd
import os
import filecmp
import zipfile
import farm_stat_utils as utils
import itertools
from functools import partial

def find_gpu_dupes_in_configs(start, stop, logs_dir):
    request = zipstat.Zip_stat_request(start, stop, logs_dir=logs_dir)
    dups = request.check_gpus_history()
    for item in dups:
        print(item)

def find_diffs_between_config_and_stat(start, stop, logs_dir):
    request = zipstat.Zip_stat_request(start, stop, logs_dir=logs_dir)
    stat = request.get_uncurled_stat()
    gpu_setts = request.get_all_gpus_settings()
    dt_dict = match_dt_series(stat.keys(), gpu_setts.keys())
    for stat_dt, setts_dt in dt_dict.items():
        stat_snapshot = stat[stat_dt]
        setts_snapshot = gpu_setts[setts_dt]
        find_missing_gpus(setts_snapshot, stat_snapshot)

# The GPUs those are in configs, but missing in stat
def find_missing_gpus(config_snapshot, stat_snapshot):
    config_errors = {}
    if not assert_the_keys_set_are_the_same:
        assert(False)
    merged_config = merge_workers_config_gpu_setts(config_snapshot)

    for worker in stat_snapshot:
        rig = get_rig_by_worker(worker)
        config = merged_config[rig]
        stat = stat_snapshot[worker]
        if not stat:
            continue
        bns_idx = zipstat.idx_gpu_bus_indexes
        stat_bns = stat[bns_idx]
        if not stat_bns:
            continue

        for bn in stat_bns:
            bname = get_BN99_by_busnum(bn)
            if bname in config:
                config.pop(bname)
                continue
            if not rig in config_errors:
                config_errors[rig] = []
            config_errors[rig].append(bname)

    rez = {}
    for rig in merged_config:
        if merged_config[rig] or (rig in config_errors):
            rez[rig]={}
            rez[rig]['Found'] = config_errors.get(rig, None)
            rez[rig]['Lost'] = merged_config[rig]
    return rez

def get_BN99_by_busnum(bn):
    return 'BN%02d'%bn

def get_rig_by_worker(worker):
    widx = 'ABCDEFGH'.find(worker[-1])
    rig = worker
    if widx >= 0:
        rig = worker[:-1]
    return rig

def merge_workers_config_gpu_setts(config_snapshot):
    rez = {}
    for worker in config_snapshot:
        rig = get_rig_by_worker(worker)
        if not rig in rez:
            rez[rig] = {}
        bns = rez[rig]
        cfg = config_snapshot[worker]
        bns.update(cfg)
    return rez

def assert_the_keys_set_are_the_same(dict1, dict2):
    return True

def find_nearest_dt(found, series):
    best = None
    for dt in series:
        if not best:
            best = dt
            continue
        if dt - found < dt - best:
            best = dt
    return best

def match_dt_series(dt1, dt2):
    rez = {}
    for dt in dt1:
        rez[dt] = find_nearest_dt(dt, dt2)
    return rez

def get_period_wo_config_changes_and_server_restarts(first_date, last_date, logs_dir):
    fls = []
    for dt in (last_date, first_date):
        fldr = logs_dir + f'\{dt:%Y-%m-%d}\configs'
        tree = os.walk(fldr)
        for folder, subfolders, files in tree:
            fls += [f'{folder}\{fn}' for fn in files if os.path.splitext(fn)[1] == '.zip']
    fls.sort(reverse=True)
    fn_newest = fls[0]
    fn_oldest = fn_newest
    for i, filename in enumerate(fls[1:]):
        if not filecmp.cmp(fn_newest, filename, shallow=False):
            break
        fn_oldest = filename
    path, fname = os.path.split(fn_oldest)
    fname, ext = os.path.splitext(fname)
    rez_oldest = datetime.datetime.strptime(fname, '%Y-%m-%d %H-%M-%S')

    path, fname = os.path.split(fn_newest)
    fname, ext = os.path.splitext(fname)
    rez_newest = datetime.datetime.strptime(fname, '%Y-%m-%d %H-%M-%S')

    return (rez_oldest, rez_newest, fn_newest)

def get_dt_from_config_filename(fn):
    path, fname = os.path.split(fn)
    fname, ext = os.path.splitext(fname)
    rez = datetime.datetime.strptime(fname, '%Y-%m-%d %H-%M-%S')
    return rez

def get_mid_time(dt1, dt2):
    tst1 = int(dt1.timestamp())
    tst2 = int(dt2.timestamp())
    tst_mid = (tst1+tst2)/2
    rez = datetime.datetime.fromtimestamp(tst_mid)
    return rez

def split_configs_period(date_range, logs_dir):
    fls = []
    for dt, dt2 in date_range:
        fldr = logs_dir + f'\{dt:%Y-%m-%d}\configs'
        tree = os.walk(fldr)
        for folder, subfolders, files in tree:
            fls += [f'{folder}\{fn}' for fn in files if os.path.splitext(fn)[1] == '.zip']
    fls.sort()
    n = len(fls)
    cmps = []
    for i in range(n-1):
        cmps += [filecmp.cmp(fls[i], fls[i+1], shallow=False)]

    tms = [get_dt_from_config_filename(fn) for fn in fls]
    mids = [get_mid_time(tms[i], tms[i+1]) for i in range(n-1) if not cmps[i]]
    brdrs =  sorted([tms[0]] + mids*2 + [tms[-1]])

    mids2 = [(i, i+1) for i in range(n-1) if not cmps[i]]
    idxs = sorted([0] + list(itertools.chain.from_iterable(mids2)) + [n-1])
    rez = [(brdrs[i], brdrs[i+1], fls[idxs[i]]) for i in range(0, len(brdrs), 2)]

    return rez


def found_invalid_shares(stats, threshold = 0.1):
    cols_dict = {col: np.max for col in stats[0].columns if col[1:] in ('_busnum', '_shares_good', '_shares_bad')}
    dfs = []
    for stat in stats:
        df = stat.groupby(['worker'], as_index = False).agg(cols_dict)
        for gpu in range(8):
            mask = (df.loc[:,f'{gpu}_busnum'] > 0) & (df.loc[:,f'{gpu}_shares_bad'] > 0)
            df2 = df[mask]
            df3 = df2[['worker']]
            df3.loc[:,'gpu'] = gpu
            df3.loc[:,'bn'] = df2[f'{gpu}_busnum']
            bad = df[f'{gpu}_shares_bad']
            good = (df[f'{gpu}_shares_good'])
            df3.loc[:,'ratio'] = bad / (bad + good)
            df3.loc[:,'shares_bad'] = df[f'{gpu}_shares_bad']
            df3.loc[:,'shares_good'] = df[f'{gpu}_shares_good']
            dfs += [df3]
    agg_df = pd.concat(dfs)
    agg_df = agg_df[agg_df.ratio > threshold]
    mask = agg_df['worker'] >= 100
    agg_df['worker'][mask] = agg_df['worker'][mask] // 100
    agg_df.sort_values(by = 'ratio', ascending = False, inplace = True)
    agg_df = agg_df.reset_index(drop=True)
    return agg_df


# def make_workers_dict():
#     rez = {}
#     for r in range(74):
#         rs = 'M%02d'%r
#         rez[rs] = r
#         for i, sfx in enumerate('ABCDEFGH'):
#             key = rs + sfx
#             rez[key] = 100*r+i
#     return rez
# workers_dict = make_workers_dict()

def convert_configs_snapshot_to_dataframe(configs_snapshot):
    tmp = []
    z = zipfile.ZipFile(configs_snapshot, 'r')
    zinfo = z.infolist()
    for item in zinfo:
        item_fn = item.filename
        path, fname = os.path.split(item_fn)
        path,rig = os.path.split(path)
        fname, ext = os.path.splitext(fname)
        fname = fname.lower()
        if fname[:6] != 'config':
            continue
        if rig[:5].lower() != 'miner':
            continue
        rig = int(rig[-2:])

        worker = rig
        if len(fname) == 7:
            worker = rig * 100 + ord(fname[6]) - ord('a')

        sss = z.read(item)
        sss = sss.decode('windows-1251')
        sss = sss.split('\r\n')
        gpu_bns = utils.get_gpu_list_from_config(sss)
        gpus = [-1]*8
        ewal = utils.get_wallet_from_config(sss)
        for i, (b, g) in enumerate(gpu_bns):
            gpus[i] = g
        tmp += [(rig, worker, ewal, *gpus)]

    rez = pd.DataFrame(data=tmp, columns=['rig', 'worker', 'user', '0_gpu',  '1_gpu', '2_gpu', '3_gpu', '4_gpu', '5_gpu', '6_gpu', '7_gpu',])
    return rez

def check_configs(conf_df):
    return []
    # df = conf_df.groupby('GPU').agg('count')
    # df = df[df.BN > 1]
    # # df = df[(df.index < 400) | (df.index >= 500)]
    # rez = df.index.values
    # return rez

def estimate_gpus(configs_snapshot, stat_dfs):
    pass


def found_troubles(stats, period):
    first_date, last_date = period
    ts1 = int(first_date.timestamp())
    ts2 = int(last_date.timestamp())
    fstats = []
    for stat in stats:
        if stat is None:
            continue
        mask = (stat.dt >= ts1) & (stat.dt < ts2)
        fstat = stat[mask]
        fstats += [fstat]
    invalid_shares_df = found_invalid_shares(fstats)
    print(invalid_shares_df)

def gimme_stat_file(first_date, last_date):
    dayz = (last_date - first_date).days
    for d in range(dayz):
        d1 = first_date + timedelta(days=d)
        rez_file = f'{farm_stat_API.cache_folder}\{d1:%Y-%m-%d}.csv'
        if os.path.exists(rez_file):
            return rez_file

def make_days_range(first_day, last_day):
    dt = first_day
    start = datetime.datetime(dt.year, dt.month, dt.day)
    dt = last_day
    finish = datetime.datetime(dt.year, dt.month, dt.day)
    dayz = (finish - start).days + 1
    rez = [(start + timedelta(days=i),start + timedelta(days=i+1)) for i in range(dayz)]
    return rez

def is_periods_intersects(per1, per2):
    rez = not(per1[0] > per2[1] or per1[1] < per2[0])
    return rez

stat_cache = {}
config_cache = {}

def gimme_dataframes(stat_period, config_period):
    if not stat_period in stat_cache:
        stat_df = pd.read_csv(gimme_stat_file(*stat_period), index_col=0)
        stat_cache[stat_period] = stat_df
    if not config_period in config_cache:
        config_filename = config_period[2]
        conf_df = convert_configs_snapshot_to_dataframe(config_filename)
        config_cache[config_period] = conf_df
    return stat_cache[stat_period], config_cache[config_period]

def split_stats_chunk_by_uptime_id(df_stat):
    # rez = list(df_stat.groupby('uptime_id'))
    uids = df_stat.uptime_id.unique()
    rez = []
    for uid in uids:
        rez += [df_stat[df_stat.uptime_id == uid]]
    return rez

def fix_mismatch_workers(df_a, df_b):
    wrks_a = set(df_a.worker.unique())
    wrks_b = set(df_b.worker.unique())
    mismatch_wrks = wrks_a-wrks_b
    reps = {}
    for w in mismatch_wrks:
        if w > 99:
            w2 = w // 100
            if w2 in wrks_b:
                reps[w] = w2
            else:
                assert False, f'Cannot find match to worker {w}'
        else:
            w2 = {w*100 + i for i in range(9)}
            w3 = w2 & wrks_b
            if w3 == set():
                assert False, f'Cannot find match to worker {w}'
            else:
                reps[w] = list(w3)[0]
    for k, v in reps.items():
        mask = df_a.worker == k
        df_a.loc[mask, 'worker'] = v
    return

def merge_stat_and_config(stat_period, config_period):
    stats_df, conf_df = gimme_dataframes(stat_period, config_period)
    # stats_df.to_csv(r'e:\tmp\123\a.csv')
    # conf_df.to_csv(r'e:\tmp\123\b.csv')
    ts1, ts2 = config_period[:2]
    mask =(stats_df.dt >= ts1.timestamp()) & (stats_df.dt <= ts2.timestamp())
    stats_df = stats_df[mask]
    stats = split_stats_chunk_by_uptime_id(stats_df)
    dfs = []
    for stat in stats:
        stat = stat[['dt', 'worker', 'hashrate', 'uptime_id']]
        conf_df = conf_df[['worker', 'user']].drop_duplicates()
        fix_mismatch_workers(stat, conf_df)
        df = pd.merge(stat, conf_df, on = "worker", how = 'left')
        assert df.worker.notna().all()
        assert df.user.notna().all()
        assert df.hashrate.notna().all()
        sum1 = stat.hashrate.sum()
        sum2 = df.hashrate.sum()
        if sum1 != sum2:
            print('!!!!!!!!!!', sum1, sum2)
            # stats_df.to_csv(r'e:\tmp\123\a.csv')
            # conf_df.to_csv(r'e:\tmp\123\b.csv')
            assert False

        dfs += [df]
    return dfs


def merge_stats_and_configs(first_day, last_day):
    logs_dir = farm_stat_API.logs_dir
    dr = make_days_range(first_day, last_day)
    config_periods = split_configs_period(dr, logs_dir)
    stat_periods = [dri for dri in dr if os.path.exists(f'{farm_stat_API.cache_folder}\{dri[0]:%Y-%m-%d}.csv')]
    pairs = [(s, c) for s,c in itertools.product(stat_periods, config_periods) if is_periods_intersects(s, c)]
    dfs = [merge_stat_and_config(*pair) for pair in pairs]
    rez = list(itertools.chain.from_iterable(dfs))
    return rez

def gimme_gpus_with_invalid_shares(stat_df, bad_rate_threshold = 0):
    prefix = ['dt', 'uptime', 'rig']
    gpu_cols = ['gpu', 'shares_good', 'shares_bad']
    dfs = []
    for i in range(8):
        cols = prefix + ['%d_'%i+s for s in gpu_cols]
        df = stat_df[cols]
        df.columns = prefix + gpu_cols
        df = df[df.gpu >= 0]
        dfs += [df]
    gpus_df = pd.concat(dfs, sort=False)
    # cols_dict = {col: np.max for col in stats[0].columns if col[1:] in ('_busnum', '_shares_good', '_shares_bad')}
    # .groupby(['worker'], as_index = False).agg(cols_dict)
    gpus_bad = gpus_df.groupby(['gpu', 'rig'], as_index = False).agg({'shares_good': np.max, 'shares_bad': np.max})
    gpus_bad = gpus_bad[gpus_bad.shares_bad > 0]
    gpus_bad['bad_rate'] = gpus_bad.shares_bad / (gpus_bad.shares_bad + gpus_bad.shares_good)
    gpus_very_bad = gpus_bad[gpus_bad.bad_rate > bad_rate_threshold]
    gpus_very_bad = gpus_very_bad.sort_values(by=['bad_rate'])
    gpus_very_bad = gpus_very_bad[['gpu', 'rig', 'bad_rate']]
    gpus_very_bad.index = range(len(gpus_very_bad))
    return gpus_very_bad


def evalute_gpu_classes_hashrate(df_cls, df_peaks):
    df_peaks = df_peaks[df_peaks.hashrate > 0]
    # df_cls = df_cls[df_cls.class_id >= 0]
    df_cls.gpu = df_cls.gpu.apply(lambda s: int(s[3:]))
    # print(df_cls.head)
    # print(df_peaks.head)
    df = pd.merge(df_cls, df_peaks, on = "gpu", how = 'inner')
    q60 = partial(np.quantile, q=0.6)
    df = df.groupby(['class_id'], as_index = False).agg({'hashrate': q60})
    # print(df.head)
    df_cls = pd.merge(df_cls, df, on = "class_id", how = 'left')
    df_cls.columns = list(df_cls.columns[:-1]) + ['class_hashrate']
    df_cls = pd.merge(df_cls, df_peaks, on = "gpu", how = 'left')
    print(df_cls.head)
    return df_cls



def evalute_gpus_peak_hashrate(stat_df):
    prefix = ['dt']
    gpu_cols = ['gpu', 'hashrate']
    dfs = []
    for i in range(8):
        cols = prefix + ['%d_'%i+s for s in gpu_cols]
        df = stat_df[cols]
        df.columns = prefix + gpu_cols
        dfs += [df]
    df = pd.concat(dfs, sort=False)
    df = df[df.gpu >= 0]
    q90 = partial(np.quantile, q=0.9)
    df = df.groupby(['gpu'], as_index = False).agg({'hashrate': q90})
    df = df.sort_values(by=['hashrate'],ascending=False)
    # df_test = df[(df.gpu >= 400) & (df.gpu < 500)]
    # print(df_test.head(50))
    return df


def estimate_user_contract_hashrate(gpu_class_df, peak_hashrate_df):
    print(gpu_class_df.head)
    # print(peak_hashrate_df.head)
    # df = pd.merge(gpu_class_df, peak_hashrate_df, on = "gpu", how = 'left')
    df = gpu_class_df.groupby(['user'], as_index = False).agg({'class_hashrate': sum, 'gpu':'count'})
    print(df.head)
    return df

nnn = 0
def estimate_user_produced_hashes(stat_df):
    df = stat_df[['dt', 'uptime_id']]
    if len(df.uptime_id.unique()) != 1:
        assert(False)
    dt = datetime.datetime.fromtimestamp(df.dt.min()).date()
    id = df.uptime_id.min()
    global nnn
    nnn += 1
    fn = f'{farm_stat_API.cache_folder}\produced {dt:%Y-%m-%d} {id} {nnn}.csv'

    timestep = (df.dt.max() - df.dt.min())/(len(df.dt.unique())-1)
    df = stat_df.groupby(['user'], as_index = False).agg({'hashrate':sum})
    df['MHs*day'] = round(df.hashrate * timestep / 86400)
    df = df.drop(columns=['hashrate'])
    # df.to_csv(fn)
    return df

def estimate_hashes_produces_due_contracts(eth_stat_df, contract_df):
    full_contract_hashrate = contract_df.contract_hr.sum()
    df = eth_stat_df
    df['losses'] = df.hashrate - full_contract_hashrate

def make_produced_table():
    first_date = datetime.datetime(2020, 9, 24)
    last_date =  datetime.datetime(2020, 10, 4)
    logs_dir = farm_stat_API.logs_dir
    dr = make_days_range(first_date, last_date)
    config_periods = split_configs_period(dr, logs_dir)
    stat_periods = [dri for dri in dr if os.path.exists(f'{farm_stat_API.cache_folder}\{dri[0]:%Y-%m-%d}.csv')]
    # stat_periods = [s for s in stat_periods if is_periods_intersects(s, (first_date, last_date))]
    pairs = [(s, c) for s,c in itertools.product(stat_periods, config_periods) if is_periods_intersects(s, c)]
    hps = []
    i = 0
    for pair in pairs:
        print(i, pair)
        stat_dfs = merge_stat_and_config(*pair)
        for stat_df in stat_dfs:
            i += 1
            hpdf = estimate_user_produced_hashes(stat_df)
            hpdf['num'] = i
            dt = pair[0][0]
            hpdf['dt'] = dt #datetime.datetime.strptime(dt, '%Y-%m-%d')
            # stat_df.to_csv(f'e:/tmp/123/{i}.csv')
            hps += [hpdf]
    rez = pd.concat(hps)
    rez.to_csv(f'{farm_stat_API.cache_folder}\hp {first_date:%Y-%m-%d} - {last_date:%Y-%m-%d}.csv')
    return None


make_produced_table()

dt = datetime.datetime.now()
# last_date = datetime.datetime(dt.year, dt.month, dt.day)
first_date = datetime.datetime(2020, 9, 12)
last_date = first_date + timedelta(days=3)
# first_date = last_date - timedelta(days=7)
# first_date = last_date - timedelta(days=2)

cache_fn = f'contract\peak_hr_tmp.csv'
if not os.path.exists(cache_fn):
    stat_dfs = merge_stats_and_configs(first_date, last_date)
    stat_dfs[0].to_csv(cache_fn)
df = pd.read_csv(cache_fn, index_col=0)
# inv_share_gpus = gimme_gpus_with_invalid_shares(df, bad_rate_threshold=0.2)
# print(inv_share_gpus.head(100))
peak_hashrate_df = evalute_gpus_peak_hashrate(df)
gpu_class_df = pd.read_csv('GPUs.csv', encoding='cp1258', delimiter=';')
# gpu_class_df = evalute_gpu_classes_hashrate(gpu_class_df, peak_hashrate_df)
# gpu_class_df.to_csv('GPUs_2.csv', encoding='cp1258')
# gpu_class_df = pd.read_csv('GPUs_2.csv', encoding='cp1258')
# estimate_use  r_contract_hashrate(gpu_class_df, peak_hashrate_df)
stat_dfs = merge_stats_and_configs(first_date, last_date)

eth_stat_fn = f'contract\eth_stat.csv'
eth_stat_df = pd.read_csv(eth_stat_fn, encoding='cp1258', delimiter=';', usecols=['date', 'farm_hr', 'diff', 'eth_usd', 'usd_rub', 'eth_rub', 'tx_fees', 'fee_keff', 'income_per_mhs', 'tech_cost', 'bonus_mhs'])

contract_fn = f'contract\contract.csv'
contract_df = pd.read_csv(contract_fn, encoding='cp1258', delimiter=',')


# periods = split_configs_period(first_date, last_date, logs_dir)
# for dt1, dt2, config_filename in periods:
#     conf_df = convert_configs_snapshot_to_dataframe(config_filename)
#     duped_gpus = check_configs(conf_df)
#     if len(duped_gpus) > 0:
#         print('There is some mistakes in configs!!! Some GPUs are duplicated, check it!')
#         print(duped_gpus)
#     conf_df = conf_df.drop(duped_gpus)
#     print(conf_df.head())
#     stats_df = pd.read_csv(gimme_stat_file(dt1, dt2))
#     print(stats_df.shape)
#     stats_df['rig'] = stats_df['worker'].apply(lambda wrk: wrk//100 if wrk >= 100 else wrk)
#     stats_df.drop(columns=['worker'])
#
#     stats_df['bn_code'] = stats_df['0_busnum'].astype('str') + stats_df['1_busnum'].astype('str') + stats_df['2_busnum'].astype('str') + stats_df['3_busnum'].astype('str') + stats_df['4_busnum'].astype('str') + stats_df['5_busnum'].astype('str') + stats_df['6_busnum'].astype('str') + stats_df['7_busnum'].astype('str')
#     ttt =  stats_df.groupby(['bn_code']).agg('count')
#     print(ttt)
#     # cols = ['0_busnum', '1_busnum']
#     # stats_df['bn_code'] = sum((stats_df[col].astype('str') for col in cols), '')
#
#     gpu_templ = ['%d_busnum', '%d_hashrate', '%d_t', '%d_fan', '%d_shares_good', '%d_shares_bad']
#     dfs = []
#     for i in range(8):
#         cols = ['dt','rig'] + [s%i for s in gpu_templ]
#         df = stats_df[cols]
#         df.columns = ['dt', 'user', 'busnum', 'hashrate', 't', 'fan', 'shares_good', 'shares_bad']
#         dfs += [df]
#     gpus_df = pd.concat(dfs, sort=False)
#
#     print(gpus_df.shape)
#     print(gpus_df.head())


# found_troubles(stats, (good_period_start, good_period_end))
# stats_df = pd.merge(stats_df, conf_df, on = "")
