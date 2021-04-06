import zipfile
import json
import os
from datetime import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
import csv

columns = {'dt':'datetime', 'worker':'', 'uptime':'int', 'hashrate':'int', 'shares_good':'int', 'shares_bad':'int', 'pool_switches':'int','power':'int'}
gpu_templ = ['%d_busnum', '%d_hashrate', '%d_t', '%d_fan', '%d_shares_good', '%d_shares_bad']
for gpu in range(8):
    for col in gpu_templ:
        columns[col%gpu] = 'int'
columns['uptime_id'] = 'int'

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

class Zip_stat_request:
    def __init__(self, start=None, stop=None, effective_only=True, logs_dir=''):
        config = {}
        self.start = config.get('Start', start)
        self.stop = config.get('Stop', stop)
        self.logs_dir = config.get('LogsDir', logs_dir)
        self.effective_only = config.get('EffectiveOnly', effective_only)
        self.stat_zip_files = []
        self.raw_stats = None
        self.uncurled_stat = None
        self.raw_configs = None
        self.gpus_setting = None
        self.gpus = None
        self.pandas_stats = None

    def get_raw_stats(self):
        if not self.raw_stats:
            if not self.stat_zip_files:
                self.stat_zip_files = self.retrieve_matched_zip_files(self.stat_folder_filter, self.stat_files_filter)
            self.raw_stats = self.extract_raw_stats_from_zip_files(self.stat_zip_files)
        return self.raw_stats

    def get_pandas_stats(self):
        if not self.pandas_stats:
            if not self.stat_zip_files:
                self.stat_zip_files = self.retrieve_matched_zip_files(self.stat_folder_filter, self.stat_files_filter)
            self.pandas_stats = self.extract_pandas_stats_from_zip_files(self.stat_zip_files)
        return self.pandas_stats

    def get_uncurled_stat(self):
        if not self.uncurled_stat:
            raw_stats = self.get_raw_stats()
            self.uncurled_stat = uncurl_all_stat(raw_stats)
        return self.uncurled_stat

    def get_uncurled_pandas_stat(self):
        if not self.pandas_stats:
            self.pandas_stats = self.get_pandas_stats()
        return self.pandas_stats

    def get_total_hashrate_pitch(self, stat_item):
        hr = 0
        for rig, pitch in stat_item.items():
            if pitch[idx_hashrate]:
                hr += pitch[idx_hashrate]
        return hr

    def get_total_hashrate(self):
        rez = {}
        uncurled_stat = self.get_uncurled_stat()
        for tstamp, stat_item in uncurled_stat.items():
            tot_hr = self.get_total_hashrate_pitch(stat_item)
            rez[tstamp] = tot_hr
        return rez

    def get_raw_configs(self):
        if not self.raw_configs:
            config_zip_files = self.retrieve_matched_zip_files(self.config_folder_filter, self.config_files_filter)
            self.raw_configs = self.extract_raw_configs_from_zip_files(config_zip_files)
        return self.raw_configs

    def get_gpu_setting_in_config(self, config):
        gpus = []
        bns = []
        for sss in config:
            fl1 = sss.find('GPU') >= 0
            fl2 = sss.find('BN0') >= 0
            if not (fl1 and fl2):
                continue
            assert(sss[0] == '#')
            lst1 = sss[1:].split(', ')
            for pair in lst1:
                gpu, bn = None, None
                lst2 = pair.split(' ')
                for item in lst2:
                    if item[:3] == 'GPU':
                        gpu = item
                    if item[:2] == 'BN':
                        bn = item
                if bn == 'BN0A':
                    bn = 'BN10'
                gpus.append(gpu)
                bns.append(bn)

        return dict(zip(bns, gpus))

    def get_all_gpus_settings(self):
        if not self.gpus_setting:
            gpus_setting = {}
            raw_configs = self.get_raw_configs()
            for dt in raw_configs:
                snapshot = {}
                for mnr in raw_configs[dt]:
                    config = raw_configs[dt][mnr]
                    setting = self.get_gpu_setting_in_config(config)
                    snapshot[mnr] = setting
                gpus_setting[dt] = snapshot
            self.gpus_setting = gpus_setting
        return self.gpus_setting

    def get_gpu_trajectory(self, gpu):
        return self.get_all_gpus_settings()

    def get_gpu_hashrate(self, gpu):
        uncurled_stat = self.get_uncurled_stat()
        return 0

    def folder_date_if_fit(self, folder):
        dt_st, dt_end = expand_interval(self.start, self.stop)
        return folder_date_is_in_interval(folder, dt_st, dt_end)

    def retrieve_matched_zip_files(self, folders_filter, files_filter):
        rez = []
        tree = os.walk(self.logs_dir)
        for folder, subfolders, files in tree:
            fld = repair_folder_name(folder)
            if not folders_filter(fld):
                continue
            for fname in files:
                if not files_filter(fname):
                    continue
                rezname = '%s/%s' % (fld, fname)
                rez.append(rezname)
        return rez

    def extract_date_matched_stats_from_zipfile(self, zipname):
        file_stat = {}

        z = zipfile.ZipFile(zipname, 'r')
        zinfo = z.infolist()
        for item in zinfo:
            dt = self.get_datetime_from_statfilename(item.filename)
            if not (self.start <= dt <= self.stop):
                continue
            sss = z.read(item)
            stat_item = json.loads(sss)
            uncurl_stat_item(stat_item, file_stat)
        return file_stat

    def extract_date_matched_stats_from_zipfile_pandas(self, zipname):
        # df_main = pd.DataFrame(columns=columns)
        # df = df.set_index('dt')

        z = zipfile.ZipFile(zipname, 'r')
        zinfo = z.infolist()
        n = len(zinfo)
        mx_lst = []
        for i, item in enumerate(zinfo):
            # if i==50:
            #     break
            dt = self.get_datetime_from_statfilename(item.filename)
            if dt is None:
                continue
            if not (self.start <= dt <= self.stop):
                continue
            if i%20==0:
                print(f'     {i}/{n}')
            sss = z.read(item)
            try:
                stat_item = json.loads(sss)
            except Exception as e:
                print(e, sss)
                continue
            mx = put_stat_item_to_dataframe(stat_item)
            mx_lst += [mx]
        if not mx_lst:
            return
        full_mx = np.concatenate(mx_lst)
        return pd.DataFrame(full_mx, columns=columns)


    def extract_date_matched_configs_from_zipfile(self, zipname, zip_item_filter):
        configs = {}
        dt = self.get_datetime_from_statfilename(zipname)
        if not (self.start <= dt <= self.stop):
            return configs
        z = zipfile.ZipFile(zipname, 'r')
        zinfo = z.infolist()
        for item in zinfo:
            path, fn = os.path.split(item.filename)
            if not zip_item_filter(fn):
                continue
            path, last_dir = os.path.split(path)
            if not last_dir[:5] == 'Miner':
                continue
            key = 'M'+last_dir[-2:]
            if len(fn) == len('config*.txt'):
                key = key+fn[6]
            sss = z.read(item)
            try:
                sss = sss.decode('windows-1251')
            except:
                print(key)
            lines = sss.split('\r\n')
            configs[key] = lines

        return configs

    def extract_raw_stats_from_zip_files(self, zipfile_names):
        raw_stats = {}
        for zipname in zipfile_names:
            try:
                file_stat = self.extract_date_matched_stats_from_zipfile(zipname)
                if not file_stat:
                    continue
                for k in file_stat:
                    item = raw_stats.get(k, [])
                    item.append(file_stat[k])
                    raw_stats[k] = item
            except Exception as e:
                print(e, zipname)
                raise e
        return raw_stats

    def extract_pandas_stats_from_zip_files(self, zipfile_names):
        raw_stats = []
        n = len(zipfile_names)
        rez = None
        for i, zipname in enumerate(zipfile_names):
            try:
                print(f'{zipname}, {i}/{n}')
                file_stat = self.extract_date_matched_stats_from_zipfile_pandas(zipname)
                if file_stat is None:
                    continue
                raw_stats += [file_stat]
            except Exception as e:
                print(e, zipname)
                raise e
        try:
            rez = [pd.concat(raw_stats)]
        except Exception as e:
            print(e)
        return rez

    def extract_raw_configs_from_zip_files(self, zipfile_names):
        raw_configs = {}
        for zipname in zipfile_names:
            dt = self.get_datetime_from_statfilename(zipname)
            file_configs = self.extract_date_matched_configs_from_zipfile(zipname, lambda fn: fn[:6] == 'config' and fn[-3:]=='txt')
            if file_configs:
                raw_configs[dt] = file_configs
        return raw_configs

    def get_datetime_from_statfilename(self, filename):
        path, fname = os.path.split(filename)
        name, ext = os.path.splitext(fname)
        rez = None
        try:
            rez = datetime.strptime(name, '%Y-%m-%d %H-%M-%S')
        except:
            print(f'Bad filename, expects %Y-%m-%d %H-%M-%S, got {name}. With path {filename}')
        return rez

    def stat_folder_filter(self, folder):
        path, last_folder = os.path.split(folder)
        rez = last_folder.upper() == 'STAT'
        rez = rez and self.folder_date_if_fit(folder)
        return rez

    def stat_files_filter(self, filename):
        return filename.upper() == 'STAT.ZIP'

    def config_files_filter(self, filename):
        name, ext = os.path.splitext(filename)
        if ext != '.zip':
            return False
        try:
            rez = self.get_datetime_from_statfilename(filename)
            return True
        except:
            return False

    def config_folder_filter(self, folder):
        path, last_folder = os.path.split(folder)
        rez = last_folder == 'Configs'
        rez = rez and self.folder_date_if_fit(folder)
        return rez

    def check_gpus_history(self):
        rez = {}
        history = self.get_all_gpus_settings()
        for dt, snapshot in history.items():
            gpu_adresses, dups = self.build_gpu_adresses_table_from_snapshot(snapshot)
            if dups:
                rez[dt] = dups
        return rez

    def get_gpus(self):
        if not self.gpus:
            gpus = {}
            f = open('GPU.csv', 'r')
            for r in f.readlines():
                if r == 'gpuId,Num,Rig,BN,Location,Status,Class\n':
                    continue
                row = r[:-1].split(',')
                gpuId, Num, Rig, BN, Location, Status, Class = tuple(row)
                Num = int(Num)
                Rig = int(Rig) if Rig else None
                BN = int(BN) if BN else None
                ttt = zip(('Num', 'Rig', 'BN', 'Location', 'Status', 'Class'), (Num, Rig, BN, Location, Status, Class))
                gpus[gpuId] = dict(ttt)
            f.close()
            self.gpus = gpus
        return self.gpus

    def build_gpu_adresses_table_from_snapshot(self, snapshot):
        rez = {}
        dups = []
        for rig, setting in snapshot.items():
            if not setting:
                continue
            for bn, gpu in setting.items():
                if gpu in rez:
                    dups.append(dict(GPU=gpu, Rig1=rez[gpu]['Rig'], BN1=rez[gpu]['BN'], Rig2=rig, BN2=bn))
                    continue
                rez[gpu] = dict(Rig=rig, BN=bn)
        return rez, dups


    def check_gpus_snapshot(self, snapshot):
        return True


def uncurl_all_stat(all_stat):
    rez = {}
    i = 0
    for tstamp in all_stat:
        i+=1
        if i%1000 == 0:
            print(datetime.now(), i, len(all_stat))
        pitch_list = all_stat[tstamp][0]

        pitch_list_dict = uncurl_pitch_list(pitch_list)
        rez[tstamp] = pitch_list_dict
    return rez

def folder_to_date(folder):
    rez = None
    try:
        rez = datetime.strptime(folder, '%Y-%m-%d')
    except ValueError as e:
        pass
    except Exception as e:
        print(type(e), e)
    return rez

def get_folder_date(folder):
    path, last_folder = os.path.split(folder)
    path, date_level = os.path.split(path)
    date = folder_to_date(date_level)
    return date

def trunc_date(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def folder_date_is_in_interval(folder, st_date, end_date):
    folder_date = get_folder_date(folder)
    if folder_date is None:
        print('There is not folder date', folder)
        return False
    rez = st_date <= folder_date <= end_date
    return rez

def repair_folder_name(folder):
    good_slash = '/'
    bad_slash = '\\'
    return folder.replace(bad_slash, good_slash)

def make_workers_dict():
    rez = {}
    for r in range(75):
        rs = 'M%02d'%r
        rez[rs] = r
        for i, sfx in enumerate('ABCDEFGH'):
            key = rs + sfx
            rez[key] = 100*r+i
    return rez
workers_dict = make_workers_dict()

def expand_interval(dt1, dt2):
    dt_low = trunc_date(dt1) - timedelta(days=1)
    dt_high = trunc_date(dt2) + timedelta(days=2)
    return dt_low, dt_high

def uncurl_stat_item(item, all_stat):
    for key in item:
        dt = datetime.strptime(key, '%Y-%m-%d %H-%M-%S')
        if not dt in all_stat:
            all_stat[dt] = []
        info = all_stat[dt]
        t_item = item[key]
        for it1 in t_item:
            info += it1

idx_ts = 0
idx_worker = 1
idx_uptime = 2
idx_hashrate = 3
idx_shares_good = 4
idx_shares_bad = 5
idx_power = 7
idx_pool_switches = 6
base_idxs = np.array((8, 14, 20, 26, 32, 38, 44, 50),dtype=int)

np_bus_idx = np.array((1,2,3,5,6,7,8,10),dtype=int)
np_all_zeros = np.zeros(8,dtype=int)
zero_row = np.zeros(57, dtype=int)
mx_templ = np.zeros((6,8),dtype=int)

def make_row(pitch):
    mx = mx_templ.copy()
    row = zero_row.copy()
    try:
        row[idx_uptime] = int(pitch[1])
        lst = pitch[2].split(';')
        row[idx_hashrate] = int(lst[0])
        row[idx_shares_good] = int(lst[1])
        row[idx_shares_bad] = int(lst[2])
        row[idx_power] = int(pitch[17])
        lst = pitch[8].split(';')
        invalid, swtchs = tuple(map(int, lst[0:2]))
        row[idx_shares_bad] += invalid
        row[idx_pool_switches] = swtchs

        sss = pitch[3].replace('off', '-100').replace('stopped', '-500')
        lst = sss.split(';')
        gpu_hrs_lst = list(map(int, lst))
        gpu_cnt = len(lst)
        lst = pitch[6].split(';')
        gpu_t_lst =  list(map(int, lst[0::2]))
        gpu_fan_lst = list(map(int, lst[1::2]))
        lst = pitch[9].split(';')
        gpu_eth_acc_shares = list(map(int, lst))
        if pitch[15] == '1;2;3;5;6;7;8;10':
            gpu_bus_idxs = np_bus_idx.copy()
        else:
            lst = pitch[15].split(';')
            gpu_bus_idxs = np.array(list(map(int, lst)))

        if pitch[10] == '0;0;0;0;0;0;0;0':
            gpu_bad1 = np_all_zeros.copy()
        else:
            lst = pitch[10].split(';')
            gpu_bad1 = list(map(int, lst))

        if pitch[11] == '0;0;0;0;0;0;0;0':
            gpu_bad2 = np_all_zeros.copy()
        else:
            lst = pitch[11].split(';')
            gpu_bad2 = list(map(int, lst))

        # gpu_templ = ['%d_busnum', '%d_hashrate', '%d_t', '%d_fan', '%d_shares_good', '%d_shares_bad']
        mx[:,:gpu_cnt] = [gpu_bus_idxs, gpu_hrs_lst, gpu_t_lst,gpu_fan_lst,gpu_eth_acc_shares,gpu_bad1]
        mx[-1,:gpu_cnt] += gpu_bad2
        row[8:56] = mx.T.ravel()
        pass
    except:
#        print('Bad pitch:', pitch)
        pass
    return row


def put_stat_item_to_dataframe(stat_item):
    # keys = []
    # for i in range(8):
    #     keys += list(map(lambda s:s%i, gpu_templ))
    height = 0
    for dt_key, snapshot in stat_item.items():
        for snapshot_chunk in snapshot:
            height += len(snapshot_chunk)

    mx = np.zeros((height, 57), dtype=np.int32)
    height = 0
    for dt_key, snapshot in stat_item.items():
        dt = datetime.strptime(dt_key, '%Y-%m-%d %H-%M-%S')
        uptime_id = -1
        h_start = height
        for snapshot_chunk in snapshot:
            for rig_pitch in snapshot_chunk:
# columns = {'dt':'datetime', 'worker':'', 'uptime':'int', 'hashrate':'int', 'shares_good':'int', 'shares_bad':'int', 'pool_switches':'int','power':'int'}
                if rig_pitch[0] == 'Meta':
                    uptime_id = rig_pitch[1]['uptime_id']
                    continue
                if rig_pitch[1] is None:
                    mx[height, idx_ts] = int(dt.timestamp())
                    mx[height, idx_worker] = workers_dict[rig_pitch[0]]
                else:
                    pitch = rig_pitch[1]['result']
                    row = make_row(pitch)
                    row[idx_ts] = int(dt.timestamp())
                    row[idx_worker] = workers_dict[rig_pitch[0]]
                    mx[height] = row
                height += 1
        mx[h_start:height, 56] = uptime_id
    return mx


def up1(rez, pitch_dict):
    rez['ver'] = pitch_dict['ver']
    rez['uptime'] = pitch_dict['uptime_minutes']
    lst = pitch_dict['worker_hashrate'].split(';')
    rez['hashrate'] = int(lst[0])
    rez['shares'] = int(lst[1])
    rez['rejected'] = int(lst[2])

def up2(rez, pitch_dict):
    sss = pitch_dict['gpu_hashrates'].replace('off', '-100').replace('stopped','-500')
    lst = sss.split(';')
    rez['gpu_hashrates'] = list(map(int, lst))

def up3(rez, pitch_dict):
    lst = pitch_dict['gpu_t_and_fans'].split(';')
    rez['gpu_t'] = list(map(int, lst[0::2]))
    rez['gpu_fan'] = list(map(int, lst[1::2]))

    rez['pool'] = pitch_dict['pool']

    rez['gpu_shares'] = list(map(int, pitch_dict['gpu_shares'].split(';')))
    rez['gpu_rej'] = list(map(int, pitch_dict['shares_rej'].split(';')))
    rez['gpu_inv'] = list(map(int, pitch_dict['shares_inv'].split(';')))

    rez['gpu_bus_indexes'] = list(map(int, pitch_dict['gpu_bus_indexes'].split(';')))


def uncurl_pitch(pitch_dict):
    rez = [None]*13
    if not pitch_dict:
        return rez

    rez[0] = pitch_dict['ver']
    rez[1] = pitch_dict['uptime_minutes']
    lst = pitch_dict['worker_hashrate'].split(';')
    rez[2] = int(lst[0])
    rez[3] = int(lst[1])
    rez[4] = int(lst[2])

    sss = pitch_dict['gpu_hashrates'].replace('off', '-100').replace('stopped', '-500')
    lst = sss.split(';')
    rez[5] = tuple(map(int, lst))

    lst = pitch_dict['gpu_t_and_fans'].split(';')
    try:
        rez[6] = tuple(map(int, lst[0::2]))
        rez[7] = tuple(map(int, lst[1::2]))
    except:
        print('invalid pitch: ', pitch_dict)

    rez[8] = pitch_dict['pool']

    rez[9] = tuple(map(int, pitch_dict['gpu_shares'].split(';')))
    rez[10] = tuple(map(int, pitch_dict['shares_rej'].split(';')))
    rez[11] = tuple(map(int, pitch_dict['shares_inv'].split(';')))

    rez[12] = tuple(map(int, pitch_dict['gpu_bus_indexes'].split(';')))
    return tuple(rez)


def uncurl_pitch_list(pitch_list):
    keys = ('ver', 'uptime_minutes', 'worker_hashrate', 'gpu_hashrates', '????_DCR1', '????_DCR2','gpu_t_and_fans', 'pool', 'rig_shares', 'gpu_shares', 'shares_rej', 'shares_inv',
            '????_DCR3', '????_DCR4', '????_DCR5', 'gpu_bus_indexes', 'shares_accept_time_ms', 'gpus_power')
    rez = {}
    for pitch_item in pitch_list:
        name = pitch_item[0]
        dct = pitch_item[1]
        pitch_dict = {}
        if dct:
            pitch = dct.get('result', [])
            if len(pitch) >= 1:
                kv = zip(keys, pitch)
                pitch_dict = dict(kv)
        normal_pitch = uncurl_pitch(pitch_dict)
        rez[name] = normal_pitch
    return rez


# ------------------------------------ API ENTRIES ------------------------------

logs_dir='e:\sandbox\logs'
# logs_dir=r'd:\farmlogs'

def gimme_farm_hashrate(start, stop, effective_only=True):
    request = Zip_stat_request(start, stop, logs_dir=logs_dir, effective_only=effective_only)
    rez = request.get_total_hashrate()
    return rez

def gimme_farm_stat(start, stop):
    request = Zip_stat_request(start, stop, logs_dir=logs_dir)
    rez = request.get_uncurled_pandas_stat()
    return rez

def gimme_rig_hashrate(rig, start, stop, effective_only=True):
    pass

def gimme_rig_stat(rig, start, stop):
    pass

def gimme_gpu_hashrate(gpu, start, stop, effective_only=True):
    pass

def gimme_gpu_stat(gpu, start, stop):
    request = Zip_stat_request(start, stop, logs_dir=logs_dir)
    rez = request.get_gpu_trajectory(gpu)
    if request.check_gpus_history():
        print('Oops')
    return rez

def gimme_user_hashrate(user, start, stop, effective_only=True):
    pass

def gimme_user_stat(user, start, stop):
    pass
