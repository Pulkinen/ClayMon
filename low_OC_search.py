import zipfile
import json
import os
from datetime import datetime
from datetime import timedelta

global_config = None

def string_to_date(dtstr):
    return datetime.strptime(dtstr, '%Y-%m-%d %H:%M:%S')

def statname_to_date(dtstr):
    return datetime.strptime(dtstr, '%Y-%m-%d %H-%M-%S')


def replace_dtstr_to_date(struc, key):
    try:
        struc[key] = string_to_date(struc[key])
    except Exception as e:
        print(e)

def folder_to_date(folder):
    rez = None
    try:
        rez = datetime.strptime(folder, '%Y-%m-%d')
    except ValueError as e:
        pass
    except Exception as e:
        print(type(e), e)
    return rez

def get_config(config_fname=''):
    global global_config
    if config_fname:
        f = open(config_fname, 'r')
        strs = f.read()
        config = json.loads(strs)
        replace_dtstr_to_date(config, 'Start')
        replace_dtstr_to_date(config, 'Stop')
        global_config = config
    return global_config

def folder_contains_stat(folder):
    config = get_config()
    path, last_folder = os.path.split(folder)
    return last_folder == config['StatDir']

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
    # print(rez, folder, folder_date, st_date, end_date)
    return rez

def repair_folder_name(folder):
    good_slash = '/'
    bad_slash = '\\'
    return folder.replace(bad_slash, good_slash)


def expand_interval(dt1, dt2):
    dt_low = trunc_date(dt1) - timedelta(days=1)
    dt_high = trunc_date(dt2) + timedelta(days=2)
    # print(dt1, dt2, '-->', dt_low, dt_high)
    return dt_low, dt_high

def folder_date_if_fit(folder):
    config = get_config()
    dt_st, dt_end = expand_interval(config['Start'], config['Stop'])
    return folder_date_is_in_interval(folder, dt_st, dt_end)


def get_stat_zips_in_interval():
    config = get_config()
    tree = os.walk(config['LogsDir'])
    rez = []
    for folder, subfolders, files in tree:
        if not folder_contains_stat:
            continue
        if not folder_date_if_fit(folder):
            continue
        if not config["StatZipName"] in files:
            continue
        fld = repair_folder_name(folder)
        print(folder)
        zipname = '%s/%s'%(fld, config["StatZipName"])
        # print(zipname)
        rez.append(zipname)
    return rez

def get_datetime_from_statfilename(filename):
    path, fname = os.path.split(filename)
    name, ext = os.path.splitext(fname)
    rez = statname_to_date(name)
    return rez

def uncurl_stat_item(item, all_stat):
    for dt in item:
        if not dt in all_stat:
            all_stat[dt] = []
        info = all_stat[dt]
        t_item = item[dt]
        for it1 in t_item:
            info += it1

def extract_date_matched_stats_from_zipfile(zipname):
    all_stat = {}
    z = zipfile.ZipFile(zipname, 'r')
    config = get_config()
    st_date = config['Start']
    end_date = config['Stop']
    zinfo = z.infolist()
    for item in zinfo:
        dt = get_datetime_from_statfilename(item.filename)
        if not(st_date <= dt <= end_date):
            continue
        print(item.filename)
        sss = z.read(item)
        stat_item = json.loads(sss)
        uncurl_stat_item(stat_item, all_stat)
    return all_stat

def extract_stats_from_zip_files(zipfile_names):
    all_stat = {}
    for zipname in zipfile_names:
        file_stat = extract_date_matched_stats_from_zipfile(zipname)
        for k in file_stat:
            item = all_stat.get(k, [])
            item.append(file_stat[k])
            all_stat[k] = item
        print(zipname)
    uncurled_stat = uncurl_all_stat(all_stat)
    return all_stat

def uncurl_pitch(pitch_item):
    keys = ('ver', 'uptime_minutes', 'worker_hashrate', 'gpu_hashrates', '????_DCR1', '????_DCR2','gpu_t_and_fans', 'pool', 'rig_shares', 'gpu_shares', 'shares_rej', 'shares_inv',
            '????_DCR3', '????_DCR4', '???_DCR5', 'gpu_bus_indexes', 'shares_accept_time_ms', 'gpus_power')
    rez = {}
    name = pitch_item[0][0]
    dct = pitch_item[0][1]
    pitch = dct.get('result', [])
    pitch_dict = {}
    if len(pitch) >= 1:
        kv = zip(keys, pitch)
        pitch_dict = dict(kv)

    rez[name] = pitch_dict
    return rez


def uncurl_all_stat(all_stat):
    rez = {}
    for tstamp in all_stat:
        item = all_stat[tstamp][0]
        pitch_dict = uncurl_pitch(item)
        rez.update(pitch_dict)



get_config('low_oc_config.json')
stat_zip_files = get_stat_zips_in_interval()
rez = extract_stats_from_zip_files(stat_zip_files)
