import json
import os
import zipfile
from datetime import datetime, timedelta
from shutil import copyfile
import time

last_proceed_files = []

def zip_whole_dir(config):
    src_dir = config['SrcDir']
    dst_dir = config['DstDir']
    if '%date' in dst_dir:
        dst_dir = dst_dir.replace('%date', '%s')
        date_str = datetime.strftime(datetime.now(), "%Y-%m-%d")
        dst_dir = dst_dir % date_str
        print(dst_dir)
    try:
        if not os.path.isdir(dst_dir):
            os.mkdir(dst_dir)
    except Exception as e:
        print('Exception:', e)
    zipname = dst_dir + datetime.strftime(datetime.now(), "%Y-%m-%d %H-%M-%S")
    z = zipfile.ZipFile('%s.zip' % zipname, mode='a', compression=zipfile.ZIP_LZMA)

    tree = os.walk(src_dir)
    for folder, subfolders, files in tree:
        try:
            print(folder, subfolders, files)
            for fn in files:
                src_fname = '%s/%s'%(folder, fn)
                z.write(src_fname)
        except Exception as e:
            print('Exception:', e)
    z.close()
    proceed_dst_files = ['%s.zip' % zipname]
    rez = dict(zip(proceed_dst_files, [None]*len(proceed_dst_files)))
    return(list(rez.keys()))

def replicate_item(config):
    accum_src_size = 0
    accum_zip_size = 0
    start_time = datetime.now()
    global last_proceed_files
    proceed_dst_files = []
    src_dir = config['SrcDir']
    dst_dir = config['DstDir']
    src_len = len(src_dir)
    if not os.path.isdir(dst_dir):
        os.mkdir(dst_dir)
    if config["DateLevel"]:
        dst_dir = dst_dir + datetime.strftime(datetime.now(), "%Y-%m-%d") + '/'
        print(dst_dir)
    if not os.path.isdir(dst_dir):
        os.mkdir(dst_dir)
    if ("SrcFilter" in config) and (config["SrcFilter"] == "Last proceed files"):
        print('Start copying last files')
        print(last_proceed_files)

    tree = os.walk(src_dir)
    for folder, subfolders, files in tree:
        try:
            print(folder, subfolders, files)
            if config['CopyDirsStruct']:
                rel = folder[src_len:]
                for sf in subfolders:
                    if not os.path.isdir(dst_dir+rel+'/'+sf):
                        os.mkdir(dst_dir+rel+'/'+sf)
            else:
                rel = ''
            for fn in files:
                src_fname = '%s/%s'%(folder, fn)
                src_check = src_fname.replace('\\', '/')
                if ("SrcFilter" in config) and (config["SrcFilter"] == "Last proceed files"):
                    if not src_check in last_proceed_files:
                        print('Skip file', src_fname)
                        continue
                fname = dst_dir+rel+'/'+fn
                src_stats = os.stat(src_fname)
                accum_src_size += src_stats.st_size
                print(src_fname, '--->',dst_dir, src_stats.st_size, 'bytes', end=' ', flush=True)
                if config['Zip']:
                    if config['OneZip']:
                        zipname = dst_dir+rel+'/' + os.path.split(folder)[1]
                    else:
                        zipname = dst_dir+rel+'/'+ os.path.splitext(fn)[0]
                    if config.get('AppendDatetimeToName', 0):
                        zipname += datetime.strftime(datetime.now(), " %Y-%m-%d %H-%M-%S")

                    z = zipfile.ZipFile('%s.zip'%zipname, mode='a', compression=zipfile.ZIP_LZMA)
                    zip_stats1 = os.stat('%s.zip'%zipname)
                    z.write(src_fname)
                    z.close()
                    proceed_dst_files.append('%s.zip'%zipname)
                    zip_stats2 = os.stat('%s.zip'%zipname)
                    zip_size_delta = zip_stats2.st_size - zip_stats1.st_size
                    print('%s.zip zipped ok'%zipname, 'to', zip_size_delta, 'bytes')        
                    accum_zip_size += zip_size_delta
 
                    td = datetime.now() - start_time
                    ttt = int(td.total_seconds())
                    tstr = str(timedelta(seconds=ttt))
                    zip_speed = 100500
                    if ttt != 0: 
                        zip_speed = accum_zip_size/(ttt*1024*1024)
                    src_mb, zip_mb = accum_src_size//(1024*1024), accum_zip_size//(1024*1024)
                    s1 = 'Total size %d Mb, total zipped size %d Mb, total time spent'%(src_mb, zip_mb)
                    s2 = '%s sec, avg zip speed %.2f Mbytes/sec'%(tstr, zip_speed)
                    print(s1, s2)
                else:
                    print('')
                    dst_fname = dst_dir + rel + '/' + fn
                    copyfile(src_fname, dst_fname)
                    proceed_dst_files.append(dst_fname)
                if config['EraseAfterCopy']:
                    os.remove(src_fname)
        except Exception as e:
            print('Exception:', e)
    rez = dict(zip(proceed_dst_files, [None]*len(proceed_dst_files)))
    return(list(rez.keys()))

def process_clean_command(config):
    print('process clean command')
    dst_dir = config['DstDir']
    target_size = config['TargetSizeMb']
    if not os.path.isdir(dst_dir):
        print('Clean command: Dst dir not found')
        return
    if not config['DateLevel']:
        print('Only DateLevel cleaning is implemented yet')
        return

    start_time = datetime.now()
    accum_size = 0
    dates = []
    try:
        tree = os.walk(dst_dir)
        for folder, subfolders, files in tree:
            if folder==dst_dir:
                for sf in subfolders:
                    try:
                        datetime.strptime(sf, '%Y-%m-%d')
                        dates.append(sf)
                    except ValueError:
                        print('Subfolder %s is not a date, skipped'%sf)
                print('Date level found', dates)

#            print(folder, subfolders, files)
            for fn in files:
                fname = dst_dir+'/'+fn
                dst_fname = '%s/%s'%(folder, fn)
                stats = os.stat(dst_fname)
                accum_size += stats.st_size

        print('Total file sizes in %s = %d Mb'%(dst_dir, accum_size//(1024*1024)))
        print('Clean target size is %dMb'%target_size)
        if accum_size <= target_size*1024*1024:
            print('Nothing to do')
            return

        dates.sort()

        for sf in dates:
            dir = '%s/%s'%(dst_dir, sf)
            tree = os.walk(dst_dir)
            for folder, subfolders, files in tree:
                for fn in files:
                    fname = dst_dir+'/'+fn
                    dst_fname = '%s/%s'%(folder, fn)
                    stats = os.stat(dst_fname)
                    os.remove(dst_fname)
                    accum_size -= stats.st_size
                    print('%s removed, total size is %dMb'%(dst_fname, accum_size//(1024*1024)))
                    if accum_size <= target_size*1024*1024:
                        print('Cleaning completed')
                        return

    except Exception as e:
        print('Exception:', e)

def repair_folder_name(folder):
    rez = folder
    # good_slash = chr(92)
    good_slash = '/'
    bad_slash = '\\'
    rez = rez.replace(bad_slash, good_slash)
    bad_slash = '//'
    rez = rez.replace(bad_slash, good_slash)
    return rez

def get_folder_files_list(dir):
    rez = []
    try:
        tree = os.walk(dir)
        for folder, subfolders, files in tree:
            for f in files:
                fname = repair_folder_name(folder + '/' + f)
                stat = os.stat(fname)
                size = stat.st_size
                mtime = stat.st_mtime
                rez.append((fname, size, mtime))
    except Exception as e:
        print('Exception:', e)
    return rez

def filter_too_old_files_from_list(flist, days):
    SCE = time.time()
    too_old_range = SCE - 86400*days
    return list(filter(lambda x: x[2] >= too_old_range, flist))

def cut_disk_name(fname):
    return fname[3:]

def filter_also_existing_files(src_files, dst_files):
    dst = {}
    for fname, size, mtime in dst_files:
        key = cut_disk_name(fname), size
        dst[key] = None

    rez = []
    for item in src_files:
        fname, size, mtime = item
        key = cut_disk_name(fname), size
        if not key in dst:
            rez.append(item)

    return rez

def get_files_accum_size(file_list):
    rez_size = 0
    for fname, size, mtime in file_list:
        rez_size += size
    return rez_size

def select_oldest_files_adjust_size_to(file_list, size_to_delete):
    del_list = [fl for fl in file_list if not 'stat' in fl[0].lower()]
    srtd_files = sorted(del_list, key=lambda itm: itm[2])
    accum_size = 0
    todelete = []
    for itm in srtd_files:
        accum_size += itm[1]
        todelete.append(itm)
        if accum_size > size_to_delete:
            break
    return todelete

def copy_em_all_to_disk_O(config):
    print('process copy_em_all_to_disk_O command')
    dst_dir = config['DstDir']
    src_dir = config['SrcDir']
    diskO_max_size = config['DiskOMaxSizeMb']

    src_files_0 = get_folder_files_list(src_dir)
    src_files_1 = filter_too_old_files_from_list(src_files_0, 30)
    dst_files = get_folder_files_list(dst_dir)
    src_files_2 = filter_also_existing_files(src_files_1, dst_files)

    dst_size = get_files_accum_size(dst_files)
    to_copy_size = get_files_accum_size(src_files_2)

    if dst_size + to_copy_size > diskO_max_size*1024*1024: # We have to delete some files, to free some space
        size_to_delete = dst_size + to_copy_size - diskO_max_size*1024*1024
        to_delete_files = select_oldest_files_adjust_size_to(dst_files, size_to_delete)
        for fname, size, mtime in to_delete_files:
            os.remove(fname)

    for fname, size, mtime in src_files_2:
        dst_fname = 'O:/' + cut_disk_name(fname)
        path1, ttttt = os.path.split(dst_fname)
        path2, ttttt = os.path.split(path1)
        if not os.path.isdir(path2):
            os.mkdir(path2)
        if not os.path.isdir(path1):
            os.mkdir(path1)
        copyfile(fname, dst_fname)
        print(fname, '---->', dst_fname)

def process_item(config):
    global last_proceed_files
    command = config['Command']
    if command == 'ZipCopyMove':
        last_proceed_files = replicate_item(config)
    elif command == 'Clean':
        process_clean_command(config)
    elif command == 'ZipFolder':
        last_proceed_files = zip_whole_dir(config)
    elif command == 'CopyEmAllToDiskO':
        copy_em_all_to_disk_O(config)
    else:
        print('Unknown command in config: %s'%command)

try:
    f = open('replicate-config.txt', 'r')
    st = f.read()
    conf = json.loads(st)
    f.close()
    while True:
        last_proceed_files = []
        conf_items = conf['Items']
        for item in conf_items:
            process_item(item)
        print('Lawfully sleep until ', datetime.now()+timedelta(seconds=1500))
        time.sleep(1500)
except Exception as e:
    print(e)
    s = input()

