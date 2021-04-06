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
                zipname = dst_dir + '/' + os.path.split(folder)[1]
                z.write(src_fname)
        except Exception as e:
            print('Exception:', e)
    z.close()


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


def process_item(config):
    global last_proceed_files
    command = config['Command']
    if command == 'ZipCopyMove':
        last_proceed_files = replicate_item(config)
    elif command == 'Clean':
        process_clean_command(config)
    elif command == 'ZipFolder':
        zip_whole_dir(config)
    else:
        print('Unknown command in config: %s'%command)


f = open('replicate-config.txt', 'r')
st = f.read()
conf = json.loads(st)
f.close()
while True:
    last_proceed_files = []
    conf_items = conf['Items']
    for item in conf_items:
        process_item(item)
    print('Lawfully sleep until ', datetime.now()+timedelta(seconds=900))
    time.sleep(900)