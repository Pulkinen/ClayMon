import json

def BuildConfig(user, worker, mode):
    rez = []
    wname = worker["Name"]
    widx = 'ABCDEFGH'.find(wname[-1])
    wnum = wname[1:]
    if widx >= 0:
        wnum = wname[1:-1]
    rez.append('-mode ' + str(mode))
    prt = 3333
    if widx >= 0:
        prt = 3332 + widx
    rez.append('-mport ' + str(prt))
    rez.append('-minspeed 10')
    rez.append('-tt 65')
    rez.append('-ttdcr 70')
    rez.append('-ttli 80')
    rez.append('-tstop 85')
    rez.append('-tstart 50')
    rez.append('-ethi 16')
    wrkname = '%COMPUTERNAME%'
    if widx >= 0:
        wrkname = wrkname + 'ABCDEFGH'[widx]
    pool = 'europe1.ethash-hub.miningpoolhub.com:20535'
    if "pool" in user:
        pool = user["pool"]
    rez.append('-epool ' + pool)
    if pool == 'europe1.ethash-hub.miningpoolhub.com:20535':
        wal = user["Nick"] + '.' + wrkname
        rez.append('-ewal ' + wal)
        rez.append('-eworker ' + wal)
        rez.append('-esm 2')
        rez.append('-epsw x')
    if pool == 'eth-eu1.nanopool.org:9999':
        wallet = user["wallet"]
        psw = user["pass"]
        wal = wallet + '/' + wrkname + '/' + psw
        rez.append('-ewal ' + wal)
        rez.append('-epsw x')
        rez.append('-erate 1')

    rez.append('-allpools 1')
    rez.append('-allcoins 1')
    rez.append('-dpool stratum+tcp://dcr.coinmine.pl:2222')
    rez.append('-dwal pulk.%COMPUTERNAME%D')
    rez.append('-dpsw x')
    rez.append('-platform 1')
    rez.append('-logsmaxsize 10')

    return rez

