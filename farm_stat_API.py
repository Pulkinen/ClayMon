import zipstat

def api_gimme_farm_hashrate(start, stop, effective_only = True):
    return zipstat.gimme_farm_hashrate(start, stop, effective_only)

def api_gimme_farm_stat(start, stop):
    return zipstat.gimme_farm_stat(start, stop)

def api_gimme_rig_hashrate(rig, start, stop, effective_only = True):
    return zipstat.gimme_rig_hashrate(rig, start, stop, effective_only)

def api_gimme_rig_stat(rig, start, stop):
    return zipstat.gimme_rig_stat(rig, start, stop)

def api_gimme_gpu_hashrate(gpu, start, stop, effective_only = True):
    return zipstat.gimme_gpu_hashrate(gpu, start, stop, effective_only)

def api_gimme_gpu_stat(gpu, start, stop):
    return zipstat.gimme_gpu_stat(gpu, start, stop)

def api_gimme_user_hashrate(user, start, stop, effective_only=True):
    return zipstat.gimme_user_hashrate(user, start, stop, effective_only)

def api_gimme_user_stat(user, start, stop):
    return zipstat.gimme_user_stat(user, start, stop)
