def get_gpu_list_from_config(st):
    rez = []
    for s in st:
        if not s.startswith('#'):
            continue
        #                print(s)
        s = s[1:]
        pairs = s.split(',')
        #                print(pairs)
        for p in pairs:
            pair = p.split()
            if len(pair) != 2:
                continue
            #                    print(pair[0], pair[1])
            gpustr = max(pair)
            bnstr = min(pair)
            if gpustr[:3] != 'GPU' or bnstr[:2] != 'BN':
                continue
            gpu = gpustr[3:]
            bn = bnstr[2:]
            if bn.upper() == '0A':
                bn = '10'
            try:
                rez += [(int(bn), int(gpu))]
            except:
                print('ERROR IN CONFIG!!!', bnstr, gpustr, 'in', s)
    return rez

def get_wallet_from_config(st):
    for s in st:
        if s.startswith('-ewal'):
            sss = s.split(' ')[1]
            if '/' in sss:
                sss = sss.split('/')
                rez = sss[-1]
            else:
                sss = sss.split('.')
                rez = sss[0]
            return rez



