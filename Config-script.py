import os, json
import hashrate_aligner as aligner


def build_gpu_list(base_folder):
    gpu_dict = {}
    tree = os.walk(base_folder)
    for folder, subfolders, files in tree:
#        print(folder, subfolders, files)
        mn_str = folder[-2:]
#        print(mn_str)
        if not mn_str.isdigit():
            continue
        mn = int(mn_str)
#        print(mn_str)
        for fname in files:
            f = open(folder+'\\'+fname, 'r')
            st = f.readlines()
            f.close()
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
                    print(gpu, mn, bn)




def scan_configs_and_save_users_json(base_folder, users_filename):
    f2 = open(users_filename, 'r')
    st2 = f2.read()
    f2.close()
    data = json.loads(st2)
    ws = data["Workers"]
    usrs = data["Users"]

    for w in ws:
        wn = w["Name"]
        mnfldr = base_folder + wn[0:1] + "iner" + wn[1:3]
        suff = ''
        if wn[-1].isalpha():
            suff = wn[-1]
        config_name = mnfldr + '\\config'+suff+'.txt'
        if not os.path.isfile(config_name):
            print(config_name)
            continue

        f3 = open(config_name, 'r')
        st = f3.readlines()
        f3.close()

        conf = {}
        for i in range(len(st)):
            s = st[i]
            ss = s.split(' ')
            key = ss[0].lower()
            conf[key] = i

        print(wn)
        ewal = st[conf['-ewal']]
        epool = st[conf['-epool']]

        if epool.find('miningpoolhub.com') >= 0:
            st3 = ewal.split(' ')
            st4 = st3[1].split('.')
            user = st4[0]
            w["User"] = user
        elif epool.find('nanopool') >= 0:
            st3 = ewal.split('/')
            mail = st3[-1]
            for u in usrs:
                if not 'pass' in u:
                    continue
                mail2 = u['pass']
                if mail.find(mail2) >= 0:
                    user = u["Nick"]
                    break
            w["User"] = user


    st5 = json.dumps(data)
    f2 = open(users_filename, 'w')
    f2.writelines(st5)
    f2.close()


def build_configs_for_claymore(base_folder, res_folder, users_filename):
    f2 = open(users_filename, 'r')
    st2 = f2.read()
    f2.close()

    data = json.loads(st2)
    ws = data["Workers"]
    usrs = data["Users"]
    print(ws)
    print(usrs)

    if not os.path.isdir(res_folder):
        os.mkdir(res_folder)

    for w in ws:
        wn = w["Name"]
        mnfldr = base_folder + wn[0:1] + "iner" + wn[1:3]
        suff = ''
        if wn[-1].isalpha():
            suff = wn[-1]
        config_name = mnfldr + '\\config'+suff+'.txt'
        if not os.path.isfile(config_name):
            print(config_name)
            continue

        f3 = open(config_name, 'r')
        st = f3.readlines()
        f3.close()

        conf = {}
        for i in range(len(st)):
            s = st[i]
            ss = s.split(' ')
            key = ss[0].lower()
            conf[key] = i

        print(wn)

        username = w["User"]
        user = None
        for u in usrs:
            if u["Nick"].upper() == username.upper():
                user = u

        pool = '-epool asia.ethereum.miningpoolhub.com:20535\n'
        epool = st[conf['-epool']]
        if epool.find('miningpoolhub.com') >= 0:
            pool = epool
        if 'pool' in user:
            pool = '-epool ' + user["pool"] + '\n'

        new_epool = pool
        # print(epool, new_epool, sep ='')

        ewal = st[conf['-ewal']]
        new_ewal = '-ewal ' + w["User"] + '.%COMPUTERNAME%' + suff.upper() + '\n'
        if pool.find('nanopool.org') >= 0:
            new_ewal = '-ewal ' + user["wallet"] + "/%COMPUTERNAME%" + suff + "/" + user["pass"] + '\n'
        # print(ewal, new_ewal, sep ='')

        eworker = ''
        if '-eworker' in conf:
            eworker = st[conf['-eworker']]
        new_eworker = '-eworker ' + w["User"] + '.%COMPUTERNAME%' + suff.upper() + '\n'
        # print(eworker, new_eworker, sep ='')

        i = conf['-epool']
        st[i] = new_epool

        i = conf['-ewal']
        st[i] = new_ewal

        if '-esm' in conf:
            i = conf['-esm']
            st.pop(i)

        if '-eworker' in conf:
            i = conf['-eworker']
            st.pop(i)

        if new_epool.find('miningpoolhub.com') >= 0:
            p = conf['-ewal']
            st.insert(p+1, '-esm 2\n')
            st.insert(p+1, new_eworker)

        res_mn_fldr = res_folder + wn[0:1] + "iner" + wn[1:3]
        if not os.path.isdir(res_mn_fldr):
            os.mkdir(res_mn_fldr)
        res_config_name = res_mn_fldr + '\\config'+suff+'.txt'

        f4 = open(res_config_name, 'w')
        f4.writelines(st)
        f4.close()


def prepare_eth_contracts_and_workers(users, eth_workers, workers):
    all_w_dict = {}
    for w in workers:
        hr = w['Hashrate']
        nm = w['Name']
        all_w_dict[nm] = hr

    eth_w_dict = {}
    for w_name in eth_workers:
        eth_w_dict[w_name] = all_w_dict[w_name]

    eth_contr = {}
    for u in users:
        if 'EthHashrate' in u:
            u_name = u['Nick']
            eth_contr[u_name] = u['EthHashrate']

    return all_w_dict, eth_w_dict, eth_contr

def postprocess_workers(aligned_workers):
    rez = {}
    for w_name, wrkr in aligned_workers.items():
        wrkr_copy = dict()
        wrkr_copy.update(wrkr)
        if wrkr['Hashrate'] < 1:
            wrkr_copy['user'] = 'pulk2'
        rez[w_name] = wrkr_copy
    return rez

def align_eth_and_etc_workers_to_users(users, eth_workers, workers):
    all_ws, eth_ws, eth_contr = prepare_eth_contracts_and_workers(users, eth_workers, workers)
    aligned_workers = aligner.align_workers_to_users(eth_ws, eth_contr, 'ETH')
    for k in aligned_workers:
        eth_ws.pop(k)

    if eth_ws:
        contr = {'pulk': 100500}
        left_eth_workers = aligner.align_workers_to_users(eth_ws, contr, 'ETH')
        aligned_workers.update(left_eth_workers)

    etc_contracts = {}
    for u in users:
        u_name = u['Nick']
        total_hr = u['ContractHashrate']
        eth_hr = eth_contr.get(u_name, 0)
        etc_hr = total_hr - eth_hr
        etc_contracts[u_name] = etc_hr

    for w_name in aligned_workers:
        # u_name = aligned_workers[w_name]['user']
        # hr = all_ws[w_name]
        aligned_workers[w_name]['Hashrate'] = all_ws[w_name]
        all_ws.pop(w_name)
        # contracts[u_name] -= hr

    etc_workers = aligner.align_workers_to_users(all_ws, etc_contracts, 'ETC')
    aligned_workers.update(etc_workers)
    for w_name in all_ws:
        if w_name in aligned_workers:
            aligned_workers[w_name]['Hashrate'] = all_ws[w_name]

    aligned_workers = postprocess_workers(aligned_workers)
    return aligned_workers

def make_users_hashrate_report(ws, users, filename = ''):
    usrs = dict()
    for u in users:
        d = dict(eth_rigs=[], etc_rigs=[], eth_hr=0, etc_hr=0)
        d.update(u)
        nick = u['Nick']
        usrs[nick] = d
    for w_name, w_info in ws.items():
        user = w_info['user']
        coin = w_info['coin']
        hr   = w_info['Hashrate']
        d = usrs[user]
        if coin == 'ETH':
            d['eth_rigs'] += [w_name]
            d['eth_hr'] += hr
        if coin == 'ETC':
            d['etc_rigs'] += [w_name]
            d['etc_hr'] += hr

    if filename:
        f = open(filename, 'w')
    for k, v in usrs.items():
        print(k)
        etc_contract = v['ContractHashrate'] - v.get('EthHashrate', 0)
        print(f'    ETC  contract {etc_contract: 7.0f}, aligned{v["etc_hr"]: 8.2f}')
        if 'EthHashrate' in v:
            print(f'    ETH  contract {v["EthHashrate"]: 7.0f}, aligned{v["eth_hr"]: 8.2f}')

        if filename:
            print(k, file=f)
            print(f'    ETC  contract {etc_contract: 7.0f}, aligned{v["etc_hr"]: 8.2f}', file=f)
            if 'EthHashrate' in v:
                print(f'    ETH  contract {v["EthHashrate"]: 7.0f}, aligned{v["eth_hr"]: 8.2f}', file=f)

    pass


def build_configs_for_lolminer_etc(base_folder, res_folder, users, eth_workers, workers, plan_period=10):
    ws = align_eth_and_etc_workers_to_users(users, eth_workers, workers)
    make_users_hashrate_report(ws, users, res_folder+'hashrate_report.txt')

    # default_pools = dict(ETH='pool=asia.ethereum.miningpoolhub.com:20535\n', ETC='pool=us-east.etchash-hub.miningpoolhub.com:20615\n')
    default_pools = dict(ETH='pool=eth.2miners.com:2020\n', ETC='pool=etc.2miners.com:1010\n')
    # default_pools = dict(ETH='pool=asia.ethereum.miningpoolhub.com:20535\n', ETC='pool=etc.2miners.com:\n')
    algo_names = dict(ETH='ETHASH', ETC='ETCHASH')

    if not os.path.isdir(res_folder):
        os.mkdir(res_folder)

    for wn in ws:
        w = ws[wn]
        username = w["user"]
        coin = w["coin"]
        mnfldr = base_folder + wn[0:1] + "iner" + wn[1:3]
        suff = ''
        if wn[-1].isalpha():
            suff = wn[-1]
        config_name = mnfldr + '\\config_lolminer'+suff+'.txt'
        if not os.path.isfile(config_name):
            print(config_name)
            continue

        f3 = open(config_name, 'r')
        st = f3.readlines()
        f3.close()

        conf = {}
        for i in range(len(st)):
            s = st[i]
            ss = s.split('=')
            key = ss[0].lower()
            conf[key] = i

        print(wn, ws[wn])

        user = None
        for u in usrs:
            if u["Nick"].upper() == username.upper():
                user = u

        pool = default_pools[coin]
        epool = st[conf['pool']]
        if epool.find('miningpoolhub.com') >= 0:
            pool = epool
        pool_key = coin.lower() + '_pool'
        if pool_key in user:
            pool = 'pool=' + user[pool_key] + '\n'

        new_epool = pool
        MPH = pool.find('miningpoolhub.com') >= 0
        # print(epool, new_epool, sep ='')

        ewal = st[conf['user']]
        try:
            wallet_key = coin.lower() + '_wallet'
            if MPH:
                new_ewal = 'user=' + f'{username}.{wn}\n'
            elif pool.find('nanopool.org') >= 0:
                new_ewal = 'user=' + user[wallet_key] + f'.{username}-{wn}/{user["pass"]}\n'
            else:
                new_ewal = 'user=' + user[wallet_key] + f'.{username}-{wn}\n'
        except:
            print('!!!!!', wn, user)

        i = conf['algo']
        st[i] = f'algo={algo_names[coin]}\n'

        i = conf['pool']
        st[i] = new_epool

        i = conf['user']
        st[i] = new_ewal

        if 'ethstratum' in conf:
            i = conf['ethstratum']
            st.pop(i)

        if MPH:
            p = conf['user']
            st.insert(p+1, 'ethstratum=ETHV1\n')

        res_mn_fldr = res_folder + wn[0:1] + "iner" + wn[1:3]
        if not os.path.isdir(res_mn_fldr):
            os.mkdir(res_mn_fldr)
        res_config_name = res_mn_fldr + '\\config_lolminer'+suff+'.txt'

        f4 = open(res_config_name, 'w')
        f4.writelines(st)
        f4.close()
    print('New configs have been done!')


if __name__ == '__main__':
    base_folder = "Y:\\_Майнинг\\Autoupdate\\Config\\"
    res_folder = "Y:\\_Майнинг\\Config_test\\"
    contracts_filename = 'contracts-to-electric.json'
    # contracts_filename = 'contracts-to-users.json'

    f2 = open(contracts_filename, 'r')
    st2 = f2.read()
    f2.close()
    data = json.loads(st2)
    usrs = data["Users"]
    eth_ws = data["ETH_Workers"]
    all_ws = data["Workers"]

    build_configs_for_lolminer_etc(base_folder, res_folder, usrs, eth_ws, all_ws)
