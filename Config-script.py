import os, json

def build_configs(base_folder, res_folder, users_filename):
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

        pool = '-epool europe1.ethereum.miningpoolhub.com:20535\n'
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

base_folder = "Y:\\_Майнинг\\Autoupdate\\Config\\"
res_folder = "Y:\\_Майнинг\\Autoupdate\\Config_test\\"
users_filename = 'workers-to-electric.json'
build_configs( base_folder, res_folder, users_filename )
# scan_configs_and_save_users_json(base_folder, users_filename)