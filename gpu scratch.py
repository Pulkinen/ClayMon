import os, json

out_fn = 'gpu_list.csv'
out_file = open(out_fn, 'w')
out_fn2 = 'all_gpus.txt'
out_file2 = open(out_fn2, 'w')

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
        print(folder[-7:], file=out_file2, end=' ')
#        print(mn_str)
        for fname in files:
            gpu_file_name = f'M{mn_str}-GPU.txt'
            if fname.upper() != gpu_file_name.upper():
                continue
            f = open(folder+'\\'+fname, 'r')
            st = f.readlines()
            f.close()
            for s2 in st:
                if not s2.startswith('#'):
                    continue
#                print(s)
                cnt = 0
                s = s2[1:]
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
                    print(gpu, mn, bn, file = out_file)
                    cnt += 1
                print(cnt, '   ', s2, file=out_file2, end='')
        print('', file=out_file2)


base_folder = r"C:\\tmp\\7777\\"
build_gpu_list( base_folder )