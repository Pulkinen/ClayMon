import itertools as it
import numpy as np
import networkx as nx
import itertools

def align_hashrates(users_contract, users_debt, worker_list, period):
    pass

def evaluate_matching(matching, workers_vec, contract_vec):
    WN, UN = matching.shape
    assert workers_vec.shape == (1, WN)
    assert contract_vec.shape == (1, UN)
    hashrate_vec = np.matmul(workers_vec, matching)
    residual_vec = hashrate_vec - contract_vec
    rez = np.linalg.norm(residual_vec[0], 1)
    return rez

def make_greed_matching(workers_vec, contract_vec):
    UN = len(contract_vec[0])
    WN = len(workers_vec[0])
    assert workers_vec.shape == (1, WN)
    assert contract_vec.shape == (1, UN)
    matching = np.zeros((WN, UN))
    workers = workers_vec.copy()
    for k in range(WN):
        hashrate_vec = np.matmul(workers_vec, matching)
        unaligned_contracts = contract_vec - hashrate_vec
        probe_matrix = abs(unaligned_contracts - workers.T)
        probe_matrix = abs(unaligned_contracts) - probe_matrix
        assert (WN, UN) == probe_matrix.shape
        probe_matrix[probe_matrix==0] = -100500
        i, j = np.unravel_index(np.argmax(probe_matrix), probe_matrix.shape)
        matching[i, j] = 1
        workers[0, i] = 0
        if max(np.sum(matching, axis = 1)) != 1:
            assert False
    assert min(np.sum(matching, axis = 1)) == 1
    return matching

    # for _ in range(WN*UN):
    #     i, j = np.unravel_index(np.argmax(probe_matrix), probe_matrix.shape)

def better_matching(workers_vec, contract_vec):
    _, UN = contract_vec.shape
    assert _ == 1
    _, WN = workers_vec.shape
    assert _ == 1
    matching = np.zeros((WN, UN))
    probe_matrix = abs(contract_vec - workers_vec.T)
    probe_matrix = abs(contract_vec) - probe_matrix
    assert (WN, UN) == probe_matrix.shape

    if WN == 1:
        hr = workers_vec[0, 0]
        i = np.argmax(probe_matrix)
        matching[0, i] = 1
        target = abs(contract_vec[i] - hr)
        return matching, target

    best_target = 100500100500
    best_matching = None
    for i in range(WN):
        _workers = np.concat([workers_vec[:,:i], workers_vec[:,i+1:]])
        for j in range(UN):
            _contracts = contract_vec.copy
            _contracts[j] -= workers_vec[i]
            small_target, small_matching = better_matching(_workers, _contracts)
            if small_target + 0:
                pass

    return best_matching, best_target

def better_matching2(workers_vec, contract_vec):
    pass

def make_match_dict_key_is_user(mx):
    WN, UN = mx.shape
    aaaa = list(np.argmax(mx, axis=1))
    rez = {u:[] for u in range(UN)}
    for w, u in enumerate(aaaa):
        rez[u] += [w]
    return rez

def make_match_dict_key_is_worker(mx):
    WN, UN = mx.shape
    aaaa = list(np.argmax(mx, axis=1))
    rez = {w:[] for w in range(WN)}
    for w, u in enumerate(aaaa):
        rez[w] = u
    return rez


def version_0():
    wrks = np.array([[150, 198, 172, 215, 186, 202, 200, 205, 206, 207, 202, 180, 181, 178, 197, 156, 207, 205, 202, 186, 201, 195, 153, 157, 149, 209, 184, 171, 172, 167, 182, 178, 166, 120, 172, 201, 94, 159, 185, 211, 195, 210, 193, 214, 103, 171, 181, 193, 100, 106, 199, 166, 213, 195, 88, 202, 167, 210, 120, 223, 93, 174, 183, 212, 137, 192, 184, 12]])
    cntrs = np.array([[3200, 2200, 39, 72, 114, 230, 20, 80, 370, 219, 97, 146, 80, 176, 97, 97, 72, 97, 174, 5540]])
    k = np.sum(wrks) / np.sum(cntrs)
    cntrs = k * cntrs
    mtch = make_greed_matching(wrks, cntrs)
    cost = evaluate_matching(mtch, wrks, cntrs)
    print(mtch)
    print(cost)
    best_cost = cost
    best_match = mtch
    aaaa = np.argmax(mtch, axis=1)
    a1, a2, a3 = 0, 0, 0
    md = make_match_dict_key_is_user(best_match)

    _, UN = cntrs.shape
    _, WN = wrks.shape
    L = 5
    idxs_U = list(range(UN))
    idxs_W = list(range(WN))

    p = list(range(L))
    pairs = list(zip(p, [p[-1]] + p[:-1]))

    _=-1
    for u_list in itertools.permutations(idxs_U, L):
        if u_list[0] != _:
            _ = u_list[0]
            print(u_list[0])

        ts = [md[u_list[i]] for i in range(L)]
        for w_list in itertools.product(*ts):
            a1 += 1
            if a1 % 50000 == 0:
                print('.', end='')
            m1 = mtch.copy()
            for w1, w2 in pairs:
                m1[w_list[w1]] = mtch[w_list[w2]]
            cost = evaluate_matching(m1, wrks, cntrs)
            if cost < best_cost:
                print('\n', cost, w_list)
                best_cost = cost
                best_match = m1.copy()
    print(best_match)

def align_hashrate_v1(wrks, cntrs):

    # wrks = np.array([[150, 198, 172, 215, 186, 202, 200, 205, 206, 207, 202, 180, 181, 178, 197, 156, 207, 205, 202, 186, 201, 195, 153, 157, 149, 209, 184, 171, 172, 167, 182, 178, 166, 120, 172, 201, 94, 159, 185, 211, 195, 210, 193, 214, 103, 171, 181, 193, 100, 106, 199, 166, 213, 195, 88, 202, 167, 210, 120, 223, 93, 174, 183, 212, 137, 192, 184, 12]])

    # wrks = np.array([[230,221.9,204,27.6,219,239,230,29,174,232,61,61,31,92,242,231,117,92,236,197,244,227,245,235,239,225,245,
    #                   223,216,238,91,117,212,244,236,169,235,194,207,234,224,235,213,180,95,212,241,243,225,
    #                   183,235,246,13.5,13.5,13.5,13.5,13.5,13.5,13.5,13.5,240,246,174,102,81,27,226,180,245,212,
    #                   207,209,246,248,95,73,256,67,12,253,254,254,252,220,230]])
    #
    # cntrs = np.array([[231, 494, 174, 230, 138, 2800, 158, 78, 78, 97, 194, 97, 97, 78, 81, 39, 19, 3200, 410, 6164]])

    wrks[wrks==0]=1
    k = np.sum(wrks) / np.sum(cntrs)
    cntrs = k * cntrs
    mtch = make_greed_matching(wrks, cntrs)
    cost = evaluate_matching(mtch, wrks, cntrs)
    print(mtch)
    print(cost)
    best_cost = cost
    best_match = mtch
    a1, a2, a3 = 0, 0, 0
    md = make_match_dict_key_is_user(best_match)

    _, UN = cntrs.shape
    _, WN = wrks.shape
    L = 2
    idxs_U = list(range(UN))

    p = list(range(L))
    pairs = list(zip(p, [p[-1]] + p[:-1]))

    smth_changes = True
    while smth_changes:
        smth_changes = False
        _=-1
        for u_list in itertools.permutations(idxs_U, L):
            ts = [md[u_list[i]] for i in range(L)]
            for w_list in itertools.product(*ts):
                m1 = mtch.copy()
                for w1, w2 in pairs:
                    m1[w_list[w1]] = mtch[w_list[w2]]
                cost = evaluate_matching(m1, wrks, cntrs)
                if cost < best_cost:
                    # print('\n', cost, w_list)
                    best_cost = cost
                    best_match = m1.copy()
                    smth_changes = True
        mtch = best_match.copy()
        md = make_match_dict_key_is_user(best_match)
    rez = make_match_dict_key_is_worker(best_match)
    return rez

def pay_hashrate_debt(workers, contracts, debts, days_to_return):
    cntrs = contracts + debts/days_to_return
    md = align_hashrate_v1(workers, cntrs)
    return md

def align_workers_to_users(workers_dict, users_dict, coin):
    WN = len(workers_dict)
    UN = len(users_dict)
    w_lst = [v for k, v in workers_dict.items()]
    c_lst = [v for k, v in users_dict.items()]
    wrks = np.array([w_lst])
    cntrs = np.array([c_lst])
    md = align_hashrate_v1(wrks, cntrs)
    w_names = [k for k, v in workers_dict.items()]
    u_names = [k for k, v in users_dict.items()]
    rez = {}
    for w, u in md.items():
        w_name = w_names[w]
        u_name = u_names[u]
        rez[w_name] = dict(user=u_name, coin=coin)
    return rez


if __name__ == '__main__':
    wrks = np.array([[105,167,112,211,233,172,201,175,178,207,199,25,148,148,174,0,174,207,85,0,204,199,178,144,179,153,157,157,151,182,0,0,201,104,0,180,0,0,0,183,205,223,198,130,0,114,110,0,192,145,117,195,167,145,0,83,173,168,209,0,246,116,171,0,174,102,186,159,149]])
    cntrs = np.array([[3200, 2800, 39, 72, 114, 230, 20, 80, 370, 219, 97, 146, 80, 176, 97, 97, 72, 97, 174, 5540]])
    md = align_hashrate_v1(wrks, cntrs)