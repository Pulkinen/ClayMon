import json
import random
import Configs

def AgregateHashrate(workers):
    sum = 0
    if workers == None:
        return 0
    for wrk in workers:
        hrs = wrk["Hashrate"]
        sum += hrs
    return sum

#cccccc = 0

def DoStep(id, state, actualChain, candidates):
#    global cccccc
#    cccccc += 1
#    print(cccccc)
#    print(id)
#    print(actualChain)
    target = state["Target"]
    tol = state["Tolerance"]
    bestSum = state["BestSum"]
    candSum = state["candSum"]
    accSum = state["AccSum"]
#    print(state["BestChain"])
    if bestSum < target:
        return state

    if accSum > bestSum:
        return state

    if target - tol < accSum < bestSum:
        bestSum = accSum
        bestChain = actualChain.copy()
#        for cand in candidates:
#            bestChain.append(cand)
        state["BestChain"] = bestChain
        state["BestSum"] = bestSum
        return state

    cands = []
    candSum = 0
    for cand in candidates:
        candHash = cand["Hashrate"]
        if candHash <= bestSum - accSum:
            cands.append(cand)
            candSum += candHash
    state["candSum"] = candSum

    if not cands:
        return state

    if accSum + candSum < target - tol:
        return state

    cand = cands.pop()
    st1 = state.copy()
    id0 = id + '0'
    id1 = id + '1'
    ac1 = actualChain.copy()
    st1 = DoStep(id0, st1, ac1, cands)
    if st1["BestSum"] < state["BestSum"]:
        state["BestChain"] = st1["BestChain"]
        state["BestSum"] = st1["BestSum"]

    actualChain.append(cand)
    candHash = cand["Hashrate"]
    state["candSum"] = candSum - candHash
    accSum += candHash
    state["AccSum"] = accSum

    st2 = state.copy()
    ac2 = actualChain.copy()
    st2 = DoStep(id1, st2, ac2, cands)
    if st2["BestSum"] < st1["BestSum"]:
        st1 = st2
    return st1


def ExecuteContracts(users, workers):
    bestrez = {}
    bestOR = 100500
    lastuser = {}
    for i in range(10):
        rez = {}
        wrks = workers.copy()
        overrun = 0
        random.shuffle(users)
        for user in users:
            if "Contract" in user:
                lastuser = user
                continue
            state = {}
            state["Target"] = user["ContractHashrate"]
            state["Tolerance"] = user["Tolerance"]
            state["BestSum"] = 100500
            state["candSum"] = 0
            state["AccSum"] = 0
            state["BestChain"] = []
            st = DoStep('', state, [], wrks )
            ws = st["BestChain"]
    #        ws = GetWorkers(wrks, minHash, maxHash)
            ovr = AgregateHashrate(ws) - user["ContractHashrate"]
            overrun += abs(ovr)
            nick = user["Nick"]
            rez[nick] = ws
            for w in ws:
                wrks.remove(w)
        if overrun < bestOR:
            bestOR = overrun
            bestrez = rez.copy()
    nick = lastuser["Nick"]
    bestrez[nick] = wrks
    return bestrez


f1 = open('users.json', 'r')
st1 = f1.read()
us = json.loads(st1)["Users"]
users = sorted(us, key=lambda user: user["Tolerance"])
f2 = open('wrks.json', 'r')
st2 = f2.read()
ws = json.loads(st2)["Workers"]
workers = sorted(ws, key=lambda worker: worker["Hashrate"])
workers.reverse()
print(len(workers))

random.seed(0)
rez = ExecuteContracts(users, workers)
f3 = open("rez.txt", 'w')
print(rez, file=f3, sep="\n")
f3.close()
cnt = 0
cnt2 = 0
for user in users:
    nick = user["Nick"]
    wrkrs = rez[nick]
    cnt2 += len(wrkrs)
    for wrk in wrkrs:
        config = Configs.BuildConfig(user, wrk, 1)
        cnt+=1
        wname = wrk["Name"]
        widx = 'ABCDEFGH'.find(wname[-1])
        wnum = wname[1:]
        if widx >= 0:
            wnum = wname[1:-1]
        fname = "Configs\Miner"+wnum+"\config"
        if widx > 0:
            fname += 'ABCDEFGH'[widx]
        fname += '.txt'
        f3 = open(fname, 'w')
        print(*config, file=f3, sep="\n")
        f3.close()

print(cnt2)