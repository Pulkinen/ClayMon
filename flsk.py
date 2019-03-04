from flask import Flask, render_template, request
from datetime import datetime
import sqlite3, json

Param = "Total"
maxMonthBonus = 23000
target = 15300
cutoff = 14300

def CalcBonus(points, start, end):
    rez = []
    bns = []
    avg = []
    cut = []
    techStops = 6 * 3600
    b = maxMonthBonus/2
    decline = maxMonthBonus / 2 / (end - start).days / 86400
    AvgH = 0
    p0 = points[0]
    if p0[0] > start.timestamp() - 5 * 3600 - 1000:
        for i in range(len(points)-1):
            p1 = points[i]
            p2 = points[i+1]
            h = (p1[1] + p2[1]) / 1000 / 2
            AvgH += h * (p2[0] - p1[0])
            if (p2[0] - p1[0]) > 3600:
                print(p2[0], p2[0] - p1[0], h)
        print(points[-1][0] - points[0][0])
        AvgH = AvgH / (points[-1][0] - points[0][0] - techStops)
        points[0:0] = [(start.timestamp(), AvgH*1000), (p0[0]-1, AvgH*1000)]

    for i in range(len(points)-1):
        p1 = points[i]
        p2 = points[i+1]
        h = (p1[1] + p2[1]) / 1000 / 2
        if h > target:
            h = target
        elif h < cutoff:
            h = cutoff
        k = (target - h) / (target - cutoff)
        b = b - decline * k * (p2[0] - p1[0])
        rez.append((p1[0], b))
        bns.append([p1[0], 10 * (1 - k) * maxMonthBonus / 30.5])
        avg.append((p1[0], AvgH))
        cut.append((p1[0], cutoff))
    if points[-1][0] < end.timestamp():
        rez.append((end.timestamp(), b))
        avg.append((end.timestamp(), AvgH))
        cut.append((end.timestamp(), cutoff))

    rezult = {}
    rezult["Decline"] = rez
    rezult["Growth"] = bns
    rezult["AvgH"] = avg
    rezult["Cutoff"] = cut

    return rezult

def GetActualPeriod():
    now = datetime.now()
    if (now.day <= 27) or (now.day >= 12):
        st = datetime(now.year, now.month, 10, 0, 0, 0, 0)
        end = datetime(now.year, now.month, 25, 0, 0, 0, 0)
    else:
        st = datetime(now.year, now.month, 25, 0, 0, 0, 0)
        end = datetime(now.year, now.month, 10, 0, 0, 0, 0)
        end.replace(month = now.month + 1)
    return (st, end)


def BuildPiecewiseFunc(rows):
    segments = []
    newseg = None
    points = []
    for row in rows:
        if newseg == None:
            newseg = {}
            newseg["min"] = row[0]
            newseg["max"] = row[1]
            newseg["lastuptime"] = row[3]
            points = []
            points.append((row[0], row[2]))
            points.append((row[1], row[2]))
            newseg["points"] = points
            continue

        if row[2] > 0:
            isectCond = row[0] <= newseg["max"] + 10
            uptimeCond = row[3] > newseg["lastuptime"]
            uptimeCond = uptimeCond and ((row[3] - newseg["lastuptime"]) * 60 - (row[1] - newseg["max"]) > -90)
            if isectCond or uptimeCond:
                points.append((row[0], row[2]))
                points.append((row[1], row[2]))
                if row[3] > newseg["lastuptime"]:
                    newseg["lastuptime"] = row[3]
                if row[1] > newseg["max"]:
                    newseg["max"] = row[1]
                continue
            ps = sorted(points, key = lambda p: p[0])
            segments.append(newseg)
            emptyseg = {}
            emptyseg["min"] = newseg["max"] + 1
            emptyseg["max"] = row[0] - 1
            points = []
            points.append((emptyseg["min"], 0))
            points.append((emptyseg["max"], 0))
            emptyseg["points"] = points
            segments.append(emptyseg)

            newseg = {}
            newseg["min"] = row[0]
            newseg["max"] = row[1]
            newseg["lastuptime"] = row[3]
            points = []
            points.append((row[0], row[2]))
            points.append((row[1], row[2]))
            ps = sorted(points, key = lambda p: p[0])
            newseg["points"] = points
            continue

    if newseg != None:
        segments.append(newseg)
    return segments

def EvalutePCWFunc(pcw, x):
    if (x < pcw["min"]) or x > pcw["max"]:
        return 0
    points = pcw["points"]
    for i in range(len(points)-1):
        p1 = points[i]
        p2 = points[i+1]
        if x == p1[0]:
            return p1[1]
        if x == p2[0]:
            return p2[1]
        if p2[0] > x > p1[0]:
            t = (x - p1[0])/(p2[0] - p1[0])
            y = p1[1] + t*(p2[1] - p1[1])
            return y
    return 0

def TwoPCWfuncAddition(pcw1, pcw2):
    pl = pcw1 + pcw2
    rez = []
    pool = sorted(pl, key=lambda s: s["min"])
    newseg = None
    i = 0

    for seg in pool:
        i += 1
        if i == 7:
            break
        if newseg == None:
            newseg = {}
            newseg["min"] = seg["min"]
            newseg["max"] = seg["max"]
            ps = seg["points"]
            newseg["points"] = sorted(ps, key=lambda p: p[0])
            continue
        isectCond = seg["min"] < newseg["max"]
        if not isectCond:
            ps = sorted(newseg["points"], key=lambda p: p[0])
            newseg["points"] = ps
            rez.append(newseg)
            newseg = {}
            newseg["min"] = seg["min"]
            newseg["max"] = seg["max"]
            ps = seg["points"]
            newseg["points"] = sorted(ps, key=lambda p: p[0])
            continue
        if seg["max"] > newseg["max"]:
            newseg["max"] = seg["max"]
        if seg["min"] < newseg["min"]:
            newseg["min"] = seg["min"]
        pnts1 = newseg["points"]
        pnts2 = seg["points"]
        pnts = []
        Xs = {p[0] for p in pnts1+pnts2}
        for x in Xs:
            y1 = EvalutePCWFunc(seg, x)
            y2 = EvalutePCWFunc(newseg, x)
            pnts.append((x, y1+y2))
        newseg["points"] = sorted(pnts, key=lambda p: p[0])

    if newseg != None:
        ps = sorted(newseg["points"], key=lambda p: p[0])
        newseg["points"] = ps
        rez.append(newseg)
    return rez


def GetSandboxChartJson():
    connection = sqlite3.connect("Claymon.sqlite")
    cursor = connection.cursor()


    sql = """
        With
        t1 As (Select (Startsecs+5*3600)*1000, AvgHashrate/1000, (Endsecs+5*3600)*1000, AvgHashrate/1000 From WorkersHistory Where Worker = 'M00' Order by 1 DESC Limit 150)
        Select * from t1 Order by 1 ASC 
    """


    sss1 = """{
            "rangeSelector": {
                "selected": 1
            },
            "title": {
                "text": "Sandbox"
            },
            "series": []
        }"""
    rez = json.loads(sss1)
    t1 = rez['series']

    for row in cursor.execute(sql):
        sss2 = """ {
            "name": "",
            "data": [],
            "tooltip": {
                "valueDecimals": 2
            }
        } """
        ser1 = json.loads(sss2)
        ser1["data"] = [(row[0], row[1]), (row[2], row[3])]
        t1.append(ser1)

    return json.dumps(rez)

def GetTotalHashrateChartJson():
    logHr = GetClayMonLogTotal()
    hashrates = {}
    connection = sqlite3.connect("Claymon.sqlite")
    cursor = connection.cursor()
    per = GetActualPeriod()
    tstamp1 = per[0].timestamp()
    tstamp2 = per[1].timestamp()
    # 1539340791
    # sql = "Select Min(startsecs), Max(endsecs) from WorkersHistory where EndSecs > strftime('%s', 'now', 'localtime') - 5 * 3600 - 1 * 86400 And Uptime > 0"
    sql = "Select Min(startsecs), Max(endsecs) from WorkersHistory where EndSecs + 5 * 3600 Between {0} And {1} And Uptime > 0".format(tstamp1, tstamp2)
    print(sql)
    cursor.execute(sql)
    tmp1 = cursor.fetchall()
    tmp2 = tmp1[0]
    tmin = tmp2[0]
    tmax = tmp2[1]

    cursor = connection.cursor()
    sql = "Select Distinct Worker From Workers"
    t = datetime.now()
    cursor.execute(sql)
    workers = cursor.fetchall()
    for row in workers:
        worker = row[0]
        # sql = """
        #         Select startsecs, endsecs, AvgHashrate, uptime
        #         from WorkersHistory where
        #         EndSecs > strftime('%s', 'now', 'localtime') - 5 * 3600 - 1 * 86400
        #         And Worker = '{0}' And Uptime >= 0
        #         Order by 1
        #     """.format(worker)
        sql = """
                Select startsecs, endsecs, AvgHashrate, uptime
                from WorkersHistory where
                EndSecs + 5 * 3600 Between {0} And {1}
                And Worker = '{2}' And Uptime >= 0
                Order by 1
            """.format(tstamp1, tstamp2, worker)
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        pcw = BuildPiecewiseFunc(rows)
        hashrates[worker] = pcw
    print("all hashrates fetched in", datetime.now()-t)
    XSet = set()
    for ww in workers:
        w = ww[0]
        hr = hashrates[w]
        for pcw in hr:
            XSet = XSet | {p[0] for p in pcw["points"]}
    print("Xs len = ", len(XSet))

    Xs = sorted(XSet)
    points = []
    t = datetime.now()
    print(tmin, tmax)
    i = 0
    for x in Xs:
        i += 1
        y = 0
        for ww in workers:
            w = ww[0]
            hr = hashrates[w]
            for pcw in hr:
                y += EvalutePCWFunc(pcw, x)
        points.append((x, y))
        if i%100 == 0:
            print(x, y, len(points))


    print("all hashrates composed in", datetime.now()-t)

    sss = """{
                "rangeSelector": {
                    "selected": 1
                },
                "title": {
                    "text": "%s"
                },
                "series": []
            }"""%Param

    rez = json.loads(sss)
    t1 = rez['series']

    sr = json.loads("""{"name": "Hashrate","data": [],"tooltip": {"valueDecimals": 2}}""")
    dt = sr["data"]
    for p in points:
        dt.append(((p[0] + 5 * 3600) * 1000, p[1] / 1000))
    t1.append(sr)

    per = GetActualPeriod()
    return json.dumps(rez)

def AppendLinesToChart(points, per, chart):
    t1 = chart["series"]
    ttt = CalcBonus(points, per[0], per[1])

    bns = ttt["Decline"]
    print(len(bns))
    sr = json.loads("""{"name": "Bonus decline","data": [],"tooltip": {"valueDecimals": 2}}""")
    dt = sr["data"]
    for p in bns:
        dt.append(((p[0] + 5 * 3600) * 1000, p[1]))
    # t1.append(sr)

    grw = ttt["Growth"]
    print(len(grw))
    sr = json.loads("""{"name": "Bonus growth","data": [],"tooltip": {"valueDecimals": 2}}""")
    dt = sr["data"]
    for p in grw:
        dt.append(((p[0] + 5 * 3600) * 1000, p[1]))
    # t1.append(sr)

    avgH = ttt["AvgH"]
    print(len(avgH))
    sr = json.loads("""{"name": "Avg hashrate","data": [],"tooltip": {"valueDecimals": 2}}""")
    dt = sr["data"]
    for p in avgH:
        dt.append(((p[0] + 5 * 3600) * 1000, p[1]))
    t1.append(sr)

    cut = ttt["Cutoff"]
    print(len(cut))
    sr = json.loads("""{"name": "Cutoff","data": [],"tooltip": {"valueDecimals": 2}}""")
    dt = sr["data"]
    for p in cut:
        dt.append(((p[0] + 5 * 3600) * 1000, p[1]))
    t1.append(sr)

    # sr = json.loads("""{"name": "Value","data": [],"tooltip": {"valueDecimals": 2}}""")
    # dt = sr["data"]
    # for p in logHr:
    #     dt.append(((p[0] + 5 * 3600) * 1000, p[1] / 1000))
    # t1.append(sr)
    return(chart)


def GetWorkerHashrateChartJson():
    global Param

    per = GetActualPeriod()
    tstamp1 = per[0].timestamp()
    tstamp2 = per[1].timestamp()

    connection = sqlite3.connect("Claymon.sqlite")
    cursor = connection.cursor()
    t = datetime.now()
    sql = """
            Select startsecs, endsecs, AvgHashrate, uptime
            from WorkersHistory where
            EndSecs + 5 * 3600 Between {0} And {1} And Uptime > 0
            And Worker = '{2}'
            Order by 1
        """.format(tstamp1, tstamp2, Param)

    cursor.execute(sql)
    rows = cursor.fetchall()
    print("sql exec in:", datetime.now()-t)

    segments = BuildPiecewiseFunc(rows)

    print("selected rows: ", rows)
    print("len rows: ",len(rows))
    print("segments : ", segments)
    print("len segments: ", len(segments))
    # uptime - minutes, int

    sss = """{
                "rangeSelector": {
                    "selected": 1
                },
                "title": {
                    "text": "%s"
                },
                "series": []
            }"""%Param

    rez = json.loads(sss)
    t1 = rez['series']

    allpoints = []
    for seg in segments:
        points = seg["points"]
        sr = json.loads("""{"name": "Value","data": [],"tooltip": {"valueDecimals": 2}}""")
        dt = sr["data"]
        for p in points:
            dt.append(((p[0] + 5 * 3600) * 1000, p[1] / 1000))
            allpoints.append(p)
        t1.append(sr)

    chart = AppendLinesToChart(allpoints, per, rez)
    return json.dumps(chart)

def GetClayMonLogTotal():
    rez = []
    f = open('claymonlog.txt', 'r')
    for s1 in f:
        s2 = s1.split()
        if (s2[2] == "Total" and s2[3] == "Eth" and s2[4] == "hashrate:") or (s2[2] == "TotalHashrate"):
            s3 = s2[0].split('-')
            s4 = s2[1].split(':')
            dt = datetime(int(s3[0]), int(s3[1]), int(s3[2]), int(s4[0]), int(s4[1]), int(s4[2]))
            ts = dt.timestamp()
            if s2[2] == "TotalHashrate":
                tpl = (ts, round(1000 * float(s2[3])))
            else:
                tpl = (ts, round(1000 * float(s2[5])))
            rez.append(tpl)
            continue
    return rez

def GetRawPeriodsJson():
    connection = sqlite3.connect("Claymon.sqlite")
    cursor = connection.cursor()
    sql = """Select Startsecs*1000, AvgHashRate/1000, endsecs*1000, AvgHashrate/1000 From WorkersHistory Where worker = "M00" Order by 1 desc Limit 10"""
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    sss = """{
            "rangeSelector": {
                "selected": 1
            },
            "title": {
                "text": "Farm total hashrate"
            },
            "series": []
        }"""

    rez = json.loads(sss)
    t1 = rez['series']
    for r in results:
        sr = json.loads("""{"name": "Value","data": [],"tooltip": {"valueDecimals": 2}}""")
        dt = sr["data"]
        dt.append((r[0],r[1]))
        dt.append((r[2],r[3]))
        t1.append(sr)

    print(rez)
    return json.dumps(rez)

def GetAllWorkers():
    results = []
    connection = sqlite3.connect("Claymon.sqlite")
    cursor = connection.cursor()
    sql = """Select DISTINCT Worker From Workers"""
    for row in cursor.execute(sql):
        results.append(row[0])
    return results

def Test():
    sss1 = """{
            "rangeSelector": {
                "selected": 1
            },
            "title": {
                "text": "Test"
            },
            "series": []
        }"""

    connection = sqlite3.connect("Claymon.sqlite")
    cursor = connection.cursor()
    print(datetime.now())
    sql = """
        Select startsecs, endsecs, AvgHashrate, uptime
        from WorkersHistory where
        EndSecs > strftime('%s', 'now', 'localtime') - 5 * 3600 - 30 * 86400
        And Worker = 'M00' And Uptime >= 0 
        Order by 1
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(datetime.now())

    segments = []
    newseg = None
    for row in rows:
        if (newseg == None) and (row[2] > 0):
            newseg = {}
            newseg["min"] = row[0]
            newseg["max"] = row[1]
            newseg["lastuptime"] = row[3]
            points = []
            points.append((row[0], row[2]))
            points.append((row[1], row[2]))
            newseg["points"] = points
            continue
        if row[2] > 0:
            isectCond = row[0] <= newseg["max"] + 10
            uptimeCond = row[3] > newseg["lastuptime"]
            uptimeCond = uptimeCond and ((row[3] - newseg["lastuptime"]) * 60 - (row[1] - newseg["max"]) > -90)
            if isectCond or uptimeCond:
                points.append((row[0], row[2]))
                points.append((row[1], row[2]))
                if row[3] > newseg["lastuptime"]:
                    newseg["lastuptime"] = row[3]
                if row[1] > newseg["max"]:
                    newseg["max"] = row[1]
                continue
            points.sort()
            segments.append(newseg)
            newseg = None

            continue

    print(datetime.now())
    print(rows)
    print(len(rows))
    print(segments)
# uptime - minutes, int

    sss = """{
            "rangeSelector": {
                "selected": 1
            },
            "title": {
                "text": "Test"
            },
            "series": []
        }"""

    rez = json.loads(sss)
    t1 = rez['series']

    for seg in segments:
        points = seg["points"]
        print(len(points))
        sr = json.loads("""{"name": "Value","data": [],"tooltip": {"valueDecimals": 2}}""")
        dt = sr["data"]
        for p in points:
            dt.append(((p[0] + 5*3600)*1000,p[1]/1000))
        t1.append(sr)

    # for r in rows:
    #     sr = json.loads("""{"name": "Value","data": [],"tooltip": {"valueDecimals": 2}}""")
    #     dt = sr["data"]
    #     dt.append(((r[0] + 5*3600)*1000,r[2]/1000))
    #     dt.append(((r[1] + 5*3600)*1000,r[2]/1000))
    #     t1.append(sr)

    return json.dumps(rez)

app = Flask(__name__)

@app.route("/data.json")
def data():
    global Param
    workers = GetAllWorkers()
    if Param == "Agregate":
        return GetTotalHashrateChartJson()
    elif Param == "RawPeriods":
        return GetRawPeriodsJson()
    elif (Param) in workers:
        return GetWorkerHashrateChartJson()
    elif Param == "Test":
        return Test()
    elif Param == "Log":
        return GetClayMonLogTotal()
    else:
        return GetSandboxChartJson()

@app.route("/graph/<kind>")
def graph(kind):
    global Param
    Param = kind
    return render_template('graph.html')
 
 
if __name__ == '__main__':
    app.run(
    debug=True,
    threaded=True,
    host='0.0.0.0'
)