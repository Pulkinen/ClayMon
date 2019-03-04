from bs4 import BeautifulSoup
import csv, os

def parse_ranks(raw_html):
    html = BeautifulSoup(raw_html, 'html.parser')
    ranks = []
    tbs = html.select('tbody')
    if len(tbs) < 1:
        return ranks
    tb = tbs[1]
    trs = tb.select('tr')
    for tr in trs:
        tds = tr.select("td")
        l = len(tds)
        if len(tds) == 8:
            rank = (tds[0].text, tds[1].text, tds[2].text, tds[6].text, tds[7].text)
            ranks.append(rank)
    return ranks


def get_url(start):
    return f"https://ethereum.miningpoolhub.com/index.php?page=account&action=transactions&start={start}"


ranks = []
try:
    # folder = "C:\\temp\\Vit\\"
    # folder = "C:\\temp\\Vit2\\"
    folder = "C:\\temp\\pulk\\"
    files = os.listdir(folder)
    print(files)
except:
    print('oops')

for fn in files:
    filename, ext = os.path.splitext(fn)
    if ext != ".html":
        continue

    fname = folder + fn
    f = open(fname, 'rb')
    raw_html = f.read()
    f.close()
    rs = parse_ranks(raw_html)
    for r in rs:
        ranks.append(r)
    if len(ranks) % 3000 == 0:
        print(fn, "ok", len(ranks)/30, len(files))

with open('pulk_raw.csv', 'w') as f:
    writer = csv.writer(f, lineterminator='\n')
    for rank in ranks:
        writer.writerow(rank)



# -*- coding: utf-8 -*
# from urllib.request import Request, urlopen
# import json, codecs
#
# Coin = ("adzcoin", "auroracoin-qubit", "bitcoin", "bitcoin-cash", "Bitcoin-Gold", "dash", "Digibyte-Groestl",
#         "Digibyte-Qubit", "Digibyte-Skein", "Electroneum", "Ethereum", "Ethereum-Classic", "Expanse", "Feathercoin",
#         "Gamecredits", "Geocoin", "Globalboosty", "Groestlcoin", "Litecoin", "Maxcoin", "Monacoin", "Monero",
#         "Musicoin",
#         "Myriadcoin-Groestl", "Myriadcoin-Skein", "Myriadcoin-Yescrypt", "Sexcoin", "Siacoin", "Startcoin",
#         "verge-scrypt",
#         "Vertcoin", "Zcash", "Zclassic", "Zcoin", "Zencash")
#
# #api_key = "&api_key=<>"
# ApiData = "ad0196499df19d20bbe67d96fc6431157a0d60815b6b21267af90a0c55074f8e"
# Api = "&api_key=" + ApiData
#
# Action = (
# "getminingandprofitsstatistics", "getautoswitchingandprofitsstatistics", "getuserallbalances", "getblockcount",
# "getblocksfound",
# "getblockstats", "getcurrentworkers", "getdashboarddata", "getdifficulty", "getestimatedtime", "gethourlyhashrates",
# "getnavbardata",
# "getpoolhashrate", "getpoolinfo", "getpoolsharerate", "getpoolstatus", "gettimesincelastblock", "gettopcontributors",
# "getuserbalance",
# "getuserhashrate", "getusersharerate", "getuserstatus", "getusertransactions", "getuserworkers", "public")
#
#
# def menu_coin():
#     global c
#     try:
#         c
#     except NameError:
#         for index, group in enumerate(Coin):
#             print("%s: %s" % (index, group))
#         c = int(input("coin to choose: "))
#         print("selected: ", Coin[c])
#     else:
#         print("default coin: ", Coin[c])
#
#
# def menu_action():
#     global a
#     try:
#         a
#     except NameError:
#         for index, group in enumerate(Action):
#             print("%s: %s" % (index, group))
#         a = int(input("action: "))
#         print("selected:", Action[a])
#     else:
#         print("default Action: ", Action[a])
#
#
# def fonction(c):
#     Url = "https://" + Coin[c] + ".miningpoolhub.com/index.php?page=api&action=" + Action[a] + Api
#     print("url:", Url)
#     Url = "https://ethereum.miningpoolhub.com/index.php?page=account&action=transactions&start=30"
#
#     hdrs = {}
#     hdrs["accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
#     hdrs["accept-encoding"] = "gzip, deflate, br"
#     hdrs["accept-language"] = "ru, en - US"
#     hdrs["accept-language"] = "ru,en-US;q=0.9,en;q=0.8,de;q=0.7"
#     hdrs["cookie"] = "__cfduid=d31beeca2b5914a79f18077d77ffa20741528812634; _ga=GA1.2.33596953.1528819843; _gid=GA1.2.414182250.1540711196; PHPSESSID=qhb4hlbeke0vjceqhkfq0hm8m2"
#     hdrs["dht"] = 1
#     hdrs["referer"] = "https://ethereum.miningpoolhub.com/index.php?page=account&action=transactions"
#     hdrs["upgrade-insecure-requests"] = 1
#     hdrs["user-agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"
#
#     Req = Request(Url, headers=hdrs)
#     Webpage = urlopen(Req).read()
#     sss = Webpage.decode()
#     print(sss)
#
#
#
# c = 11  # comment to enable coin menu selection
#
# a=22 #comment to enable action menu selection
#
# menu_coin()
# menu_action()
# print()
# fonction(c)
