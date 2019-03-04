import csv

rez = {}
with open('pulkRaw1.csv', 'r') as f:
    reader = csv.reader(f, delimiter = "\t", lineterminator='\n')
    for row in reader:
        dt = row[1]
        if dt in rez:
            rez[dt] += float(row[5])
        else:
            rez[dt] = float(row[5])

with open('pulkAgg.csv', 'w') as f1:
    writer = csv.writer(f1, delimiter = ";", lineterminator='\n' )
    for dt in rez:
        writer.writerow((dt, rez[dt]))

f.close()
f1.close()
