from hashlib import sha256
# username = input()
username = 'CNO@deeplightventures.com'
a = username + 'MySecretSalt153358'
b = bytearray(a, encoding = 'ANSI')
for i in range(87):
    b = sha256(b).digest()

SpecialChars = "!@#$%^&*=+"
SymbolsCount = 26 + 26 + 10 + 10
rarefac = 0
rez = ""
for c in b:
    d = c % SymbolsCount
    if d < 10:
        rez = rez + chr(0x30 + d)
    elif d < 10 + 26:
        rez = rez + chr(0x41 + d - 10)
    elif d < 10 + 26 + 26:
        rez = rez + chr(0x61 + d - 10 - 26)
    else:
        if rarefac == 2:
            rez = rez + SpecialChars[d - 10 - 26 - 26]
        rarefac += 1

print(rez)