import pandas as pd
import shelve
from math import sin, cos, sqrt, atan2, radians
from time import time
from tqdm import tqdm
import pgeocode

print('reading excel and coordinat file...')
start = time()
file = r'Data/dataset.xlsx'
df = pd.read_excel(file)
file2 = r'Data/IsoChange.txt'
cd = pd.read_csv(file2, sep="\t", header = None, low_memory=False, names= ["country", "iso2", "iso3", "4"])
print('reading excel file and coordinat file completed (', round(time() - start, 2), 'sec )')
df2 = pd.DataFrame()

def isSafe(i, j):
    if str(df.at[i,'Destination Postal Code']) == str(df.at[j,'Origin Postal Code']) :
        if str(df.at[i,'Destination Postal Code']) != str(df.at[i,'Origin Postal Code']) :
            if str(df.at[j,'Destination Postal Code']) != str(df.at[j,'Origin Postal Code']) :
                if ( df.at[i,'Delivery Date'] <= df.at[j,'Goods issue date'] ) :
                    if ( (df.at[j,'Goods issue date'] - df.at[i,'Delivery Date']).total_seconds() <= 7*24*60*60 )  :
                        if ( str(df.at[i,'ID']) != str(df.at[j,'ID']) ):
                            if ( df.at[i,'Delivery Date'] > df.at[i,'Goods issue date'] ):
                                if ( df.at[j,'Delivery Date'] > df.at[j,'Goods issue date'] ):
                                    return True

def knightsT(x): #GÜVENLİ YOLLARI ISSAFE FONKSİYONU İLE TEST EDİP X MATRİSİNE AKTARIYOR
    print('data işlenip matrix e aktarılıyor...')
    for i in tqdm(range(len(df))):
        for j in range(len(df)):
            if isSafe(i, j):
                x[i][1].append(j) #i nin gittiği yer dizisine j yi koyuyor
                x[j][0].append(i) #j nin geldiği yer dizisine i yi koyuyor

def check(i,j,k): # çek fonksiyonu
    if k == 0:
        a='Origin Postal Code'
        b='Destination Postal Code'
        if str(df.at[ i, a]) == str(df.at[ j, b]): #i ve j yi posta koduna göre doğruluyor
            return True
    if k == 1:
        if distanceChecker(i,j) < 100 :
            return True
    return False

def isClosed(i,o,d,road,k):
    road.append(i)
    if check(o,i,k):
        if len(x[i][1]) == 0 :
            return True
    if len(x[i][1]) > 0:
        for j in range(len(x[i][1])):
            j=x[i][1][j]
            if ( df.at[j,'Goods issue date'] <= df.at[d,'Goods issue date'] ) :
                if isClosed(j,o,d,road,k):
                    return True
    if i != o :
        road.remove(i)

def roadRemover(road): #KULLANILAN YOLLARI X MATRİSİNDEN SİLEN FONKSİYOR
    print(road[0] ,'için rota bulundu:', road)
    for k in range(len(road)):
        for i in range(len(df)):
            if road[k] in x[i][0] :    x[i][0].remove(road[k])
            if road[k] in x[i][1] :    x[i][1].remove(road[k])
        x[road[k]][0] = []
        x[road[k]][1] = []

def destCheck(i,k): # başlangıç noktasının gidebileceği bir yer var mı? fonksiyonu
    tmp_time = df.at[i, 'Goods issue date']
    tmp = -1
    for d in range(len(df)):
        if len(x[d][1]) == 0 and len(x[d][0]) > 0 : #d'nin bitiş noktası olup olmadığını kotnrol ediyor
            if ( df.at[i,'Delivery Date'] <= df.at[d,'Goods issue date'] ) :
                if check(i,d,k) :
                    if df.at[d,'Goods issue date'] > tmp_time:
                        tmp = d
                        tmp_time = df.at[d,'Goods issue date']
    return tmp

def ifClosed(k,final): #BAŞLANGIÇ VE BİTİŞ NOKTALARINI BULUP YOLU TEST EDİYOR
    starttt = time()
    for i in range(len(df)):
        if len(x[i][1]) > 0 and len(x[i][0]) == 0 : #başlangıç noktasını buluyor
            dest = -1
            dest = destCheck(i,k) # başlangıç noktasının gidebileceği bir yer var mı? - fonksiyonuna gider
            if dest != -1 :
                road = []
                if isClosed(i,i,dest,road,k):
                    final.append(road) #ROTALARI TUTAN DİZİYE EKLİYOR
                    print('sürede yol bulundu: (', round(time() - starttt, 2), 'sec )')
                    roadRemover(road) #KULLANILAN YOLLARIN SİLİNMESİ İÇİN roadREMOVER FONKSİYONUNA GÖNDERİLİYOR
                    return True

def koordinatlamav2(cds): #HAM DATA >> CDS MATRİSİ >> KOORDİNALAR >> CDS >> HAM DATA
    print('datadaki iso3 ülke kodları iso2 ye çevriliyor..')
    for i in tqdm(range(len(df))):
        for j in range(len(cd)):
            if df.at[i, 'Destination Country Key'] == cd.at[j, 'iso3']:
                df.at[i, 'Destination Country Key'] = cd.at[j, 'iso2']
            if df.at[i, 'Origin Country Key'] == cd.at[j, 'iso3']:
                df.at[i, 'Origin Country Key'] = cd.at[j, 'iso2']
            if df.at[i, 'Destination Country Key'] == 'UK':
                df.at[i, 'Destination Country Key'] = 'GB'
            if df.at[i, 'Origin Country Key'] == 'UK':
                df.at[i, 'Origin Country Key'] = 'GB'
    for i in range(len(df)):
        #posta kodu ve ülkü datası toplanıyor
        if df.at[i, 'Origin Postal Code'] not in cds[0] :
            cds[0].append(df.at[i, 'Origin Postal Code'])
            cds[1].append(df.at[i, 'Origin Country Key'])
        if df.at[i, 'Destination Postal Code'] not in cds[0] :
            cds[0].append(df.at[i, 'Destination Postal Code'])
            cds[1].append(df.at[i, 'Destination Country Key'])
    print('koordinatlar postakodlarına yazılıyor..')
    for i in tqdm(range(len(cds[0]))):
        if cds[1][i] != "GR":
            cds[2].append(pgeocode.Nominatim(cds[1][i]).query_postal_code(cds[0][i]).latitude)
            cds[3].append(pgeocode.Nominatim(cds[1][i]).query_postal_code(cds[0][i]).longitude)
        else:
            cds[2].append(38.021332)
            cds[3].append(23.798630)
    print('koordinatlar dataya ekleniyor..')
    for i in tqdm(range(len(df))):
        for j in range(len(cds[0])):
            if df.at[i, 'Origin Postal Code'] == cds[0][j]:
                df.at[i,'lat'] = cds[2][j]
                df.at[i,'lon'] = cds[3][j]
            if df.at[i, 'Destination Postal Code'] == cds[0][j]:
                df.at[i,'lat2'] = cds[2][j]
                df.at[i,'lon2'] = cds[3][j]

def distanceChecker(a,b): #TESLİMATLAR ARASINDAKİ UZAKLIĞI VEREN FONKSİYON
    if(b==-1):              #B DEĞERİNİ -1 VERDİĞİMİZDE TEK TESLİMATIN GİTTİ UZAKLIK DÖNÜYOR
        lat2 = radians(df.at[a,'lat2'])
        lon2 = radians(df.at[a,'lon2'])
    else:
        lat2 = radians(df.at[b,'lat2'])
        lon2 = radians(df.at[b,'lon2'])
    R = 6373.0
    lat1 = radians(df.at[a,'lat'])
    lon1 = radians(df.at[a,'lon'])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

def checkmate(t,final):
    devamke = True
    while devamke == True :
        if ifClosed(0,final) == True:
            t = t + 1
            print('kapalı döngü bulundu')
            devamke = True
        elif ifClosed(1,final) == True:
            t = t + 1
            print('100km az mesafeye dönüldü')
            devamke = True
        else:
            devamke = False
    return t

def sectoD(time):
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return ('%d day(s) %02d:%02d:%02d' %(day, hour, minutes, seconds))

def yaz(final):
    print('bulunan rotalar dosyalanıyor...')
    m=0
    for i in tqdm(range(len(final))):
        df2.at[m,0]  = '-----ID-----'
        df2.at[m,1]  = '--Origin Postal Code--'
        df2.at[m,2]  = '---Goods issue date---'
        df2.at[m,3]  = '-----Transit time-----'
        df2.at[m,4]  = '-----Delivery Date-----'
        df2.at[m,5]  = '--Destination Postal Code--'
        df2.at[m,6]  = '---Destination ID---'
        df2.at[m,7]  = '-------------'
        df2.at[m,8]  = '-------------'
        df2.at[m,9]  = '-------------'
        df2.at[m,10] = '-----Total Wt-----'
        df2.at[m,11] = '--Total Distance--'
        m = m + 1
        w = 1
        wt = 0
        dt = 0
        for j in range(len(final[i])):
            k=final[i][j]
            df2.at[m,0]=df.at[k,'ID']
            df2.at[m,1]=str(df.at[k,'Origin Postal Code'])
            df2.at[m,2]=df.at[k,'Goods issue date']
            df2.at[m,3]=sectoD(abs((df.at[k,'Delivery Date'] - df.at[k,'Goods issue date']).total_seconds()))
            df2.at[m,4]=df.at[k,'Delivery Date']
            df2.at[m,5]=str(df.at[k,'Destination Postal Code'])
            df2.at[m,7]=str('%.1f'%(distanceChecker(k,-1))) + ' km'
            dt = dt + distanceChecker(k,-1)
            if j != len(final[i])-1:
                df2.at[m,6]=df.at[final[i][j+1],'ID']
                #m = m + 1
                df2.at[m,8]='wt' + str(w)
                df2.at[m,9]=sectoD(abs((df.at[final[i][j],'Delivery Date'] - df.at[final[i][j+1],'Goods issue date']).total_seconds()))
                wt=wt+abs((df.at[final[i][j],'Delivery Date'] - df.at[final[i][j+1],'Goods issue date']).total_seconds())
                w = w + 1
                m = m + 1
            else:
                df2.at[m,6]='######'
                df2.at[m,10]=sectoD(wt)
                df2.at[m,11]=str('%.1f'%dt) + ' km'
                m=m+1
    df2.sort_index(inplace=True)
    df2.sort_index(inplace=True,axis=1)
    df2.to_excel("output.xlsx", index=False, header=False)



final = []
x = []
for i in range(len(df)):
    x.append([[]])
    x[i].append([])

cds = []
for i in range(4):
    cds.append([])

koordinatlamav2(cds)

knightsT(x)

start = time()
t=0
t=checkmate(t,final)
print(t,'adet rota bulundu, suresi: (', round(time() - start, 2), 'sec )')

yaz(final)
