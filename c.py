import pandas as pd
import time
from tqdm import tqdm
import pgeocode
from math import sin, cos, sqrt, atan2, radians

#file = r'Data/dataset.xlsx'
df = pd.read_excel(r'Data/dataset.xlsx')
cd = pd.read_csv(r'Data/IsoChange.txt', sep="\t", header = None, low_memory=False, names= ["country", "iso2", "iso3", "4"])

#DERINLIK = 5

print('Hesaplama derinliğini girin:')
DERINLIK = input()
DERINLIK = int(DERINLIK)

###------------------------------------------------
###------------------------------------------------
def koordinatlamav2(df): #HAM DATA >> CDS MATRİSİ >> KOORDİNALAR >> CDS >> HAM DATA
    cds = []
    for i in range(4): cds.append([])
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
    fals = []
    print('data ayiklaniyor..')
    for i in range(len(df)):
        if abs((df.at[i,'Delivery Date'] - df.at[i,'Goods issue date']).total_seconds()) == 0: fals.append(i)
    df.drop(index=fals, inplace=True)
    df = df[df.lat.notnull()]
    df = df[df.lat2.notnull()]
    df.to_excel('ayik.xlsx', index = False)
    df = pd.read_excel(r'ayik.xlsx', index = False)
    return df

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
###------------------------------------------------
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

def knightsT(): #GÜVENLİ YOLLARI ISSAFE FONKSİYONU İLE TEST EDİP X MATRİSİNE AKTARIYOR
    x = []
    print('data işlenip matrix e aktarılıyor...')
    for i in tqdm(range(len(df))):
        x.append([])
        for j in range(len(df)):
            if isSafe(i, j):
                x[i].append(j) #i nin gittiği yer dizisine j yi koyuyor
    return x
###------------------------------------------------
def ifClosed():
    roads = []
    for i in range(len(df)):
        if len(x[i]) > 0 :
            dests = []
            for d in range(len(df)):
                if ( df.at[i,'Delivery Date'] <= df.at[d,'Goods issue date'] ) :
                    if (str(df.at[ i, 'Origin Postal Code']) == str(df.at[ d, 'Destination Postal Code']) and (i != d)) :
                        dests.append(d)
            if dests != []:
                road = []
                tmp = df.at[i,'Delivery Date']
                der = 0
                for d in dests:
                    if tmp < df.at[d,'Goods issue date']: tmp = df.at[d,'Goods issue date']
                print(i)
                roads = isClosed(i,dests,road,roads,tmp,der)
                print('\r' , end="")
                print('\x1b[2K\r', i, len(dests), len(roads))
    return roads

def isClosed(i,dests,road,roads,tmp,der):
    road.append(str(i))
    if i in dests :
        a=[]
        for k in road: a.append(int(k))
        roads.append(a)
        print('\x1b[2K\r', road, end='\r')
    elif (len(x[i]) > 0) and (der < DERINLIK) :
        der = der + 1
        for k in range(len(x[i])):
            j=x[i][k]
            if (j not in road) and (tmp >= df.at[j,'Goods issue date']):
                allList = isClosed(j,dests,road,roads,tmp,der)
    road.remove(str(i))
    return roads
###------------------------------------------------
def dis_val():
    total_dis  = 0
    total_time = 0
    ort = 0
    for i in tqdm(range(len(df))):
        total_dis  += distanceChecker(i,-1)
        total_time += abs((df.at[i,'Delivery Date'] - df.at[i,'Goods issue date']).total_seconds())
    return (total_dis*60*60*24)/total_time
def list_value(lst):
    total_d = 0
    total_w = 0
    for i in range(len(lst)):
        total_d += distanceChecker(lst[i],-1)
        if lst[i] != lst[-1]: total_w += (abs((df.at[lst[i],'Delivery Date'] - df.at[lst[i+1],'Goods issue date']).total_seconds()))/(60*60*24)
    return total_d - (total_w*rpd)
def value_checker(final, lst):
    change_list = []
    for l in lst:
        for i in range(len(final)):
            for j in final[i]:
                if l == j:
                    if final[i] not in change_list:
                        change_list.append(final[i])
    return change_list
###------------------------------------------------

final = []

df = koordinatlamav2(df)

x = knightsT()

allList = ifClosed()

rpd = dis_val()

final = []
final_w = []
final_t = 0
allList_w = []
final_else = []

for i in tqdm(allList):
    change_list = value_checker(final, i)
    if change_list == []:
        final.append(i)
    else:
        final_else.append(i)
        final_c  = final
        final_ct = final_t
        for l in change_list:
            final_c.remove(l)
            final_ct -= list_value(l)
        for l in final_else:
            if value_checker(final, l) == []:
                final_c.append(l)
                final_ct += list_value(l)
        if final_ct > final_t:
            final   = final_c
            final_t = final_ct

print(final)
