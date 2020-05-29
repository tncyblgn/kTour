import pandas as pd
import shelve
from math import sin, cos, sqrt, atan2, radians
from time import time
from tqdm import tqdm
import pgeocode
from typing import List


file = r'Data/dataset.xlsx'
df = pd.read_excel(file)
df = df.sort_values('Goods issue date')
file2 = r'Data/IsoChange.txt'
cd = pd.read_csv(file2, sep="\t", header = None, low_memory=False, names= ["country", "iso2", "iso3", "4"])
df2 = pd.DataFrame()

TOLERANCE = 7 * 24 * 60 * 60
BBB = 40


def koordinatlamav3(df): #HAM DATA >> CDS MATRİSİ >> KOORDİNALAR >> CDS >> HAM DATA
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
    for i in range(len(df)):
        df.at[i,'distance'] = distanceChecker(i,-1)
    #print('data ayiklaniyor..')
    for i in range(len(df)):
        if abs((df.at[i,'Delivery Date'] - df.at[i,'Goods issue date']).total_seconds()) == 0: fals.append(i)
    df.drop(index=fals, inplace=True)
    df = df[df.lat.notnull()]
    df = df[df.lat2.notnull()]
    df.to_excel('./Data/ayik.xlsx', index = False)
    df = pd.read_excel(r'Data/ayik.xlsx', index = False)
    df = df.sort_values('Goods issue date')

    return df

def isSafe(i, j):
    if str(df.at[i,'Destination Postal Code']) == str(df.at[j,'Origin Postal Code']) :
        if str(df.at[i,'Destination Postal Code']) != str(df.at[i,'Origin Postal Code']) :
            if str(df.at[j,'Destination Postal Code']) != str(df.at[j,'Origin Postal Code']) :
                if ( df.at[i,'Delivery Date'] <= df.at[j,'Goods issue date'] ) :
                    if ( (df.at[j,'Goods issue date'] - df.at[i,'Delivery Date']).total_seconds() <= TOLERANCE )  :
                        if ( str(df.at[i,'ID']) != str(df.at[j,'ID']) ):
                            if ( df.at[i,'Delivery Date'] > df.at[i,'Goods issue date'] ):
                                if ( df.at[j,'Delivery Date'] > df.at[j,'Goods issue date'] ):
                                    return True

def knightsT(): #GÜVENLİ YOLLARI ISSAFE FONKSİYONU İLE TEST EDİP X MATRİSİNE AKTARIYOR
    x = []
    for i in range(len(df)):
        x.append([[]])
        x[i].append([])
    print('data işlenip matrix e aktarılıyor...')
    for i in tqdm(range(len(df))):
        for j in range(len(df)):
            if isSafe(i, j):
                x[i][1].append(j) #i nin gittiği yer dizisine j yi koyuyor
                x[j][0].append(i) #j nin geldiği yer dizisine i yi koyuyor
    return x

def check(i,j,k): # çek fonksiyonu
    if (k == 0) or (k == 2):
        if str(df.at[ i, 'Origin Postal Code']) == str(df.at[ j, 'Destination Postal Code']): #i ve j yi posta koduna göre doğruluyor
            return True
    if (k == 1) or (k == 3):
        if distanceChecker(i,j) < 100 :
            return True
    if k == -1:
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
    if k == 0 or k == 1:
        for d in range(len(df)):
            if len(x[d][1]) == 0 and len(x[d][0]) > 0 : #d'nin bitiş noktası olup olmadığını kotnrol ediyor
                if ( df.at[i,'Delivery Date'] <= df.at[d,'Goods issue date'] ) :
                    if check(i,d,k) :
                        if df.at[d,'Goods issue date'] > tmp_time:
                            tmp = d
                            tmp_time = df.at[d,'Goods issue date']
    else:
        for d in range(len(df)):
            if len(x[d][0]) > 0 : #d'nin bitiş noktası olup olmadığını kotnrol ediyor
                if ( df.at[i,'Delivery Date'] <= df.at[d,'Goods issue date'] ) :
                    if check(i,d,k) :
                        if df.at[d,'Goods issue date'] > tmp_time:
                            tmp = d
                            tmp_time = df.at[d,'Goods issue date']
    return tmp

def ifClosed(k,final): #BAŞLANGIÇ VE BİTİŞ NOKTALARINI BULUP YOLU TEST EDİYOR
    starttt = time()
    for i in range(len(df)):
        dest = -1
        if (k == 0) or (k == 1):
            if len(x[i][1]) > 0 and len(x[i][0]) == 0 : #başlangıç noktasını buluyor
                dest = destCheck(i,k) # başlangıç noktasının gidebileceği bir yer var mı? - fonksiyonuna gider
        elif (k == 2) or (k == 3):
            if len(x[i][1]) > 0 :
                dest = destCheck(i,k) # başlangıç noktasının gidebileceği bir yer var mı? - fonksiyonuna gider
        if dest != -1 :
            road = []
            if isClosed(i,i,dest,road,k):
                if k != -1 :final.append(road) #ROTALARI TUTAN DİZİYE EKLİYOR
                print('sürede yol bulundu: (', round(time() - starttt, 2), 'sec )')
                roadRemover(road) #KULLANILAN YOLLARIN SİLİNMESİ İÇİN roadREMOVER FONKSİYONUNA GÖNDERİLİYOR
                return True

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
    devam = True
    while devam == True :
        if ifClosed(0,final) == True:
            t = t + 1
            print('kapalı döngü bulundu')
            devam = True
        elif ifClosed(1,final) == True:
            t = t + 1
            print('100km az mesafeye dönüldü')
            devam = True

        elif ifClosed(2,final) == True:
            t = t + 1
            print('kapalı döngü bulundu 2')
            devam = True
        elif ifClosed(3,final) == True:
            t = t + 1
            print('100km az mesafeye dönüldü 3')
            devam = True

        elif ifClosed(-1,final) == True:
            t = t + 1
            print('sallama mesafeye dönüldü')
            devam = True

        else :
            devam = False
    return t


def all_elements(lst):
    all_elem = []
    for i in lst:
        for j in i:
            if j not in all_elem: all_elem.append(j)
    return all_elem

def Fischer(all_list, lst):
    for i in all_list:
        for j in i:
            if j in lst:
                for k in i:
                    if k not in lst:
                        lst.append(k)
                        return Fischer(all_list, lst)
    return lst

def Corona(lst, alllst):
    tmp = alllst.copy()
    r = True
    while r:
        r = False
        for i in tmp:
            for j in i:
                if j in lst:
                    tmp.remove(i)
                    r = True
                    break
    return tmp

def list_value(lst):
    total_d = 0
    total_w = 0
    dist = 0
    for i in range(len(lst)):
        total_d += distanceChecker(lst[i],-1)
        if lst[i] != lst[-1]: total_w += (abs((df.at[lst[i],'Delivery Date'] - df.at[lst[i+1],'Goods issue date']).total_seconds()))/(60*60*24)
        else: dist = distanceChecker(lst[0] ,lst[-1])
    #return total_d - (total_w*rpd)
    #print('(',total_d, '-', dist,')/','2-', '(',BBB,'*',total_w,') - (',dist,')')
    return ((total_d - dist)/2 - (BBB*total_w) - (dist))

def f_value(lst):
    total = 0
    for i in lst: total += list_value(i)
    return total

def Alekhine3(a, tmp, lst, ff):
    tmp.append(a)
    tmp_l = Corona(a, lst)
    if lst != []:
        for i in lst:
            ff = Alekhine(i, tmp, tmp_l, ff)
    else:
        val = f_value(tmp)
        if val > ff[1]:
            ff[0] = tmp.copy()
            ff[1] = val
            print(ff)
    tmp.remove(a)
    return ff

def Cleaner(LoL, lst):
    r = 1
    while r == 1:
        r = 0
        tmp = []
        for i in LoL:
            for j in i:
                if j not in tmp:
                    tmp.append(j)
                elif j != []:
                    i.remove(j)
                    r = 1
        for i in LoL:
            if i == [[]]:
                LoL.remove(i)
                r = 1
            for j in range(len(i)):
                if j != 0:
                    if i[j] == []:
                        i.remove([])
                        r = 1

    LoL = [LoL, []]
    for i in range(len(LoL[0])):
        LoL[1].append([])
        for j in range(len(LoL[0][i])):
            LoL[1][i].append(list_value(LoL[0][i][j]))
            #print(list_value(LoL[0][i][j]))
    LoL.append(lst)
    return LoL

def checker(list1,list2):
    for i in list1:
        for j in i:
            for k in list2:
                if j == k: return False
    return True

def Alekhine(temp_list, A, lst, lst_v, ff ):
    if checker(lst, [temp_list[2][A]]):
        for i in range(len(temp_list[0][A])):
            if checker(lst,temp_list[0][A][i]):
                lst.append(temp_list[0][A][i])
                lst_v += temp_list[1][A][i]
                if A+1 < len(temp_list[0]):
                    ff = Alekhine(temp_list, A+1, lst, lst_v, ff)
                else:
                    if lst_v > ff[1]:
                        ff[1] = lst_v
                        ff[0] = lst.copy()
                        #print(ff[1],ff[0])

                lst.remove(temp_list[0][A][i])
                lst_v -= temp_list[1][A][i]
    return ff

def Sumuklu_Cansu(lst):
    a_list = lst.copy()
    final  = []
    total  = 0

    while a_list != []:
        tmp = 0
        f_list = []
        for i in a_list:
            if len(i)>tmp : tmp = len(i)
        for i in a_list:
            if len(i)==tmp : f_list.append(i)
        print(tmp)
        f_list_c = all_elements(f_list)
        while f_list_c != []:
            tmp = []
            tmp.append(f_list_c[0])
            lst = Fischer(f_list, tmp)
            for i in lst:
                for j in f_list_c:
                    if i == j: f_list_c.remove(i)
            temp_list = []
            for i in range(len(lst)):
                temp_list.append([])   ##############################
                temp_list[i].append([])
                for j in f_list:
                    for k in j:
                        if k == lst[i]:
                            if j not in temp_list[i]: temp_list[i].append(j)
            #print(temp_list, lst)
            temp_list = Cleaner(temp_list, lst)
            #print(temp_list)
            tmp_final = []
            final_v = 0
            tmp_final_v = 0
            ff = [[], 0]


            ff = Alekhine(temp_list, 0, tmp_final, tmp_final_v, ff)
            final.append(ff[0])
            total += ff[1]
            #print (ff)
            for i in ff[0]:
                a_list = Corona(i, a_list)

        #for i in f_list: a_list.remove(i)


    b = []
    for i in final:
        for j in i:
            if j != []:
                b.append(j)
    print(b)
    tmp = []
    for i in b:
        for j in i:
            if j not in tmp:
                tmp.append(j)
            else:
                print('asdasd')

    print(total)

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
        df2.at[m,12] = '--Kalan Mesafe--'
        df2.at[m,13] = '--Saving--'  ### saving sonucu  -  total saving.
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
                df2.at[m,12]=str('%.1f'%(distanceChecker( final[i][0], final[i][-1]))) + ' km'
                m=m+1
    df2.sort_index(inplace=True)
    df2.sort_index(inplace=True,axis=1)
    df2.to_excel("output.xlsx", index=False, header=False)


#df = koordinatlamav3(df)

class Route:

    def __init__(self, index, no, A, B, issue, delivery, distance):
        self.index = index
        self.no = no
        self.A = A # for backwards compatibility
        self.B = B  # for backwards compatibility
        self.origin = A
        self.destination = B
        self.issue = issue
        self.delivery = delivery
        self.distance = distance

    def __str__(self):
        return "({}, {}, {}, {}, {}, {} km)".format(self.no, self.origin, self.destination, self.issue.strftime('%d/%m/%Y'), self.delivery.strftime('%d/%m/%Y'), str('%.1f'%(self.distance)))#

class Pair:
    def __init__(self, first, second):
        self.first: Route = first
        self.second: Route = second

    def get_start_time(self):
        return self.first.issue

    def get_end_time(self):
        return self.second.delivery

    def get_anchor_location(self):
        return self.first.origin

    def __str__(self):
        return "({}, {})".format(self.first, self.second)

class Match:
    def __init__(self, pairs = []):
        self.pairs = pairs

    def append_pair(self, pair):
        self.pairs.append(pair)

    def get_end_time(self):
        return self.pairs[-1].get_end_time()

    def contains(self, pair):
        if pair in self.pairs:
            return True
        for p in self.pairs:
            if pair.first == p.second or pair.second == p.first:
                return True
        return False

    def __str__(self):
        return "[{}]".format(", ".join([str(p) for p in self.pairs]))

def calculate2():
    global rows

    routes: List[Route] = []
    for row in rows:
        routes.append(Route(0, row[0], row[1], row[2], row[3], row[4], row[5]))

    pairs: List[Pair] = []

    total_routes = len(routes)
    for i, r1 in enumerate(routes):
        for j, r2 in enumerate(routes):
            if r1.origin == r2.destination and r2.origin == r1.destination and r1 != r2 and r1.origin != r1.destination and r2.origin != r2.destination:
                time_difference = (r2.issue - r1.delivery).total_seconds()
                if time_difference < TOLERANCE and time_difference >= 0:
                        pairs.append(Pair(r1, r2))

        if i % 100 == 0:
            print("{:02.0f}%".format(100 * (i + 1) / total_routes), end='\r')

    matches: List[Match] = []

    total_pairs = len(pairs)
    for i, p1 in enumerate(pairs):
        match: Match = Match([p1])
        for j, p2 in enumerate(pairs):
            if i == j or p2.get_start_time() < p1.get_start_time() or p1.get_anchor_location() != p2.get_anchor_location():
                continue
            time_delta = (p2.get_start_time() - match.get_end_time()).total_seconds()
            if time_delta >= 0 and time_delta < TOLERANCE and not match.contains(p2):
                    match.append_pair(p2)
        if i % 50 == 0:
            print("{:02.0f}% ".format(100 * (i + 1) / total_pairs), end='\r')

        matches.append(match)

    print('Total matches:', len(matches))
    f = open('res.txt', 'w')

    for i, m in enumerate(matches):
        f.write("{} - {}\n".format(i, m))
    f.close()

    output_list = [match.pairs for match in matches]
    output_df = pd.DataFrame(output_list)
    output_df.to_excel('res.xlsx')

    output_list_2 = []
    for i, match in enumerate(matches):
        output_list_2.append([])
        for p in match.pairs:
            output_list_2[i].append(p.first.no)
            output_list_2[i].append(p.second.no)

    """output_df = pd.DataFrame(output_list_2)
    output_df.to_excel('ids.xlsx')"""

    tmp = []
    for i in range(len(output_list_2)):
        tmp.append([])
        for j in output_list_2[i]:
            for k in range(len(df)):
                if j == df.at[k, 'ID']:
                    tmp[i].append(k)
                    break


    return tmp

df = koordinatlamav3(df)

rows = []
for row in df.values:
    line = [row[0], row[7], row[12], row[14], row[15], row[29] ]
    #line = ['ID', 'Origin Postal Code', 'Destination Postal Code', 'Goods issue date', 'Delivery Date', 'distance' ]
    rows.append(line)

matches = calculate2()

x = knightsT()
t = 0
final = []
t = checkmate(t,final)
all_List = matches.copy()
for i in final: all_List.append(i)
#print(all_List)
Sumuklu_Cansu(all_List)
