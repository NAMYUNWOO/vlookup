#-*- coding: utf-8 -*-
import os,re,sys
import pandas as pd 
from datetime import datetime
sys.setrecursionlimit(10000)
MAX_ROW = 1000000

"""
# depricated
def findaddtag(x):
    try:
        res = re.findall("addtag=([^\&]+)\&",x)
        return str(res[0]).strip()
    except:
        return ""

def findctag(x):
    try:
        res = re.findall("ctag=([^\&]+)\&",x)
        return str(res[0]).strip()
    except:
        return ""
"""

def getHead(arri):
    return list(map(lambda x: x.strip() ,arri.split(",")))

def getRow(arri):
    arri_arr = list(map(lambda x: x.strip() ,arri.split('","')))
    arri_arr[0] = re.sub('"',"",arri_arr[0])
    arri_arr[-1] = re.sub('"',"",arri_arr[-1])
    return arri_arr

def findtag(x,head):
    try:
        res = re.findall(head+"([^\&]+)\&",x)
        return str(res[0]).strip()
    except:
        return ""

def cleandf(df,fkeyCol):
    
    df.columns = list(map(lambda x : str(x).strip(),df.columns))
    if fkeyCol not in df.columns:
        print("'{}' 열이 존재하지 않습니다".format(fkeyCol))
        print("프로그램을 종료합니다 아무키나 누르세요",end="")
        input()
        raise SystemExit
        

    df = df[~ (df[fkeyCol].isnull())]
    df[fkeyCol] = df[fkeyCol].map(lambda x : str(x).strip().lower())
    df = df[df[fkeyCol] != ""]
    print("total {} keywords in file".format(len(df)))
    
    return df


def getTable(keyCol=None):
    def __getTable(f,encoding):
        try:
            return pd.read_csv(f,encoding=encoding,na_filter = False,dtype={keyCol:str})
        except:
            return pd.read_csv(f,encoding=encoding,na_filter = False)

    return __getTable

def getTable_fs(f,encoding):
    
    f = open(f,encoding=encoding)
    arr = list(map(lambda x : str(x).strip().lower() ,f.readlines()))
    f.close()
    cols = getHead(arr[0])
    rows = list(map(lambda arri:getRow(arri),arr[1:]))
    df = pd.DataFrame(rows,columns=cols)
    return df

def getdf(f,funclist):
    for func in funclist:
        for enc in ["utf-8","cp949","euc-kr"]:
            try:
                res = func(f,enc)
                return res
            except:
                continue
    print("file error")
    return 0
    
def main():
    
    global MAX_ROW
    curdir = "/".join(os.getcwd().split("\\"))
    basedir =  curdir + "/origin/"
    keydir =  curdir + "/key/"
    keyfiles = list(map(lambda x : keydir + x, os.listdir(keydir)))
    
    files = list(map(lambda x : basedir + x, os.listdir(basedir)))

    print("{} 개의 key-file이 발견되었습니다.".format(len(keyfiles)))
    for kf in keyfiles:
        print("   {}".format(kf))
    print("")
    print("{} 개의 target-file이 발견되었습니다.".format(len(files)))
    for tf in files:
        print("   {}".format(tf))
    
    while True:
        print("진행하시겠습니까?",end="")
        goAhead =input(" [y/n]")
        if goAhead.lower() == "y":
            break
        elif goAhead.lower() == "n":
            return 0
        else:
            print("y 또는 n을 입력해주세요")
    
    while True:
        print("key파일의 기준 열을 입력해주세요: ",end="")
        keyFcol = input().strip()
        print("target파일의 기준 열을 입력해주세요: ",end="")
        keyTcol = input().strip()
        print("")
        print("{}등 {}개 파일의 기준 열은\n'{}' 으로 설정되었습니다\n".format(keyfiles[0].split("/")[-1],len(keyfiles),keyFcol))
        print("{}등 {}개 파일의 기준 열은\n'{}' 으로 설정되었습니다\n".format(files[0].split("/")[-1],len(files),keyTcol))
        confirm = False
        while True:
            print("진행하시겠습니까? ",end="")
            goAhead =input("[y/n]")
            if goAhead.lower() == "y":
                confirm = True
                break
            elif goAhead.lower() == "n":
                break
            else:
                print("y 또는 n을 입력해주세요")
        if confirm:
            break


    print("key파일의 '{}' 값들과  target파일의 '{}' 값들이 매칭되는 행을 추출합니다\n\n".format(keyFcol,keyTcol))
    if keyFcol != keyTcol:
        keyCol = keyFcol+"_"+keyTcol    
        print("새로운 column명 '{}'으로 저장됩니다\n\n".format(keyCol))
    else:
        keyCol = keyFcol

    print("vlookup을 시작합니다.\n")
    keydfArr = []
    for keyfile in keyfiles:
        print("read Key-file: {}".format(keyfile))
        try:
            keydf_row = getdf(keyfile,[getTable(keyCol=keyFcol)]).drop_duplicates()
            
        except:
            keydf_row = getdf(keyfile,[getTable_fs]).drop_duplicates()

        try:        
            keydf_i = cleandf(keydf_row,keyFcol)
        except:
            return 0
        keydfArr.append(keydf_i)
    keydf = pd.concat(keydfArr,0).drop_duplicates().reset_index(drop=True)
    keydf = keydf.rename(columns={keyFcol:keyCol})
    keydf = keydf.assign(extra=[True for _ in range(len(keydf))])

    print("중복된 값은 제거 됩니다\n")
    
    
    
    
    dfque,dflque = [],[]
    idx,idxl = 0,0
    for f in files:
        print("reading    {}".format(f))
        try:
            df_i = cleandf(getdf(f,[getTable(keyCol=keyTcol)]),keyTcol)
        except:
            return 0
        _df_i = df_i.rename(columns={keyTcol:keyCol})
        
        print("processing {}\n".format(f))
        
        
        df_i = _df_i.merge(keydf.drop("extra",1),on=[keyCol],how="inner")
        
        
        #joinned Rows
        if len(dfque) == 0:
            dfque.append(df_i)
        else:
            dfque[0] = pd.concat([dfque[0],df_i],0)
            dfque[0].index = range(len(dfque[0]))
            
        if len(dfque[0]) > MAX_ROW:
            t = datetime.now()
            tstr = t.strftime('%Y%m%d%H%M%S')

            dfque[0].iloc[:MAX_ROW,:].to_csv(curdir+"/{}_result_utf8_{}~{}.csv".format(tstr,idx+1,MAX_ROW+idx),index=None,encoding="utf-8")
            dfque[0].iloc[:MAX_ROW,:].to_csv(curdir+"/{}_result_cp949_{}~{}.csv".format(tstr,idx+1,MAX_ROW+idx),index=None,encoding="cp949")
            
            idx += MAX_ROW
            dfque[0] = dfque[0].iloc[MAX_ROW:,:]
            dfque[0].index = range(len(dfque[0]))
        else:
            pass
            
        """
        dfl = _df_i.merge(keydf,on=[keyCol],how="left")
        dfl = dfl[dfl.extra.isnull()].drop("extra",1)
        # dropped rows
        if len(dflque) == 0:
            dflque.append(dfl)
        else:
            dflque[0] = pd.concat([dflque[0],dfl],0)
            dflque[0].index = range(len(dflque[0]))
            
        if len(dflque[0]) > MAX_ROW:
            t = datetime.now()
            tstr = t.strftime('%Y%m%d%H%M%S')

            dflque[0].iloc[:MAX_ROW,:].to_csv(curdir+"/{}_dropped_utf8_{}~{}.csv".format(tstr,idxl+1,MAX_ROW+idxl),index=None,encoding="utf-8")
            dflque[0].iloc[:MAX_ROW,:].to_csv(curdir+"/{}_dropped_cp949_{}~{}.csv".format(tstr,idxl+1,MAX_ROW+idxl),index=None,encoding="cp949")
            
            idxl += MAX_ROW
            dflque[0] = dflque[0].iloc[MAX_ROW:,:]
            dflque[0].index = range(len(dflque[0]))
        else:
            pass

        """


    t = datetime.now()
    tstr = t.strftime('%Y%m%d%H%M%S')
    dfque[0].to_csv(curdir+"/{}_result_utf8_{}~{}.csv".format(tstr,idx+1,idx + len(dfque[0])),index=None,encoding="utf-8")
    dfque[0].to_csv(curdir+"/{}_result_cp949_{}~{}.csv".format(tstr,idx+1,idx + len(dfque[0])),index=None,encoding="cp949")
    #dflque[0].to_csv(curdir+"/{}_dropped_utf8_{}~{}.csv".format(tstr,idxl+1,idxl + len(dflque[0])),index=None,encoding="utf-8")
    #dflque[0].to_csv(curdir+"/{}_dropped_cp949_{}~{}.csv".format(tstr,idxl+1,idxl + len(dflque[0])),index=None,encoding="cp949")
    
    print("성공적으로 완료되었습니다. 아무키나 누르면 종료합니다.")
    input("by-NAMYUNWOO\nhttps://github.com/namyunwoo")
    return 0
if __name__ == "__main__":
    main()