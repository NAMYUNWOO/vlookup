#-*- coding: utf-8 -*-
import os,re,sys
import pandas as pd 
from datetime import datetime
from urllib import parse
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
        res = re.findall(head+"=([^\&]+)\&",x)
        return str(res[0]).strip()
    except:
        return ""

def cleandf(df,fkeyCol):
    rowCnt = len(df)
    df.columns = list(map(lambda x : str(x).strip(),df.columns))
    if fkeyCol not in df.columns:
        print("'{}' 열이 존재하지 않습니다".format(fkeyCol))
        print("프로그램을 종료합니다 아무키나 누르세요",end="")
        input()
        raise SystemExit
    df[fkeyCol+"_origin"] = df[fkeyCol]
    df[fkeyCol] = df[fkeyCol].map(lambda x : parse.unquote(x))
    df = df[~ (df[fkeyCol].isnull())]
    df[fkeyCol] = df[fkeyCol].map(lambda x : str(x).strip().lower())
    df = df[df[fkeyCol] != ""]
    print("total {} urls in file".format(len(df)))
    
    return df,rowCnt


def getTable(keyCol=None):
    def __getTable(f,encoding):
        try:
            return pd.read_csv(f,encoding=encoding,na_filter = False,dtype={keyCol:str})
        except:
            pass
        
        try:
            return pd.read_csv(f,encoding=encoding,na_filter = False)
        except:
            pass

        try:
            return pd.read_csv(f,encoding=encoding,na_filter = False,dtype={keyCol:str},engine="python")
        except:
            pass

        try:
            return pd.read_csv(f,encoding=encoding,na_filter = False,engine="python")
        except:
            pass
        
        raise Exception

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
    print("./tag/ 폴더에 csv 파일을 넣어주세요")
    global MAX_ROW
    curdir = "/".join(os.getcwd().split("\\"))
    basedir =  curdir + "/tag/"
    
    
    files = list(map(lambda x : basedir + x, os.listdir(basedir)))

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
        print("tag를 추출할 열을 입력해주세요: ",end="")
        tagcol = input().strip()
        
        print("")
        print("{}등 {}개 파일에서 '{}'열의 tag를 추출합니다.\n".format(files[0].split("/")[-1],len(files),tagcol))
        print("tag가 존재하는 행만 남습니다.")
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



    print("tag 추출을 시작합니다.\n")
    
    dfque,dflque = [],[]
    idx,idxl = 0,0
    totalCnt, droppedCnt = 0,0
    for f in files:
        print("reading    {}".format(f))
        try:
            df_i,rowCnt = cleandf(getdf(f,[getTable(keyCol=tagcol)]),tagcol)
        except:
            return 0
        totalCnt += rowCnt
        
        print("processing {}\n".format(f))
        
        df_i["addtag"] = df_i[tagcol].map(lambda x : findtag(x,"addtag") )
        df_i["ctag"] = df_i[tagcol].map(lambda x : findtag(x,"ctag") )
        df_i["lptag"]= df_i[tagcol].map(lambda x : findtag(x,"lptag") )
        
        df_i = df_i[(df_i.addtag != "") | (df_i.ctag != "") | (df_i.lptag != "") ]
        
        droppedCnt = droppedCnt + (rowCnt - len(df_i))

        #joinned Rows
        if len(dfque) == 0:
            dfque.append(df_i)
        else:
            dfque[0] = pd.concat([dfque[0],df_i],0)
            dfque[0].index = range(len(dfque[0]))
            
        if len(dfque[0]) > MAX_ROW:
            t = datetime.now()
            tstr = t.strftime('%Y%m%d%H%M%S')
            try:
                dfque[0].iloc[:MAX_ROW,:].to_csv(curdir+"/{}_tag_result_utf8_{}~{}.csv".format(tstr,idx+1,MAX_ROW+idx),index=None,encoding="utf-8")
            except:
                pd.DataFrame(["brokenFile"],columns=["val"]).to_csv(curdir+"/{}_tag_result_utf8_{}~{}.csv".format(tstr,idx+1,MAX_ROW+idx),index=None,encoding="utf-8")

            try:
                dfque[0].iloc[:MAX_ROW,:].to_csv(curdir+"/{}_tag_result_cp949_{}~{}.csv".format(tstr,idx+1,MAX_ROW+idx),index=None,encoding="cp949")
            except:
                pd.DataFrame(["brokenFile"],columns=["val"]).to_csv(curdir+"/{}_tag_result_cp949_{}~{}.csv".format(tstr,idx+1,MAX_ROW+idx),index=None,encoding="cp949")
            
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
    try:
        dfque[0].to_csv(curdir+"/{}_tag_result_utf8_{}~{}.csv".format(tstr,idx+1,idx + len(dfque[0])),index=None,encoding="utf-8")
    except:
        pd.DataFrame(["brokenFile"],columns=["val"]).to_csv(curdir+"/{}_tag_result_utf8_{}~{}.csv".format(tstr,idx+1,idx + len(dfque[0])),index=None,encoding="utf-8")
    try:
        dfque[0].to_csv(curdir+"/{}_tag_result_cp949_{}~{}.csv".format(tstr,idx+1,idx + len(dfque[0])),index=None,encoding="cp949")
    except:
        pd.DataFrame(["brokenFile"],columns=["val"]).to_csv(curdir+"/{}_tag_result_cp949_{}~{}.csv".format(tstr,idx+1,idx + len(dfque[0])),index=None,encoding="cp949")
    #dflque[0].to_csv(curdir+"/{}_dropped_utf8_{}~{}.csv".format(tstr,idxl+1,idxl + len(dflque[0])),index=None,encoding="utf-8")
    #dflque[0].to_csv(curdir+"/{}_dropped_cp949_{}~{}.csv".format(tstr,idxl+1,idxl + len(dflque[0])),index=None,encoding="cp949")
    with open(curdir+"/{}_tag_summary.txt".format(tstr), "w") as f:
        f.write("original {} rows\ndropped {} rows\nsaved {} rows".format(totalCnt,droppedCnt,totalCnt-droppedCnt))
    
    print("성공적으로 완료되었습니다. 아무키나 누르면 종료합니다.")
    input("by-NAMYUNWOO\nhttps://github.com/namyunwoo")
    return 0
if __name__ == "__main__":
    main()