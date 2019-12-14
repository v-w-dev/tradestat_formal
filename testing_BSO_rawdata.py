"""
testing BSO.rawdata2.py
"""
import timeit
import pandas as pd
import time
from BSO.rawdata import get_hsccit as get_hsccit1
from BSO.rawdata2 import get_hsccit as get_hsccit2

if __name__ == '__main__':
    #calculate time spent
    def hsccit_time1():
        start_time = time.time()
        print("test")
        df = get_hsccit1(year=2019, month='10')
        print(df)

        elapsed_time = round(time.time() - start_time, 2)
        print("time used: ", elapsed_time, " seconds")

    def hsccit_time2():
        start_time = time.time()
        print("test")
        df = get_hsccit2(year=2019, month=10)
        print(df)

        elapsed_time = round(time.time() - start_time, 2)
        print("time used: ", elapsed_time, " seconds")
        return df

    #print("time: ", timeit.timeit(hsccit_time1, number=1)/1)
    df=hsccit_time2()
    print("hi")
    #print(df.iloc[3])
    #print(df.iloc[:,1])
