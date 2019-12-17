"""
The raw data from Census and Statistics Department, HK, contain 3 parts:
1. Imports/domestic exports/re-exports by consignment country/territory by HS commodity item
2. Imports by origin country/territory by HS commodity item
3. Re-exports by origin country/territory by destination country/territory by HS commodity item
They can be found in folder "./C&SD_raw_data", grouped by year and month.

File names for above namely are:
1. HSCCIT.DAT
2. HSCOIT.DAT
3. HSCOCCIT.DAT
Define functions to get raw data from the 3 files, then merge in dataframe
"""
import pandas as pd
#import numpy as np
import io
import time
from .hstositc import get_hstositc_code

rawdata_folder="C&SD_raw_data"

def get_hsccit(year, month=12, path=rawdata_folder):
    try:
        file_path = f'{path}/{year}{month}/hsccit.dat'
        open(file_path)
    except:
        file_path = f'{path}/{year}{month}/hsccit.txt'
        open(file_path)
        print(f"Import from txt file: {file_path}")
    else:
        print(f"Import from dat file: {file_path}")

    col_names = ['f1','f2','f3','f6','f7','f10','f11','f14','f15']
    #col_widths = [1,8,3,18,18,18,18,18,18,18,18,18,18,18,18]

    df = pd.read_fwf(file_path,
                    colspecs=[(0,1),(1,9),(9,12),
                    (48,66),(66,84),
                    (120,138),(138,156),
                    (192,210),(210,228)],
                    #widths=col_widths,
                    names=col_names,
                    converters={1:str})
    # select transaction type 1 (HS-8digit) only
    #print(df)
    HS8only = df.f1.isin([1])
    df = df[HS8only]

    # select only useful columns but will slower, so not do
    #df = df[['f1','f2','f3','f6','f7','f10','f11','f14','f15']]
    df=df.rename(columns={"f6": "IM", "f7": "IM_Q", "f10": "DX", "f11": "DX_Q",\
                        "f14": "RX", "f15": "RX_Q"})
    #print(df)
    df['TX'] = df['DX'] + df['RX']
    df['TT'] = df['IM'] + df['TX']
    df['TX_Q'] = df['DX_Q'] + df['RX_Q']
    df['TT_Q'] = df['TX_Q'] + df['IM_Q']

    # add HS2, HS4 and HS6 columns
    df['HS-2'] = [str(x)[:2] for x in df.f2]
    df['HS-4'] = [str(x)[:4] for x in df.f2]
    df['HS-6'] = [str(x)[:6] for x in df.f2]

    # conversion of hs to SITC
    hstositc = get_hstositc_code()
    df['SITC-1'] = [hstositc.get(str(x), "NA")[:1] for x in df.f2]
    df['SITC-2'] = [hstositc.get(str(x), "NA")[:2] for x in df.f2]
    df['SITC-3'] = [hstositc.get(str(x), "NA")[:3] for x in df.f2]
    df['SITC-4'] = [hstositc.get(str(x), "NA")[:4] for x in df.f2]
    df['SITC-5'] = [hstositc.get(str(x), "NA") for x in df.f2]

    df['reporting_time'] = f'{year}{month}'

    return df

def get_hscoit(year, month=12, path=rawdata_folder):
    try:
        file_path = f'{path}/{year}{month}/hscoit.dat'
        open(file_path)
    except:
        file_path = f'{path}/{year}{month}/hscoit.txt'
        print(f"Import from txt file: {file_path}")
    else:
        print(f"Import from dat file: {file_path}")

    # select for transaction type 1 (HS-8digit) only
    col_names = ['f1','f2','f3','f6','f7']
    #col_widths = [1,8,3,18,18,18,18]

    df = pd.read_fwf(file_path,
                    colspecs=[(0,1),(1,9),(9,12),
                    (48,66),(66,84)],
                    #widths=col_widths,
                    names=col_names,
                    converters={1:str})

    HS8only = df.f1.isin([1])
    df = df[HS8only]

    df=df.rename(columns={"f6": "IMbyO", "f7": "IMbyO_Q"})

    # add HS2, HS4 and HS6 columns
    df['HS-2'] = [x[:2] for x in df.f2]
    df['HS-4'] = [x[:4] for x in df.f2]
    df['HS-6'] = [x[:6] for x in df.f2]

    # conversion of hs to SITC
    hstositc = get_hstositc_code()
    df['SITC-1'] = [hstositc.get(x, "NA")[:1] for x in df.f2]
    df['SITC-2'] = [hstositc.get(x, "NA")[:2] for x in df.f2]
    df['SITC-3'] = [hstositc.get(x, "NA")[:3] for x in df.f2]
    df['SITC-4'] = [hstositc.get(x, "NA")[:4] for x in df.f2]
    df['SITC-5'] = [hstositc.get(x, "NA") for x in df.f2]

    df['reporting_time'] = f'{year}{month}'
    return df

def get_hscoccit(year, month=12, path=rawdata_folder):
    try:
        file_path = f'{path}/{year}{month}/hscoccit.dat'
        open(file_path)
    except:
        file_path = f'{path}/{year}{month}/hscoccit.txt'
        print(f"Import from txt file: {file_path}")
    else:
        print(f"Import from dat file: {file_path}")
    
    col_names = ['f1','f2','f3','f4','f5','f6','f7','f8']
    col_widths = [1,8,3,3,18,18,18,18]

    df = pd.read_fwf(file_path, widths=col_widths, names=col_names,
                    converters={1:str})
    # select for transaction type 1 (HS-8digit) only
    HS8only = df.f1.isin([1])
    df = df[HS8only]

    df=df.rename(columns={"f7": "RXbyO", "f8": "RXbyO_Q"})

    # add HS2, HS4 and HS6 columns
    df['HS-2'] = [x[:2] for x in df.f2]
    df['HS-4'] = [x[:4] for x in df.f2]
    df['HS-6'] = [x[:6] for x in df.f2]

    # conversion of hs to SITC
    hstositc = get_hstositc_code()
    df['SITC-1'] = [hstositc.get(x, "NA")[:1] for x in df.f2]
    df['SITC-2'] = [hstositc.get(x, "NA")[:2] for x in df.f2]
    df['SITC-3'] = [hstositc.get(x, "NA")[:3] for x in df.f2]
    df['SITC-4'] = [hstositc.get(x, "NA")[:4] for x in df.f2]
    df['SITC-5'] = [hstositc.get(x, "NA") for x in df.f2]

    df['reporting_time'] = f'{year}{month}'
    return df

# In directory of tradestat_formal to run "python -m BSO.rawdata" in shell
# to test this code
if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    print("test")
    df = get_hsccit(2019, month=10)
    print(df)

    elapsed_time = round(time.time() - start_time, 2)
    print("time used: ", elapsed_time, " seconds")
