#from .rawdata_array_before_df_method_past20191214 import *
from .rawdata_pd_read_fwf_method import *

def exclgold(df):
    # filter out the gold commodity
    gold_commodity = ["71081100", "71081210", "71081290", "71081300",
                      "71082010", "71082090", "71090000", "71123000",
                      "71129100", "71189000"]
    gold_T = df.f2.isin(gold_commodity)
    df_exclgold = df[~gold_T]
    return df_exclgold

def mergedf(startyear=2016, endperiod=201906, type="hsccit"):

    get_rawdata = {"hsccit":get_hsccit,
                   "hscoit":get_hscoit,
                   "hscoccit":get_hscoccit}

    if (len(str(endperiod)) == 6) & (str(endperiod)[-2:] != '12'):
        # acquire yearly data
        yearly_df = [get_rawdata[type](year = yr) for yr in range(startyear, int(str(endperiod)[:4]))]
        yearly_df = pd.concat(yearly_df,sort=True)

        # acquire year-to-date data
        endmonth = str(endperiod)[-2:]
        df1 = get_rawdata[type](int(str(endperiod)[:4]), month=endmonth)
        df2 = get_rawdata[type](int(str(endperiod)[:4])-1, month=endmonth)

        yeartod_df = pd.concat([df1,df2],sort=True)

        result = pd.concat([yearly_df,yeartod_df],sort=True)

    elif (len(str(endperiod)) == 4):
        result = [get_rawdata[type](year = yr) for yr in range(startyear, endperiod+1)]
        result = pd.concat(result,sort=True)
    # final df will exclude gold commodity
    return exclgold(result)
