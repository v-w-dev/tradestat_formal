import pandas as pd
import numpy as np
from .hstositc import get_sitc_name

def country_R1_fig_oneperiod(hsccit, hscoccit, cty_code, period):
    # convert the type of period from int to string
    period = str(period)
    # for Area or Region
    if type(cty_code) == list:
        df1 = hsccit[(hsccit.f1 == 1) & (hsccit.f3.isin(cty_code)) &\
            (hsccit.reporting_time == period)]
        df3 = hscoccit[(hscoccit.f1 == 1) & (hscoccit.f3.isin(cty_code)) & \
            (hscoccit.reporting_time == period)]
    # for country
    elif type(cty_code) == int and cty_code > 0:
        df1 = hsccit[(hsccit.f1 == 1) & (hsccit.f3 == cty_code) &\
            (hsccit.reporting_time == period)]
        df3 = hscoccit[(hscoccit.f1 == 1) & (hscoccit.f3 == cty_code) & \
                (hscoccit.reporting_time == period)]
    # for world
    elif cty_code == 'WORLD':
        df1 = hsccit[(hsccit.f1 == 1) & (hsccit.reporting_time == period)]
        df3 = hscoccit[(hscoccit.f1 == 1) & (hscoccit.reporting_time == period)]

    DX = sum(df1.DX)
    RX = sum(df1.RX)
    TX = sum(df1.DX) + sum(df1.RX)
    IM = sum(df1.IM)
    TT = sum(df1.IM) + sum(df1.DX) + sum(df1.RX)
    RXbyO = sum(df3.RXbyO)
    TB = TX - IM

    data={period: [TX, DX, RX, IM, RXbyO, TT, TB]}
    return data

def country_R1_fig(df1, df3, cty_code, periods):
    R1_trade={}
    for i, p in enumerate(periods):
        R1_fig = country_R1_fig_oneperiod(df1, df3, cty_code, period=p)
        R1_trade.update(R1_fig)

    table = pd.DataFrame(R1_trade)

    table_idx = ["TOTAL EXPORTS", "DOMESTIC EXPORTS", "RE-EXPORTS", "IMPORTS",
    "(OF WHICH RE-EXPORTED)", "TOTAL TRADE", "TRADE BALANCE"]
    table.set_index([table_idx], inplace=True)

    table = pd.DataFrame(table)
    # make percentage change of the table
    pct_chg = pecent_change(table, periods)
    return table, pct_chg

def major_commodity_fig(df, cty_code, periods, tradetype='TX', topnumber=30, codetype='SITC-3'):
    lastperiod = periods[-1]
    # for region
    if type(cty_code) == list:
        df = df[(df.f1 == 1) & (df.f3.isin(cty_code))]
    # for country
    elif type(cty_code) == int and cty_code > 0:
        df = df[(df.f1 == 1) & (df.f3 == cty_code)]
    # for world
    elif cty_code == 'WORLD':
        df = df[(df.f1 == 1)]

    # see the periods of df
    df_have_periods = set(df.reporting_time)
    nothave = []

    # find out the periods not contained in df
    for p in periods:
        if p not in df_have_periods:
            nothave.append(p)
    if nothave:
        for p in nothave:
            df = df.append({'reporting_time' : str(p), tradetype:0, codetype:'N.A.'} , ignore_index=True)

    # get the top most products for the last period
    if len(periods)==4:
        sorting=[lastperiod,periods[-2],periods[-3],periods[-4], codetype]
    if len(periods)==5:
        sorting=[lastperiod,periods[-2],periods[-4],periods[-5], codetype]

    # sorting by multiple periods, order is by the most recent to most far away
    df0 = pd.pivot_table(df, values=tradetype, index=[codetype],columns=['reporting_time'],\
              aggfunc=np.sum, fill_value=0, margins=True).\
              sort_values(by=sorting,\
              ascending=False)
    # drop col'All'
    df1 = df0.drop(['All'], axis=1)
    # copy the row 'All', measure it is on the top row
    row_all = pd.DataFrame(df1.loc['All']).T
    # remove the rows for 'All' and 'N.A.'
    df1 = df1.drop(['All'], axis=0)
    if nothave: df1 = df1.drop(['N.A.'], axis=0)

    df = row_all.append(df1)

    df_top = df[:topnumber+1].copy()
    OTHERS = df[topnumber+1:].copy().sum()
    # combine product of top no. and OTHERS
    df_top.loc["OTHERS"]=OTHERS
    df = df_top

    try:
        pctshare = df/df.loc["All"].values*100
    except:
        # if exception occurs, suppose no data for the trade types
        # so there is no 'All' in row 0, just remain df unchanged
        input("pctshare has error, line 105 in BSO_R1 figures.py, press Y to continue")

    pctshare.columns = [c+"_% Share of overall" for c in pctshare.columns]
    # calculate percentage change
    pct_chg = pecent_change(df, periods)
    # add product name to dataframe result
    # acquire product names
    if codetype =='SITC-3': code_type ='sitc3'
    elif codetype =='SITC-5': code_type ='sitc5'
    else: assert False, "can only use 'SITC-3' or 'SITC-5' to classify products"
    product_name_dict = get_sitc_name(sheet_name=code_type)
    topnumber_names = [product_name_dict.get(x, str(x)) for x in df.index]
    df.insert(loc=0, column='product name', value=topnumber_names)
    # return trade figures, products' share of total by periods
    return df, pctshare, pct_chg

def pecent_change(data, periods):
    # for data combined with yearly and monthly(year to date)
    if int(periods[-1][-2:])!=12:
        year=data.iloc[:,[0,1,3]].pct_change(axis='columns')
        ytd=data.iloc[:,[2,4]].pct_change(axis='columns')
        tablepcc=pd.concat([year,ytd],axis=1)
        tablepcc=tablepcc.iloc[:,[1,2,4]]
    # for data yearly only
    elif int(periods[-1][-2:])==12:
        tablepcc=data.pct_change(axis='columns')
        tablepcc=tablepcc.iloc[:,[1,2,3]]

    # change pecentage columns name
    tablepcc.columns = [c+"_% CHG" for c in tablepcc.columns]
    # make percentage times 100
    tablepcc*=100

    # table_result = tablepcc.dropna(axis='columns', how='all')
    return tablepcc

def trades_ranking_bycty(df, countrydict={}, field='TX', periods=[]):
    country_code_list = sorted(set(df["f3"]))

    cty_trade_sum = {}
    for x, cty_code in enumerate(country_code_list):
        df = df[(df.f1 == 1) & (df.reporting_time == periods[-1])]
        field_x = sum(df[(df.f3 == cty_code)][field])
        cty_trade_sum.update({cty_code:field_x})

    field_df = pd.Series(cty_trade_sum).to_frame()
    field_df.columns = [periods[-1]]

    trade_ranking = field_df.sort_values(by=[periods[-1]], ascending=False)

    # add ranking
    trade_ranking[periods[-1] + "rank"] = trade_ranking[periods[-1]].rank(ascending=False).astype('int')
    trade_ranking["cty_code"] = trade_ranking.index

    # acquire cty name to merge with the trade_ranking dataframe
    ranking_cty_names = [countrydict[cty_code] for cty_code in trade_ranking.index]

    s = pd.Series(data = ranking_cty_names, index=trade_ranking.index, name="Country")
    ctyname = pd.DataFrame(s)
    table = pd.merge(ctyname, trade_ranking, left_index=True, right_index=True)

    return table

def trades_ranking_bycty_multi_yrs(df, countrydict={}, field='TX', periods=[],num=300):
    df_list=[]

    for i, p in enumerate(periods):
        df_trade = trades_ranking_bycty(df, countrydict, field, periods=[p])
        df_list.append(df_trade)

    df = df_list[0]
    for df_ in df_list[1:]:
        df = df.merge(df_, how='outer')

    table = df.sort_values(by=[periods[-1]], ascending=False)
    table.set_index(['cty_code'], inplace=True)
    return table[:num]

def six_trades_ranking_bycty_multi_yrs(df1,df3,countrydict={},periods=[],num=2):
    TX_ranking = trades_ranking_bycty_multi_yrs(df1, countrydict, field='TX', periods = periods[-num:])
    DX_ranking = trades_ranking_bycty_multi_yrs(df1, countrydict, field='DX', periods = periods[-num:])
    RX_ranking = trades_ranking_bycty_multi_yrs(df1, countrydict, field='RX', periods = periods[-num:])
    IM_ranking = trades_ranking_bycty_multi_yrs(df1, countrydict, field='IM', periods = periods[-num:])
    RXbyO_ranking = trades_ranking_bycty_multi_yrs(df3, countrydict, field='RXbyO', periods = periods[-num:])
    TT_ranking = trades_ranking_bycty_multi_yrs(df1, countrydict, field='TT', periods = periods[-num:])

    return [TX_ranking,DX_ranking,RX_ranking,IM_ranking,RXbyO_ranking,TT_ranking]

def find_ranking(df, cty_code, periods=[]):
    rank_dict={}
    for i, p in enumerate(periods):
        pos = df.iloc[df.index == cty_code][p + "rank"]
        pos = pos.values[0]
        rank_dict[p] = pos
    return rank_dict

def find_all_trades_ranking(type_list, rankdf_list,cty_code,periods,rank_periods):
    All_rank_dict={}
    for type, dataf in zip(type_list, rankdf_list):
        rank_dict = find_ranking(dataf, cty_code, periods[-rank_periods:])

        All_rank_dict[type] = rank_dict

    trades_rank_df = pd.DataFrame(All_rank_dict).T
    return trades_rank_df
