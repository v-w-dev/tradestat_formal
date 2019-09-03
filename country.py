import time
import pyprind
import os
from world import Report1
from BSO.rawdata import mergedf
from BSO.geography import get_geography_code
from BSO.R1_figures import six_trades_ranking_bycty_multi_yrs, find_all_trades_ranking

class Country_Report1(Report1):
    pass

if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    print("R1 for individual Country")

    startyear, endytd = 2016, 201907

    startyear = int(input(f"Enter Start Year: eg.{startyear}: "))
    endytd = int(input(f"Enter End YeartoMonth: eg.{endytd}: "))

    #select no. of product
    toprank = 10
    toprank = int(input(f"Enter no.of products to display: eg.{toprank}: "))
    #select code type
    codetype="SITC-3"

    # acquire hsccit data from startyear to endyear and combine them into dataframe
    df1 = mergedf(startyear, endytd, "hsccit")
    # acquire hscoccit data from startyear to endyear and combine them into dataframe
    df3 = mergedf(startyear, endytd, "hscoccit")

    # sort the periods for functions use later
    periods = sorted(set(df1.reporting_time))

    # get country codes
    # dict: country code and names
    cty_dict = get_geography_code(sheet="country") # country code and names

    # create process bar
    barsize = 4*len(cty_dict)
    print(f'Number of files: {barsize}')
    bar = pyprind.ProgPercent(barsize, monitor=False)

    # set numbers of years to rank
    rank_periods = 2
    # make ranking of all countries for all trades
    All_rank_dict={}
    type_list = ['TX','DX','RX','IM','RX_O','TT']
    print("Calculating ranking...please wait for a while.")
    rankdf_list = six_trades_ranking_bycty_multi_yrs(df1,df3,countrydict=cty_dict,periods=periods,num=rank_periods)
    print("Ranking completed.")

    currentdir = os.getcwd()

    # loop all area reports
    for cty_code, cty_name in cty_dict.items():
        # if cty_code!=199:continue
        # find out the ranks of the country for all trades
        cty = Country_Report1(cty_code,cty_name,startyear,endytd,toprank,report_type="Country")

        cty.general_trade_figure(df1,df3,periods)
        cty.TX_fig, cty.TX_share, cty.TX_chg = cty.trade_commodity(data=df1, tradetype='TX', numberofproduct=toprank, code_type=codetype)
        cty.DX_fig, cty.DX_share, cty.DX_chg = cty.trade_commodity(data=df1, tradetype='DX', numberofproduct=toprank, code_type=codetype)
        cty.RX_fig, cty.RX_share, cty.RX_chg = cty.trade_commodity(data=df1, tradetype='RX', numberofproduct=toprank, code_type=codetype)
        cty.IM_fig, cty.IM_share, cty.IM_chg = cty.trade_commodity(data=df1, tradetype='IM', numberofproduct=toprank, code_type=codetype)

        # Only a few countries do not contain any data, then no need for ranking, report as None
        try:
            cty.ranking = find_all_trades_ranking(type_list,rankdf_list,cty_code,periods,rank_periods)
        except: cty.ranking = None
        cty.denote_percent()
        # export reports to excel
        for i, currency in enumerate(['HKD','USD']):
            for i, money in enumerate(['TH','MN']):
                cty.report_to_excel(currency, money, original_path=currentdir)
                bar.update() # print bar process
        os.chdir(currentdir)
    #calculate time spent
    elapsed_time = round(time.time() - start_time, 2)
    print("time used: ", elapsed_time, " seconds")
    bye=input("Bye. Press any to quit")
