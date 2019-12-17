import pandas as pd
import time
import pyprind
import os
from BSO.merge import mergedf
from BSO.R1_figures import country_R1_fig, major_commodity_fig
import export.export_file as ex

#implement tradestat report(also known as Report1) class
class Report1(object):
    """Class for Trade statistics Report, a prototype or parent class
    for report of HK's trade with world"""
    noofindustry=0
    def __init__(self, country_code, country_name, startperiod, endperiod, toprank,report_type="World", currency=None, money=None):
        self.report_type = report_type
        self.code = country_code
        self.name = country_name
        self.start = startperiod
        self.end = endperiod
        self.toprank = toprank
        self.ranking=None

    def general_trade_figure(self,df1,df3,periods):
        self.periods = periods
        self.trade_fig, self.trade_chg = country_R1_fig(df1,df3,self.code,self.periods)
        # drop percentage change for trade balance
        self.trade_chg = self.trade_chg.drop(['TRADE BALANCE'])

    def trade_commodity(self,data,tradetype,numberofproduct=10, code_type="SITC-3"):
        fig, share, chg = major_commodity_fig(data, self.code, self.periods, tradetype, \
                            topnumber=numberofproduct, codetype=code_type)
        # drop row for 'All'
        for i in [fig, share, chg]:
            i.drop(index='All', inplace=True)
        return fig, share, chg

    def denote_percent(self):
        share = [self.TX_share,self.DX_share,self.RX_share,self.IM_share]
        chg = [self.TX_chg,self.DX_chg,self.RX_chg,self.IM_chg,self.trade_chg]
        [ex.denotesymbol(p, 'share') for i, p in enumerate(share)]
        [ex.denotesymbol(p, 'chg') for i, p in enumerate(chg)]

    def report_to_excel(self,currency,money,original_path=None):
        # change to current dir every time when run this function
        os.chdir(original_path)
        excel_name = f"{self.name.replace('/',',')}_{self.periods[-1]}_{currency}_{money}.xlsx"
        ex.create_and_change_path(self.end,folder_path=self.report_type, currency=currency,money=money)
        writer = pd.ExcelWriter(excel_name, engine='xlsxwriter')

        # 1) write general trade figures and percentage change to excel
        # if there are annually and monthly year to date data,
        # delete the last monthly year to date
        if len(self.periods) == 5 and self.periods[-1][-2:] != '12':
            self.trade_fig = self.trade_fig[self.periods[:2]+self.periods[3:5]]
        ex.part1_toexcel_generaltrade(self.trade_fig, self.trade_chg, writer, currency, money)

        # 2a) export specific trade figures in different rows and columns
        start=17
        for i in [self.TX_fig,self.DX_fig,self.RX_fig,self.IM_fig]:
            # if there are annually and monthly year to date data,
            # delete the last monthly year to date, e.g. if recent is 201906, then del 201806
            if len(self.periods) == 5 and self.periods[-1][-2:] != '12':
                try:
                    i.drop(self.periods[-3], axis=1,inplace=True)
                except: pass

            # write SITC codes and product names
            i.iloc[:,0].to_excel(writer,sheet_name=f"{currency}_{money}",index=True,startrow=start, startcol=0,header=False)
            # write figures for different trade types
            ex.part2a_toexcel_specialtrade_fig(i.iloc[:,-4:], writer, currency, money, startrow=start)
            start+=(self.toprank+3)

        # 2b) export trade share in different rows and columns
        start=17
        for i, d in enumerate([self.TX_share,self.DX_share,self.RX_share,self.IM_share]):
            ex.part2b_toexcel_specialtrade_share(d, writer, currency, money, startrow=start)
            start+=(self.toprank+3)

        # 2c) export trade figure percentage change
        # write the most recent period change
        start=17
        for i, d in enumerate([self.TX_chg,self.DX_chg,self.RX_chg,self.IM_chg]):
            ex.part2c_toexcel_specialtrade_chg(d, writer, currency, money, startrow=start)
            start+=(self.toprank+3)

        # 3) export all trades' ranking for country
        if self.ranking is not None:
           ex.part3_toexcel_ranking_cty(self.ranking, writer, currency, money, self.periods)

        # adjust the format of cell in excel file using xlsxwriter
        ex.adjust_excelformat_xlsxwriter(writer, currency, money, self.periods, self.name, self.toprank)
        writer.save()
        # adjust the format of cell in excel file using openpyxl
        ex.adjust_excelformat_openpyxl(excel_name, currency, money)
        # using win32com.client to control excel to autofit
        ex.autofit(excel_name, currency, money)

    def __repr__(self):
        doc = "This Report1 class is %s, periods from %s-%s" % (self.name, self.periods[0], self.periods[-1])
        return doc

if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    print("R1 for World")

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

    # create process bar
    barsize = 4
    print(f'Number of files: {barsize}')
    bar = pyprind.ProgPercent(barsize, monitor=False)

    # select country
    cty_code = "WORLD"
    cty_name = "WORLD"
    print(f"Country: {cty_name} code: {cty_code}")

    currentdir = os.getcwd()
    # make instance of class Report1
    world = Report1(cty_code, cty_name, startyear, endytd, toprank)
    world.general_trade_figure(df1,df3,periods)
    world.TX_fig, world.TX_share, world.TX_chg = world.trade_commodity(data=df1, tradetype='TX', numberofproduct=toprank, code_type=codetype)
    world.DX_fig, world.DX_share, world.DX_chg = world.trade_commodity(data=df1, tradetype='DX', numberofproduct=toprank, code_type=codetype)
    world.RX_fig, world.RX_share, world.RX_chg = world.trade_commodity(data=df1, tradetype='RX', numberofproduct=toprank, code_type=codetype)
    world.IM_fig, world.IM_share, world.IM_chg = world.trade_commodity(data=df1, tradetype='IM', numberofproduct=toprank, code_type=codetype)
    # denote symbol to percentage only
    world.denote_percent()

    # export reports to excel
    for i, currency in enumerate(['HKD','USD']):
        for i, money in enumerate(['TH','MN']):
            world.report_to_excel(currency, money, original_path=currentdir)
            bar.update() # print bar process
    #calculate time spent
    elapsed_time = round(time.time() - start_time, 2)
    print("time used: ", elapsed_time, " seconds")
    bye=input("Bye. Press any to quit")
