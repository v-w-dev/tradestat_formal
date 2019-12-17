import time
import pyprind
import os
from world import Report1
from BSO.merge import mergedf
from BSO.geography import get_geography_code, get_geography_regcnty_code

class Area_Report1(Report1):
    def __init__(self,area_code,area_name, country_code, startperiod, endperiod, toprank,report_type="Area", currency=None, money=None):
        self.report_type = report_type
        self.area_code = area_code
        self.name = area_name
        self.code = country_code
        self.start = startperiod
        self.end = endperiod
        self.toprank = toprank
        self.ranking = None

    def __str__(self):
        doc = "This Report1 class is %s\n area code: %s\n periods from: %s-%s\n contain country: %s\n" % \
        (self.name, self.area_code, self.periods[0], self.periods[-1], self.code)
        return doc

if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    print("R1 for Area")

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

    # get Area codes and the including country codes
    # dict: area code and names
    area_dict = get_geography_code(sheet="area") # area code and names
    # dict: area code and country codes
    area_cty_dict = get_geography_regcnty_code(area_dict.keys(),sheet="areacnty")

    # create process bar
    barsize = 4*len(area_dict)
    print(f'Number of files: {barsize}')
    bar = pyprind.ProgPercent(barsize, monitor=False)

    currentdir = os.getcwd()

    # loop all area reports
    for areacode,areaname in area_dict.items():
        area = Area_Report1(areacode,areaname,area_cty_dict[areacode],startyear,endytd,toprank)

        area.general_trade_figure(df1,df3,periods)
        area.TX_fig, area.TX_share, area.TX_chg = area.trade_commodity(data=df1, tradetype='TX', numberofproduct=toprank, code_type=codetype)
        area.DX_fig, area.DX_share, area.DX_chg = area.trade_commodity(data=df1, tradetype='DX', numberofproduct=toprank, code_type=codetype)
        area.RX_fig, area.RX_share, area.RX_chg = area.trade_commodity(data=df1, tradetype='RX', numberofproduct=toprank, code_type=codetype)
        area.IM_fig, area.IM_share, area.IM_chg = area.trade_commodity(data=df1, tradetype='IM', numberofproduct=toprank, code_type=codetype)

        area.denote_percent()
        # export reports to excel
        for i, currency in enumerate(['HKD','USD']):
            for i, money in enumerate(['TH','MN']):
                area.report_to_excel(currency, money, original_path=currentdir)
                bar.update() # print bar process
        os.chdir(currentdir)
    #calculate time spent
    elapsed_time = round(time.time() - start_time, 2)
    print("time used: ", elapsed_time, " seconds")
    bye=input("Bye. Press any to quit")
