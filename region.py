import time
import pyprind
import os
from area import Area_Report1
from BSO.merge import mergedf
from BSO.geography import get_geography_code, get_geography_regcnty_code

class Region_Report1(Area_Report1):
    def __str__(self):
        doc = "This Report1 class is %s\n region code: %s\n periods from %s-%s\n contain country: %s\n" % \
        (self.name, self.area_code, self.periods[0], self.periods[-1], self.code)
        return doc

if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    print("R1 for Region")

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

    # get region codes and the including country codes
    # dict: region code and names
    region_dict = get_geography_code(sheet="region") # region code and names
    # dict: region code and country codes
    region_cty_dict = get_geography_regcnty_code(region_dict.keys(),sheet="regcnty")

    # create process bar
    barsize = 4*len(region_dict)
    print(f'Number of files: {barsize}')
    bar = pyprind.ProgPercent(barsize, monitor=False)

    currentdir = os.getcwd()

    # loop all area reports
    for regioncode,regionname in region_dict.items():
        region = Region_Report1(regioncode,regionname,region_cty_dict[regioncode],startyear,endytd,toprank,report_type="Region")

        region.general_trade_figure(df1,df3,periods)
        region.TX_fig, region.TX_share, region.TX_chg = region.trade_commodity(data=df1, tradetype='TX', numberofproduct=toprank, code_type=codetype)
        region.DX_fig, region.DX_share, region.DX_chg = region.trade_commodity(data=df1, tradetype='DX', numberofproduct=toprank, code_type=codetype)
        region.RX_fig, region.RX_share, region.RX_chg = region.trade_commodity(data=df1, tradetype='RX', numberofproduct=toprank, code_type=codetype)
        region.IM_fig, region.IM_share, region.IM_chg = region.trade_commodity(data=df1, tradetype='IM', numberofproduct=toprank, code_type=codetype)

        region.denote_percent()
        # export reports to excel
        for i, currency in enumerate(['HKD','USD']):
            for i, money in enumerate(['TH','MN']):
                region.report_to_excel(currency, money, original_path=currentdir)
                bar.update() # print bar process
        os.chdir(currentdir)
    #calculate time spent
    elapsed_time = round(time.time() - start_time, 2)
    print("time used: ", elapsed_time, " seconds")
    bye=input("Bye. Press any to quit")
