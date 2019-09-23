import pprint
import pandas as pd
import numpy as np
import time
import xlsxwriter
import calendar
import sys, os
from mergedf import exclgold, hsccit_mergedf, hscoit_mergedf, hscoccit_mergedf
from R1_figures import country_R1_fig, country_R1_figtable
from R1_figures import major_commodity_latest, major_commodity_table
from R1_figures import trades_ranking_bycty, trades_ranking_bycty_multi_yrs
from R1_figures import six_trades_ranking_bycty_multi_yrs, find_ranking
from BSO_geography import get_geography_code, get_geography_regcnty_code
from BSO_industry2 import get_industry_code

region_dict = get_geography_code(sheet="region")
area_dict = get_geography_code(sheet="area")

region_cty_dict = get_geography_regcnty_code(region_dict.keys(),sheet="regcnty")
area_cty_dict = get_geography_regcnty_code(area_dict.keys(),sheet="areacnty")

EU=('EU', 100)
Asean=('Asean', 300)
Asia=('Asia', 600)
Europe=("Europe", 900)

EU_cty_code = region_cty_dict[EU[1]]
Asean_cty_code = region_cty_dict[Asean[1]]
Asia_cty_code = area_cty_dict[Asia[1]]
Europe_cty_code = area_cty_dict[Europe[1]]

EU_cty_name=[get_geography_code(sheet="country")[i] for i in EU_cty_code]
Asean_cty_name=[get_geography_code(sheet="country")[i] for i in Asean_cty_code]
Asia_cty_name=[get_geography_code(sheet="country")[i] for i in Asia_cty_code]
Europe_cty_name=[get_geography_code(sheet="country")[i] for i in Europe_cty_code]

#print(EU_cty_name)
#print(Asean_cty_name)
"""
dollar = {'HKD':1, 'USD':7.8}
unit = {'TH':1000, 'MN':1000000}
currency = 'HK'
money = 'MN'
"""
#implement class
class Industry(object):
    """Class for Industry to export excel data"""
    noofindustry=0
    def __init__(self, group_no, name, periods, data, currency, money):
        Industry.noofindustry+=1
        self.group_no=group_no
        self.name = name
        self.periods = periods
        self.data = data
        self.currency = currency
        self.money = money

    def money_conversion(self, tabledata):
        return tabledata/dollar[self.currency]/unit[self.money]

    def run_df_table(self):
        self.df1, self.df2, self.df3=[i for i in self.data]

    def run_df_separate(self):
        self.df1_allperiods,self.df2_allperiods,self.df3_allperiods={},{},{}

        for p in self.periods:
            self.df1_allperiods[p] = self.df1[self.df1.reporting_time.isin([p])]
            self.df2_allperiods[p] = self.df2[self.df2.reporting_time.isin([p])]
            self.df3_allperiods[p] = self.df3[self.df3.reporting_time.isin([p])]

    def analysis_table1(self,select_period):
        DX = self.df1_allperiods[select_period].DX.sum()
        RX = self.df1_allperiods[select_period].RX.sum()
        RXbyCNasO = self.df3_allperiods[select_period][self.df3_allperiods[select_period].f3_origin==631].RX_O.sum()
        TX = self.df1_allperiods[select_period].TX.sum()
        IM = self.df1_allperiods[select_period].IM.sum()

        #print("testing here")
        #money_converion
        """
        attr = []
        for v in (DX, RX, RXbyCNasO, TX):
            attr.append(self.money_conversion(v))
        DX, RX, RXbyCNasO, TX = attr"""
        return DX, RX, RXbyCNasO, TX, IM

    def df_table1(self):
        self.table1_dict={}
        for p in self.periods:
            self.table1_dict[p]=self.analysis_table1(p)
            #print(self.table1_dict[p])

        self.table1=pd.DataFrame(self.table1_dict)
        table1_idx = ["Domestic Exports", "Re-exports", "   of Chinese mainland Origin", "Total Exports", "Imports"]
        self.table1.set_index([table1_idx], inplace=True)
        self.table1_result = self.mix_conversion_with_pct(self.table1)
        #print(self.table1_result)

    def mix_conversion_with_pct(self, tablefig):
        if int(self.periods[-1][-2:])!=12:
            year=tablefig.iloc[:,[0,1,3]].pct_change(axis='columns')
            ytd=tablefig.iloc[:,[2,4]].pct_change(axis='columns')
            tablepcc=pd.concat([year,ytd],axis=1)

        elif int(self.periods[-1][-2:])==12:
            tablepcc=tablefig.pct_change(axis='columns')
        #change pecentage columns name
        #print("testing")
        tablepcc.columns = [c+"_% CHG" for c in tablepcc.columns]

        # 1) make percentage times 100
        tablepcc*=100
        # 2) calculate % share of overall TX

        # 3) money_converion
        tablefig = self.money_conversion(tablefig)

        table_result = pd.concat([tablefig, tablepcc], axis=1).dropna(axis='columns', how='all')
        return table_result.sort_index(axis=1)

    def analysis_DX_cty(self):
        #for p in self.periods:
        """
        for p in self.periods:
            print(p)
            DX = self.df1_allperiods[p].groupby(['cty_name_destination'])['DX'].sum()
            print(type(DX))
            DX_sorted = DX.sort_values(ascending=False)
            print(DX.sort_values(ascending=False),'\n')"""
        #DXbycty= self.df1.groupby(['cty_name_destination','reporting_time'])['DX'].sum().sort_values(ascending=False)
        #self.DXbycty = DXbycty.unstack().sort_values(self.periods[-1],ascending=False)
        DXbycty = pd.pivot_table(self.df1, values='DX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        self.DXbycty_pctshare = self.analysis_share_of_overall_DX(DXbycty)
        self.DXbycty = self.mix_conversion_with_pct(DXbycty)

        #concatenate the fig, $ change and % share
        self.DXbycty = pd.concat([self.DXbycty, self.DXbycty_pctshare], axis=1).dropna(axis='columns', how='all')
        self.DXbycty.rename(index={'All':'All individual countries'}, inplace=True)
        self.DXbycty.index.name = "%s %s" % (self.currency, self.money)

    def analysis_TX_cty(self):
        #for p in self.periods:
        """
        for p in self.periods:
            print(p)
            TX = self.df1_allperiods[p].groupby(['cty_name_destination'])['TX'].sum()
            print(type(TX))
            TX_sorted = TX.sort_values(ascending=False)
            print(TX.sort_values(ascending=False),'\n')"""
        #TXbycty= self.df1.groupby(['cty_name_destination','reporting_time'])['TX'].sum().sort_values(ascending=False)
        #self.TXbycty = TXbycty.unstack().sort_values(self.periods[-1],ascending=False)
        TXbycty = pd.pivot_table(self.df1, values='TX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        self.TXbycty_pctshare = self.analysis_share_of_overall_TX(TXbycty)
        self.TXbycty = self.mix_conversion_with_pct(TXbycty)

        #concatenate the fig, $ change and % share
        self.TXbycty = pd.concat([self.TXbycty, self.TXbycty_pctshare], axis=1).dropna(axis='columns', how='all')
        self.TXbycty.rename(index={'All':'All individual countries'}, inplace=True)
        self.TXbycty.index.name = "%s %s" % (self.currency, self.money)

    def analysis_IM_cty_byOrigin(self):
        #for p in self.periods:
        """
        for p in self.periods:
            print(p)
            TX = self.df1_allperiods[p].groupby(['cty_name_destination'])['TX'].sum()
            print(type(TX))
            TX_sorted = TX.sort_values(ascending=False)
            print(TX.sort_values(ascending=False),'\n')"""
        #TXbycty= self.df1.groupby(['cty_name_destination','reporting_time'])['TX'].sum().sort_values(ascending=False)
        #self.TXbycty = TXbycty.unstack().sort_values(self.periods[-1],ascending=False)
        IMbyctyasO = pd.pivot_table(self.df2, values='IMbyO', index=['cty_name_origin'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        self.IMbyctyasO_pctshare = self.analysis_share_of_overall_IM(IMbyctyasO)
        self.IMbyctyasO = self.mix_conversion_with_pct(IMbyctyasO)

        #concatenate the fig, $ change and % share
        self.IMbyctyasO = pd.concat([self.IMbyctyasO, self.IMbyctyasO_pctshare], axis=1).dropna(axis='columns', how='all')
        self.IMbyctyasO.rename(index={'All':'All individual countries'}, inplace=True)
        self.IMbyctyasO.index.name = "%s %s" % (self.currency, self.money)


    def analysis_TX_regions(self):
        #self.TXbyEU = self.df1[self.df1['cty_name_destination'].isin(EU_cty_name)]
        #self.TXbyEU = self.TXbycty[self.TXbycty.index.isin(EU_cty_name)]
        EU_df1 = self.df1[self.df1['cty_name_destination'].isin(EU_cty_name)]
        Asean_df1 = self.df1[self.df1['cty_name_destination'].isin(Asean_cty_name)]
        Asia_df1 = self.df1[self.df1['cty_name_destination'].isin(Asia_cty_name)]

        TXbyEU = pd.pivot_table(EU_df1, values='TX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)
        TXbyAsean = pd.pivot_table(Asean_df1, values='TX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)
        TXbyAsia = pd.pivot_table(Asia_df1, values='TX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        #print(self.TXbyEU)
        #print(self.TXbyAsean)
        self.TXbyEU_pctshare = self.analysis_share_of_overall_TX(TXbyEU)
        self.TXbyEU = self.mix_conversion_with_pct(TXbyEU)
        self.TXbyEU = pd.concat([self.TXbyEU, self.TXbyEU_pctshare], axis=1).dropna(axis='columns', how='all')
        self.TXbyEU.rename(index={'All':'EU'}, inplace=True)
        self.TXbyEU.index.name = "%s %s" % (self.currency, self.money)

        self.TXbyAsean_pctshare = self.analysis_share_of_overall_TX(TXbyAsean)
        self.TXbyAsean = self.mix_conversion_with_pct(TXbyAsean)
        self.TXbyAsean = pd.concat([self.TXbyAsean, self.TXbyAsean_pctshare], axis=1).dropna(axis='columns', how='all')
        self.TXbyAsean.rename(index={'All':'ASEAN'}, inplace=True)
        self.TXbyAsean.index.name = "%s %s" % (self.currency, self.money)

        self.TXbyAsia_pctshare = self.analysis_share_of_overall_TX(TXbyAsia)
        self.TXbyAsia = self.mix_conversion_with_pct(TXbyAsia)
        self.TXbyAsia = pd.concat([self.TXbyAsia, self.TXbyAsia_pctshare], axis=1).dropna(axis='columns', how='all')
        self.TXbyAsia.rename(index={'All':'ASIA'}, inplace=True)
        self.TXbyAsia.index.name = "%s %s" % (self.currency, self.money)

    def analysis_DX_regions(self):
        #self.DXbyEU = self.df1[self.df1['cty_name_destination'].isin(EU_cty_name)]
        #self.DXbyEU = self.DXbycty[self.DXbycty.index.isin(EU_cty_name)]
        EU_df1 = self.df1[self.df1['cty_name_destination'].isin(EU_cty_name)]
        Asean_df1 = self.df1[self.df1['cty_name_destination'].isin(Asean_cty_name)]
        Asia_df1 = self.df1[self.df1['cty_name_destination'].isin(Asia_cty_name)]

        DXbyEU = pd.pivot_table(EU_df1, values='DX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)
        DXbyAsean = pd.pivot_table(Asean_df1, values='DX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)
        DXbyAsia = pd.pivot_table(Asia_df1, values='DX', index=['cty_name_destination'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        #print(self.DXbyEU)
        #print(self.DXbyAsean)
        self.DXbyEU_pctshare = self.analysis_share_of_overall_DX(DXbyEU)
        self.DXbyEU = self.mix_conversion_with_pct(DXbyEU)
        self.DXbyEU = pd.concat([self.DXbyEU, self.DXbyEU_pctshare], axis=1).dropna(axis='columns', how='all')
        self.DXbyEU.rename(index={'All':'EU'}, inplace=True)
        self.DXbyEU.index.name = "%s %s" % (self.currency, self.money)

        self.DXbyAsean_pctshare = self.analysis_share_of_overall_DX(DXbyAsean)
        self.DXbyAsean = self.mix_conversion_with_pct(DXbyAsean)
        self.DXbyAsean = pd.concat([self.DXbyAsean, self.DXbyAsean_pctshare], axis=1).dropna(axis='columns', how='all')
        self.DXbyAsean.rename(index={'All':'ASEAN'}, inplace=True)
        self.DXbyAsean.index.name = "%s %s" % (self.currency, self.money)

        self.DXbyAsia_pctshare = self.analysis_share_of_overall_DX(DXbyAsia)
        self.DXbyAsia = self.mix_conversion_with_pct(DXbyAsia)
        self.DXbyAsia = pd.concat([self.DXbyAsia, self.DXbyAsia_pctshare], axis=1).dropna(axis='columns', how='all')
        self.DXbyAsia.rename(index={'All':'ASIA'}, inplace=True)
        self.DXbyAsia.index.name = "%s %s" % (self.currency, self.money)

    def analysis_IM_regions_byOrigin(self):
        #self.TXbyEU = self.df1[self.df1['cty_name_destination'].isin(EU_cty_name)]
        #self.TXbyEU = self.TXbycty[self.TXbycty.index.isin(EU_cty_name)]
        Europe_df2 = self.df2[self.df2['cty_name_origin'].isin(Europe_cty_name)]

        IMbyEurope = pd.pivot_table(Europe_df2, values='IMbyO', index=['cty_name_origin'],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        self.IMbyEurope_pctshare = self.analysis_share_of_overall_IM(IMbyEurope)
        self.IMbyEurope = self.mix_conversion_with_pct(IMbyEurope)
        self.IMbyEurope = pd.concat([self.IMbyEurope, self.IMbyEurope_pctshare], axis=1).dropna(axis='columns', how='all')
        self.IMbyEurope.rename(index={'All':'EUROPE'}, inplace=True)
        self.IMbyEurope.index.name = "%s %s" % (self.currency, self.money)


    def analysis_TX_products(self, codetypeanddigit1, codetypeanddigit2=None, codetypeanddigit3=None):
        self.codetypeanddigit=[codetypeanddigit1, codetypeanddigit2, codetypeanddigit3]

        print('hi', self.codetypeanddigit,'\n')

        if codetypeanddigit2==None and codetypeanddigit3==None:
            TX_product = pd.pivot_table(self.df1, values='TX', index=[codetypeanddigit1],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        elif codetypeanddigit3==None:
            TX_product = pd.pivot_table(self.df1, values='TX', index=[codetypeanddigit1,codetypeanddigit2],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        else:
            TX_product = pd.pivot_table(self.df1, values='TX', index=[codetypeanddigit1,codetypeanddigit2,codetypeanddigit3],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        self.TXbyproduct_pctshare = self.analysis_share_of_overall_TX(TX_product)
        self.TXbyproduct = self.mix_conversion_with_pct(TX_product)
        self.TXbyproduct = pd.concat([self.TXbyproduct, self.TXbyproduct_pctshare], axis=1).dropna(axis='columns', how='all')

    def analysis_DX_products(self, codetypeanddigit1, codetypeanddigit2=None, codetypeanddigit3=None):
        self.codetypeanddigit=[codetypeanddigit1, codetypeanddigit2, codetypeanddigit3]

        print('hi', self.codetypeanddigit,'\n')

        if codetypeanddigit2==None and codetypeanddigit3==None:
            DX_product = pd.pivot_table(self.df1, values='DX', index=[codetypeanddigit1],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        elif codetypeanddigit3==None:
            DX_product = pd.pivot_table(self.df1, values='DX', index=[codetypeanddigit1,codetypeanddigit2],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        else:
            DX_product = pd.pivot_table(self.df1, values='DX', index=[codetypeanddigit1,codetypeanddigit2,codetypeanddigit3],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        self.DXbyproduct_pctshare = self.analysis_share_of_overall_DX(DX_product)
        self.DXbyproduct = self.mix_conversion_with_pct(DX_product)
        self.DXbyproduct = pd.concat([self.DXbyproduct, self.DXbyproduct_pctshare], axis=1).dropna(axis='columns', how='all')

    def analysis_share_of_overall_DX(self, tabledata):
        #print("hi",self.table1)
        table = tabledata/self.table1.loc["Domestic Exports"]*100
        #change %share columns name
        #print("testing")
        table.columns = [c+"_% Share of overall DX" for c in table.columns]
        return table


    def analysis_share_of_overall_TX(self, tabledata):
        #print("hi",self.table1)
        table = tabledata/self.table1.loc["Total Exports"]*100
        #change %share columns name
        #print("testing")
        table.columns = [c+"_% Share of overall TX" for c in table.columns]
        return table

    def analysis_share_of_overall_IM(self, tabledata):
        #print("hi",self.table1)
        table = tabledata/self.table1.loc["Imports"]*100
        #change %share columns name
        #print("testing")
        table.columns = [c+"_% Share of overall IM" for c in table.columns]
        return table


    def export_to_excel(self,numberofdecimal, periodsdata=False):
        #saving to excel files
        original_path = os.getcwd()
        folder_path="Industry"
        file_path=folder_path+"/"+self.currency+"/"+self.money
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        os.chdir(file_path)
        # include data
        filename = f"{self.group_no}_{self.name}"

        writer1 = pd.ExcelWriter(f'df1_{filename}_{self.currency}__{self.money}.xlsx')
        writer2 = pd.ExcelWriter(f'df2_{filename}_{self.currency}__{self.money}.xlsx')
        writer3 = pd.ExcelWriter(f'df3_{filename}_{self.currency}__{self.money}.xlsx')
        writer4 = pd.ExcelWriter(f'Analysis_{filename}_{self.currency}__{self.money}.xlsx')

        allwriters = [writer1,writer2,writer3,writer4]

        if periodsdata == True:
            for p in self.periods:
                print(p,"\n")
                self.df1_allperiods[p].to_excel(writer1, p)
                self.df2_allperiods[p].to_excel(writer2, p)
                self.df3_allperiods[p].to_excel(writer3, p)
        self.table1_result.to_excel(writer4, 'table1', float_format=f"%.{numberofdecimal}f" )
        self.TXbycty.to_excel(writer4, 'TXbycty', float_format=f"%.{numberofdecimal}f" )
        self.TXbyEU.to_excel(writer4, 'TXbyEU', float_format=f"%.{numberofdecimal}f" )
        self.TXbyAsean.to_excel(writer4, 'TXbyAsean',float_format=f"%.{numberofdecimal}f" )
        self.TXbyAsia.to_excel(writer4, 'TXbyAsia',float_format=f"%.{numberofdecimal}f" )

        self.IMbyctyasO.to_excel(writer4, 'IMbyctyasOrigin',float_format=f"%.{numberofdecimal}f" )
        self.IMbyEurope.to_excel(writer4, 'IMbyEuropeasOrigin',float_format=f"%.{numberofdecimal}f" )

        self.TXbyproduct.to_excel(writer4, 'TXbyproduct',float_format=f"%.{numberofdecimal}f" )

        #DX
        self.DXbycty.to_excel(writer4, 'DXbycty', float_format=f"%.{numberofdecimal}f" )
        self.DXbyEU.to_excel(writer4, 'DXbyEU', float_format=f"%.{numberofdecimal}f" )
        self.DXbyAsean.to_excel(writer4, 'DXbyAsean',float_format=f"%.{numberofdecimal}f" )
        self.DXbyAsia.to_excel(writer4, 'DXbyAsia',float_format=f"%.{numberofdecimal}f" )
        self.DXbyproduct.to_excel(writer4, 'DXbyproduct',float_format=f"%.{numberofdecimal}f" )

        [w.save() for w in allwriters]

        os.chdir(original_path)

    def __str__(self):
        return "this class is %s, periods from %s-%s" % (self.name, self.periods[0], self.periods[-1])

#export data for each industry
if __name__ == '__main__':
    # calculate time spent
    start_time = time.time()

    dollar = {'HKD':1, 'USD':7.8}
    unit = {'TH':1000, 'MN':1000000}
    currency = 'HKD'
    money = 'MN'

    # set number of decimals
    numberofdecimal=3

    print(f"********* {currency} {money}")
    # input periods for the report
    startyear, endytd = 2016, 201907
    # acquire hsccit data from startyear to endyear and combine them into dataframe
    # acquire hscoit data from startyear to endyear and combine them into dataframe
    # acquire hscoccit data from startyear to endyear and combine them into dataframe
    df1,df2,df3 = hsccit_mergedf(startyear, endytd),\
                  hscoit_mergedf(startyear, endytd),\
                  hscoccit_mergedf(startyear, endytd)
    df1,df2,df3 = [exclgold(x) for x in (df1,df2,df3)]

    # sort the periods for functions use later
    periods = sorted(set(df1.reporting_time))
    print(periods)

    cty_dict = get_geography_code()

    df1['cty_name_destination']= [cty_dict[x] for x in df1.f3]
    df2['cty_name_origin']= [cty_dict[x] for x in df2.f3]

    df3['cty_name_origin']= [cty_dict[x] for x in df3.f3]
    df3['cty_name_destination']= [cty_dict[x] for x in df3.f4]

    df1.rename(columns={'f2':'HS-8','f3':'f3_consignment'}, inplace=True)
    df2.rename(columns={'f2':'HS-8','f3':'f3_origin'}, inplace=True)
    df3.rename(columns={'f2':'HS-8','f3':'f3_origin','f4':'f4_destination'}, inplace=True)

    #boolean for overall data for all periods in an excel file
    overall = False

    #export the overall data excel file
    if overall:
        writer = pd.ExcelWriter(f'overall.xlsx')
        #df1.to_excel(writer,"df1")
        #df2.to_excel(writer,"df2")
        df3.to_excel(writer,"df3")
        writer.save()

    industrycode = get_industry_code()
    for k, v in industrycode.items():
        group_no = k
        name = v['industry_name']
        codetype = v['code_type'][0]
        product_code = v['codes']
        #print(group_no, name, code, product_code)

        table1,table2,table3 = df1,df2,df3

        # length of the product codes
        len_pcode = list(set([len(c) for c in product_code]))

        if len(len_pcode)>=2:
            commodity_digit1 = codetype +'-'+ str(len_pcode[0])
            commodity_digit2 = codetype +'-'+ str(len_pcode[1])
            commodity_digit3 = codetype +'-'+ str(len_pcode[-1])
            if len(len_pcode)>=3:
                commodity_digit3 = codetype +'-'+ str(len_pcode[2])

            products1,products2,products3=[],[],[]
            for code in product_code:

                if len(code) == int(len_pcode[0]):
                    products1.append(code)
                elif len(code) == int(len_pcode[1]):
                    products2.append(code)
                elif len(code) == int(len_pcode[2]):
                    products3.append(code)

            #include the industry product for all different lengths of codes
            table1_all_periods = table1[table1[commodity_digit1].isin(products1) | table1[commodity_digit2].isin(products2) | table1[commodity_digit3].isin(products3)]
            table2_all_periods = table2[table2[commodity_digit1].isin(products1) | table2[commodity_digit2].isin(products2) | table2[commodity_digit3].isin(products3)]
            table3_all_periods = table3[table3[commodity_digit1].isin(products1) | table3[commodity_digit2].isin(products2) | table3[commodity_digit3].isin(products3)]
            #print(commodity_digit1,commodity_digit2,commodity_digit3)
            commodity_digit = [commodity_digit1,commodity_digit2,commodity_digit3]
            # sorted is prefered than sort
            commodity_digit = sorted(set(commodity_digit))

        else:
            commodity_digit = codetype +'-'+ str(len_pcode[0])
            table1_all_periods = table1[table1[commodity_digit].isin(product_code)]
            table2_all_periods = table2[table2[commodity_digit].isin(product_code)]
            table3_all_periods = table3[table3[commodity_digit].isin(product_code)]
            commodity_digit=commodity_digit


        cols1 = ['DX','RX','TX','IM','TT','HS-2','HS-4','HS-6','HS-8','SITC-1','SITC-2','SITC-3','SITC-4','SITC-5','f1','f3_consignment','cty_name_destination','reporting_time']
        cols2 = ['IMbyO', 'IMbyO_Q','HS-2','HS-4','HS-6','HS-8','SITC-1','SITC-2','SITC-3','SITC-4','SITC-5','f1','f3_origin','cty_name_origin','reporting_time']
        cols3 = ['RX_O','RX_Q','HS-2','HS-4','HS-6','HS-8','SITC-1','SITC-2','SITC-3','SITC-4','SITC-5','f1','f3_origin','f4_destination','cty_name_origin','cty_name_destination','reporting_time']

        # implement class
        dataset = [table1_all_periods[cols1],table2_all_periods[cols2],table3_all_periods[cols3]]

        print(currency, money)
        industrycode[k]['class']=Industry(group_no,name,periods,dataset, currency, money)
        print(industrycode[k]['class'])
        print(commodity_digit)

        #print no. of industry instance
        print("Industry no.: %d " % Industry.noofindustry)

        industrycode[k]['class'].run_df_table()
        df1_all_periods_table = industrycode[k]['class'].run_df_separate()

        #table 1 result
        industrycode[k]['class'].df_table1()
        #print(industrycode[k]['class'].df_table1(),'\n')

        #table 2 DX, TX, IM by country
        industrycode[k]['class'].analysis_DX_cty()
        industrycode[k]['class'].analysis_DX_regions()
        industrycode[k]['class'].analysis_TX_cty()
        industrycode[k]['class'].analysis_TX_regions()
        #break
        industrycode[k]['class'].analysis_IM_cty_byOrigin()
        industrycode[k]['class'].analysis_IM_regions_byOrigin()

        #table 3 TX by product
        if isinstance(commodity_digit,str):
            industrycode[k]['class'].analysis_TX_products(commodity_digit)
            industrycode[k]['class'].analysis_DX_products(commodity_digit)
        if isinstance(commodity_digit,list):
            industrycode[k]['class'].analysis_TX_products(*commodity_digit)
            industrycode[k]['class'].analysis_DX_products(*commodity_digit)

        #set decimal space
        #saving to excel files
        outputexcel=True
        if outputexcel == True:
            industrycode[k]['class'].export_to_excel(numberofdecimal=numberofdecimal, periodsdata=False)

#calculate time spent
elapsed_time = round(time.time() - start_time, 2)
print("time used: ", elapsed_time, " seconds")
