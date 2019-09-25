import pprint
import pandas as pd
import numpy as np
import time
import xlsxwriter
import calendar
import sys, os
from BSO.rawdata import mergedf
from BSO.R1_figures import major_commodity_fig
from BSO.R1_figures import trades_ranking_bycty, trades_ranking_bycty_multi_yrs
from BSO.R1_figures import six_trades_ranking_bycty_multi_yrs, find_ranking
from BSO.geography import get_geography_code, get_geography_regcnty_code
from BSO.industry import get_industry_code

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

    def analysis_bycty(self,tradetype):
        data = self.df2 if tradetype == 'IMbyO' else self.df1
        sorting_index = 'cty_name_origin' if tradetype == 'IMbyO' else 'cty_name_destination'

        bycty = pd.pivot_table(data, values=tradetype, index=sorting_index,columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)
        print(bycty)

        bycty_pctshare = self.analysis_share_of_overall(bycty,tradetype)
        bycty = self.mix_conversion_with_pct(bycty)

        #concatenate the fig, $ change and % share
        bycty = pd.concat([bycty, bycty_pctshare], axis=1).dropna(axis='columns', how='all')
        bycty.rename(index={'All':'All individual countries'}, inplace=True)
        bycty.index.name = "%s %s" % (self.currency, self.money)

        print('here bycty')
        print(bycty)
        return bycty

    def analysis_byregions(self,tradetype, region):

        if region == 'EU': region_cty_name=EU_cty_name
        elif region == 'Asean': region_cty_name=Asean_cty_name
        elif region == 'Asia': region_cty_name=Asia_cty_name
        elif region == 'Europe': region_cty_name=Europe_cty_name

        data = self.df2 if tradetype == 'IMbyO' else self.df1
        sorting_index = 'cty_name_origin' if tradetype == 'IMbyO' else 'cty_name_destination'

        _df = data[data[sorting_index].isin(region_cty_name)]

        byregion = pd.pivot_table(_df1, values=tradetype, index=[sorting_index],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        #print(byregion)
        #print(self.TXbyAsean)
        byregion_pctshare = self.analysis_share_of_overall(byregion,tradetype)
        byregion = self.mix_conversion_with_pct(byregion)
        byregion = pd.concat([byregion, byregion_pctshare], axis=1).dropna(axis='columns', how='all')
        byregion.rename(index={'All':region}, inplace=True)
        byregion.index.name = "%s %s" % (self.currency, self.money)

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
        self.TXbyEU_pctshare = self.analysis_share_of_overall(TXbyEU,'TX')
        self.TXbyEU = self.mix_conversion_with_pct(TXbyEU)
        self.TXbyEU = pd.concat([self.TXbyEU, self.TXbyEU_pctshare], axis=1).dropna(axis='columns', how='all')
        self.TXbyEU.rename(index={'All':'EU'}, inplace=True)
        self.TXbyEU.index.name = "%s %s" % (self.currency, self.money)

        self.TXbyAsean_pctshare = self.analysis_share_of_overall(TXbyAsean,'TX')
        self.TXbyAsean = self.mix_conversion_with_pct(TXbyAsean)
        self.TXbyAsean = pd.concat([self.TXbyAsean, self.TXbyAsean_pctshare], axis=1).dropna(axis='columns', how='all')
        self.TXbyAsean.rename(index={'All':'ASEAN'}, inplace=True)
        self.TXbyAsean.index.name = "%s %s" % (self.currency, self.money)

        self.TXbyAsia_pctshare = self.analysis_share_of_overall(TXbyAsia,'TX')
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
        self.DXbyEU_pctshare = self.analysis_share_of_overall(DXbyEU,'DX')
        self.DXbyEU = self.mix_conversion_with_pct(DXbyEU)
        self.DXbyEU = pd.concat([self.DXbyEU, self.DXbyEU_pctshare], axis=1).dropna(axis='columns', how='all')
        self.DXbyEU.rename(index={'All':'EU'}, inplace=True)
        self.DXbyEU.index.name = "%s %s" % (self.currency, self.money)

        self.DXbyAsean_pctshare = self.analysis_share_of_overall(DXbyAsean, 'DX')
        self.DXbyAsean = self.mix_conversion_with_pct(DXbyAsean)
        self.DXbyAsean = pd.concat([self.DXbyAsean, self.DXbyAsean_pctshare], axis=1).dropna(axis='columns', how='all')
        self.DXbyAsean.rename(index={'All':'ASEAN'}, inplace=True)
        self.DXbyAsean.index.name = "%s %s" % (self.currency, self.money)

        self.DXbyAsia_pctshare = self.analysis_share_of_overall(DXbyAsia, 'DX')
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

        self.IMbyEurope_pctshare = self.analysis_share_of_overall(IMbyEurope,'IMbyO')
        self.IMbyEurope = self.mix_conversion_with_pct(IMbyEurope)
        self.IMbyEurope = pd.concat([self.IMbyEurope, self.IMbyEurope_pctshare], axis=1).dropna(axis='columns', how='all')
        self.IMbyEurope.rename(index={'All':'EUROPE'}, inplace=True)
        self.IMbyEurope.index.name = "%s %s" % (self.currency, self.money)

    def analysis_byproducts(self, tradetype, codetypeanddigit1, codetypeanddigit2=None, codetypeanddigit3=None):
        self.codetypeanddigit=[codetypeanddigit1, codetypeanddigit2, codetypeanddigit3]

        print('hi', self.codetypeanddigit,'\n')
        data = self.df2 if tradetype == 'IMbyO' else self.df1

        if codetypeanddigit2==None and codetypeanddigit3==None:
            byproduct = pd.pivot_table(data, values=tradetype, index=[codetypeanddigit1],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        elif codetypeanddigit3==None:
            byproduct = pd.pivot_table(data, values=tradetype, index=[codetypeanddigit1,codetypeanddigit2],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        else:
            byproduct = pd.pivot_table(data, values=tradetype, index=[codetypeanddigit1,codetypeanddigit2,codetypeanddigit3],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        byproduct_pctshare = self.analysis_share_of_overall(byproduct,tradetype)
        byproduct = self.mix_conversion_with_pct(byproduct)
        byproduct = pd.concat([byproduct, byproduct_pctshare], axis=1).dropna(axis='columns', how='all')

        return byproduct

    def analysis_share_of_overall(self, tabledata, tradetype):
        if tradetype == 'DX':
            tradelabel = "Domestic Exports"
        elif tradetype == 'TX':
            tradelabel = "Total Exports"
        elif tradetype == 'IMbyO':
            tradelabel = "Imports"
        print(tradelabel)

        table = tabledata/self.table1.loc[tradelabel]*100
        #change %share columns name
        #print("testing")
        table.columns = [c+f"_% Share of overall {tradetype}" for c in table.columns]
        return table

    def export_to_excel(self,numberofdecimal, periodsdata=False):
        #saving to excel files
        original_path = os.getcwd()
        folder_path="Industry"
        file_path=folder_path+"/"+self.periods[0]+"-"+self.periods[-1]+"/"+self.currency+"/"+self.money
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
    df1,df2,df3 = [mergedf(startyear, endytd, type) for type in ["hsccit","hscoit","hscoccit"]]

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
        df1.to_excel(writer,"df1")
        df2.to_excel(writer,"df2")
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
        industrycode[k]['class'].TXbycty=industrycode[k]['class'].analysis_bycty("TX")
        industrycode[k]['class'].DXbycty=industrycode[k]['class'].analysis_bycty("DX")
        industrycode[k]['class'].IMbyctyasO=industrycode[k]['class'].analysis_bycty("IMbyO")

        #table 3 by region
        industrycode[k]['class'].DXbyEU=industrycode[k]['class'].analysis_byregions('DX','EU')
        industrycode[k]['class'].DXbyAsean=industrycode[k]['class'].analysis_byregions('DX','Asean')
        industrycode[k]['class'].DXbyAsia=industrycode[k]['class'].analysis_byregions('DX','Asia')
        #industrycode[k]['class'].analysis_TX_cty()
        industrycode[k]['class'].TXbyEU=industrycode[k]['class'].analysis_byregions('TX','EU')
        industrycode[k]['class'].TXbyAsean=industrycode[k]['class'].analysis_byregions('TX','Asean')
        industrycode[k]['class'].TXbyAsia=industrycode[k]['class'].analysis_byregions('TX','Asia')

        industrycode[k]['class'].IMbyEurope=industrycode[k]['class'].analysis_byregions('IMbyO','Europe')

        #table 3 TX by product
        if isinstance(commodity_digit,str):
            industrycode[k]['class'].TXbyproduct=industrycode[k]['class'].analysis_byproducts('TX',commodity_digit)
            industrycode[k]['class'].DXbyproduct=industrycode[k]['class'].analysis_byproducts('DX',commodity_digit)
        if isinstance(commodity_digit,list):
            industrycode[k]['class'].TXbyproduct=industrycode[k]['class'].analysis_byproducts('TX',*commodity_digit)
            industrycode[k]['class'].DXbyproduct=industrycode[k]['class'].analysis_byproducts('DX',*commodity_digit)

        #set decimal space
        #saving to excel files
        outputexcel=True
        if outputexcel == True:
            industrycode[k]['class'].export_to_excel(numberofdecimal=numberofdecimal, periodsdata=False)

#calculate time spent
elapsed_time = round(time.time() - start_time, 2)
print("time used: ", elapsed_time, " seconds")
