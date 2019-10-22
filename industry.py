import pandas as pd
import numpy as np
import export.export_file as ex
import time
import os
from BSO.rawdata import mergedf
from BSO.geography import get_geography_code, get_geography_regcnty_code
from BSO.industry import get_industry_code


# get region name and codes
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

# implement class
class Industry(object):
    """Class for Industry to export excel data"""
    noofindustry=0
    def __init__(self, group_no, name, periods, data, currency, money, needsymbol):
        Industry.noofindustry+=1
        self.group_no=group_no
        self.name = name
        self.periods = periods
        self.data = data
        self.currency = currency
        self.money = money
        self.needsymbol = needsymbol
        if len(periods)==4:
            self.sorting=[periods[-1],periods[-2],periods[-3],periods[-4]]
        if len(periods)==5:
            self.sorting=[periods[-1],periods[-2],periods[-4],periods[-5]]

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
        RXbyCNasOrigin = self.df3_allperiods[select_period][self.df3_allperiods[select_period].f3_origin==631].RXbyO.sum()
        TX = self.df1_allperiods[select_period].TX.sum()
        IM = self.df1_allperiods[select_period].IM.sum()
        TT = self.df1_allperiods[select_period].TT.sum()

        TX_Q = self.df1_allperiods[select_period].TX_Q.sum()
        DX_Q = self.df1_allperiods[select_period].DX_Q.sum()
        RX_Q = self.df1_allperiods[select_period].RX_Q.sum()
        IMbyO_Q =self.df2_allperiods[select_period].IMbyO_Q.sum()
        TT_Q = self.df1_allperiods[select_period].TT_Q.sum()

        return DX, RX, RXbyCNasOrigin, TX, IM, TT, TX_Q, DX_Q, RX_Q, IMbyO_Q, TT_Q

    def df_table1(self):
        self.table1_dict={}
        for p in self.periods:
            self.table1_dict[p]=self.analysis_table1(p)
            #print(self.table1_dict[p])

        self.table1=pd.DataFrame(self.table1_dict)

        table1_idx = ["Domestic Exports", "Re-exports", "   of Chinese mainland Origin", "Total Exports", "Imports", "Total Trades", \
                    "Total Exports Quantity", "Domestic Exports Quantity", "Re-exports Quantity", "Imports Quantity", "Total Trades Quantity"]
        self.table1.set_index([table1_idx], inplace=True)
        self.table1_result = self.mix_conversion_with_pct(self.table1, self.periods)
        self.table1_result.index.name = "%s %s %s" % (self.currency, self.money, "(year to date)")

        #print(self.table1_result)

    def mix_conversion_with_pct(self, tablefig, periods):
        # validate if empty dataframe
        if not periods: return tablefig

        periods = sorted(periods, reverse=True)

        if int(periods[0][-2:])!=12:
            year=tablefig.iloc[:,[0,1,3]].pct_change(axis='columns')
            ytd=tablefig.iloc[:,[2,4]].pct_change(axis='columns')
            tablepcc=pd.concat([year,ytd],axis=1)

        elif int(periods[0][-2:])==12:
            tablepcc=tablefig.pct_change(axis='columns')
        #change pecentage columns name
        tablepcc.columns = [c+"_% CHG" for c in tablepcc.columns]

        # 1) make percentage times 100
        tablepcc*=100
        # 2) calculate % share of overall TX

        # 3) money_converion
        tablefig = self.money_conversion(tablefig)

        # denote symbol
        if self.needsymbol == True:
            tablefig = ex.denotesymbol(tablefig, datatype='fig')
            # drop NA for change
            tablepcc = tablepcc.dropna(axis='columns', how='all')
            tablepcc = ex.denotesymbol(tablepcc, datatype='chg')

        table_result = pd.concat([tablefig, tablepcc], axis=1).dropna(axis='columns', how='all')
        return table_result.sort_index(axis=1)

    def analysis_bycty(self,tradetype):
        # different trade types use different dataframe and sorting
        if tradetype == 'IMbyO'or tradetype == 'IMbyO_Q':
            data = self.df2
            sorting_index = 'cty_name_origin'

        elif tradetype == 'RXbyO' or tradetype == 'RXbyO_Q':
            data = self.df3
            sorting_index = 'cty_name_origin'

        else:
            data = self.df1
            sorting_index = 'cty_name_destination'

        # sorting
        sorted_period = sorted(set(data.reporting_time), reverse= True)

        bycty = pd.pivot_table(data, values=tradetype, index=sorting_index,columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=sorted_period,ascending=False)

        bycty_pctshare = self.analysis_share_of_overall(bycty,tradetype)
        bycty = self.mix_conversion_with_pct(bycty,self.periods)

        # combine the fig, $ change and % share
        bycty = pd.concat([bycty, bycty_pctshare], axis=1).dropna(axis='columns', how='all')
        bycty.rename(index={'All':'All individual countries'}, inplace=True)
        bycty.index.name = "%s %s %s" % (self.currency, self.money, "(year to date)")

        # drop the useless overall column in pivot table
        bycty = bycty.drop('All', 1)
        return bycty

    def analysis_byregions(self,tradetype, region):

        if region == 'EU': region_cty_name=EU_cty_name
        elif region == 'Asean': region_cty_name=Asean_cty_name
        elif region == 'Asia': region_cty_name=Asia_cty_name
        elif region == 'Europe': region_cty_name=Europe_cty_name

        # different trade types use different dataframe and sorting
        if tradetype == 'IMbyO'or tradetype == 'IMbyO_Q':
            data = self.df2
            sorting_index = 'cty_name_origin'

        elif tradetype == 'RXbyO' or tradetype == 'RXbyO_Q':
            data = self.df3
            sorting_index = 'cty_name_origin'

        else:
            data = self.df1
            sorting_index = 'cty_name_destination'

        _df = data[data[sorting_index].isin(region_cty_name)]
        #### not perfect, need adjust
        # sorting
        sorted_period = sorted(set(_df.reporting_time), reverse= True)
        #print(sorted_period)

        try:
            print(_df)
            byregion = pd.pivot_table(_df, values=tradetype, index=[sorting_index],columns=['reporting_time'],\
                      aggfunc=np.sum, margins=True).sort_values(by=sorted_period,ascending=False)
        except:
            print("need adjust")
            byregion = pd.DataFrame()
            return byregion
            #byregion = pd.pivot_table(_df, values=tradetype, index=[sorting_index],columns=['reporting_time'],\
                      #aggfunc=np.sum, margins=True).sort_values(by=periods[-1],ascending=False)

        ####
        byregion_pctshare = self.analysis_share_of_overall(byregion,tradetype)
        byregion = self.mix_conversion_with_pct(byregion,sorted_period)
        byregion = pd.concat([byregion, byregion_pctshare], axis=1).dropna(axis='columns', how='all')
        byregion.rename(index={'All':region}, inplace=True)
        byregion.index.name = "%s %s %s" % (self.currency, self.money, "(year to date)")

        if byregion.empty:
            print('here')
        # drop the useless overall column in pivot table
        else:
            byregion = byregion.drop('All', 1)
            return byregion

    def analysis_byproducts(self, tradetype, codetypeanddigit1, codetypeanddigit2=None, codetypeanddigit3=None):
        self.codetypeanddigit=[codetypeanddigit1, codetypeanddigit2, codetypeanddigit3]
        data = self.df2 if tradetype == 'IMbyO' or tradetype == 'IMbyO_Q' else self.df1

        if codetypeanddigit2==None and codetypeanddigit3==None:
            #if codetypeanddigit1=='SITC-2':codetypeanddigit1=['SITC-2','SITC-3']
            #elif codetypeanddigit1=='SITC-3':codetypeanddigit1=['SITC-3','SITC-5']

            byproduct = pd.pivot_table(data, values=tradetype, index=codetypeanddigit1,columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=self.sorting,ascending=False)

        elif codetypeanddigit3==None:
            byproduct = pd.pivot_table(data, values=tradetype, index=[codetypeanddigit1,codetypeanddigit2],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=self.sorting,ascending=False)

        else:
            byproduct = pd.pivot_table(data, values=tradetype, index=[codetypeanddigit1,codetypeanddigit2,codetypeanddigit3],columns=['reporting_time'],\
                                  aggfunc=np.sum, margins=True).sort_values(by=self.sorting,ascending=False)

        byproduct_pctshare = self.analysis_share_of_overall(byproduct,tradetype)
        byproduct = self.mix_conversion_with_pct(byproduct,self.periods)
        byproduct = pd.concat([byproduct, byproduct_pctshare], axis=1).dropna(axis='columns', how='all')

        # drop the useless overall column in pivot table
        byproduct = byproduct.drop('All', 1)
        return byproduct

    def analysis_share_of_overall(self, tabledata, tradetype):
        if tradetype == 'DX':
            tradelabel = "Domestic Exports"
        elif tradetype == 'RX'or tradetype == 'RXbyO':
            tradelabel = "Re-exports"
        elif tradetype == 'TX':
            tradelabel = "Total Exports"
        elif tradetype == 'IM' or tradetype == 'IMbyO':
            tradelabel = "Imports"
        elif tradetype == 'TT':
            tradelabel = "Total Trades"

        elif tradetype == 'IM_Q' or tradetype == 'IMbyO_Q':
            tradelabel = "Imports Quantity"
        elif tradetype == 'TX_Q':
            tradelabel = "Total Exports Quantity"
        elif tradetype == 'RX_Q'or tradetype == 'RXbyO_Q':
            tradelabel = "Re-exports Quantity"
        elif tradetype == 'DX_Q':
            tradelabel = "Domestic Exports Quantity"
        elif tradetype == 'TT_Q':
            tradelabel = "Total Trades Quantity"

        table = tabledata/self.table1.loc[tradelabel]*100

        table.columns = [c+f"_% Share of overall {tradetype}" for c in table.columns]

        # denote symbol
        if self.needsymbol == True:
            # drop NA for share
            table = table.dropna(axis='columns', how='all')
            table = ex.denotesymbol(table, datatype='share')

        return table

    def export_to_excel(self, numberofdecimal, periodsdata=False, alldata=False):
        #saving to excel files
        original_path = os.getcwd()
        folder_path = "Industry"
        file_path = folder_path+"/"+self.periods[0]+"-"+self.periods[-1]+"/"+self.currency+"/"+self.money
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        os.chdir(file_path)
        # include data
        filename = f"{self.group_no}_{self.name}"

        self.excelfile_name = f'Analysis_{filename}_{self.currency}_{self.money}_{self.periods[0]}-{self.periods[-1]}.xlsx'
        writer4 = pd.ExcelWriter(self.excelfile_name)

        if alldata == True:
            writer1 = pd.ExcelWriter(f'df1_{filename}_{self.currency}_{self.money}_{self.periods[0]}-{self.periods[-1]}.xlsx')
            writer2 = pd.ExcelWriter(f'df2_{filename}_{self.currency}_{self.money}_{self.periods[0]}-{self.periods[-1]}.xlsx')
            writer3 = pd.ExcelWriter(f'df3_{filename}_{self.currency}_{self.money}_{self.periods[0]}-{self.periods[-1]}.xlsx')
            allwriters = [writer1,writer2,writer3,writer4]

        if periodsdata == True:
                for p in self.periods:
                    self.df1_allperiods[p].to_excel(writer1, p)
                    self.df2_allperiods[p].to_excel(writer2, p)
                    self.df3_allperiods[p].to_excel(writer3, p)

        self.table1_result.to_excel(writer4, 'table1', float_format=f"%.{numberofdecimal}f", startrow=1)
        # bycty
        self.DXbycty.to_excel(writer4, 'DXbycty', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TXbycty.to_excel(writer4, 'TXbycty', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyctyasDestination.to_excel(writer4, 'RXbyctyasDestination', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyctyasOrigin.to_excel(writer4, 'RXbyctyasOrigin',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyctyasConsignment.to_excel(writer4, 'IMbyctyasConsignment',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyctyasOrigin.to_excel(writer4, 'IMbyctyasOrigin',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TTbycty.to_excel(writer4, 'TTbycty', float_format=f"%.{numberofdecimal}f", startrow=1)


        # by quantity
        self.DXbycty_Q.to_excel(writer4, 'DXbycty_Q', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TXbycty_Q.to_excel(writer4, 'TXbycty_Q', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyctyasDestination_Q.to_excel(writer4, 'RXbyctyasDestination_Q', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyctyasOrigin_Q.to_excel(writer4, 'RXbyctyasOrigin_Q',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyctyasConsignment_Q.to_excel(writer4, 'IMbyctyasConsignment_Q',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyctyasOrigin_Q.to_excel(writer4, 'IMbyctyasOrigin_Q',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TTbycty_Q.to_excel(writer4, 'TTbycty_Q', float_format=f"%.{numberofdecimal}f", startrow=1)

        # by region
        # EU
        self.DXbyEU.to_excel(writer4, 'DXbyEU', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TXbyEU.to_excel(writer4, 'TXbyEU', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyEUasDestination.to_excel(writer4, 'RXbyEUasDestination', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyEUasOrigin.to_excel(writer4, 'RXbyEUasOrigin',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyEUasConsignment.to_excel(writer4, 'IMbyEUasConsignment',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyEUasOrigin.to_excel(writer4, 'IMbyEUasOrigin',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TTbyEU.to_excel(writer4, 'TTbyEU', float_format=f"%.{numberofdecimal}f", startrow=1)

        # Asean
        self.DXbyAsean.to_excel(writer4, 'DXbyAsean',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TXbyAsean.to_excel(writer4, 'TXbyAsean',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyAseanasDestination.to_excel(writer4, 'RXbyAseanasDestination', float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyAseanasOrigin.to_excel(writer4, 'RXbyAseanasOrigin',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyAseanasConsignment.to_excel(writer4, 'IMbyAseanasConsignment',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyAseanasOrigin.to_excel(writer4, 'IMbyAseanasOrigin',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TTbyAsean.to_excel(writer4, 'TTbyAsean', float_format=f"%.{numberofdecimal}f", startrow=1)


        # Asia
        self.DXbyAsia.to_excel(writer4, 'DXbyAsia',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TXbyAsia.to_excel(writer4, 'TXbyAsia',float_format=f"%.{numberofdecimal}f", startrow=1)
        # Europe
        self.IMbyEuropeasOrigin.to_excel(writer4, 'IMbyEuropeasOrigin',float_format=f"%.{numberofdecimal}f", startrow=1)

        # by product
        self.DXbyproduct.to_excel(writer4, 'DXbyproduct',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.TXbyproduct.to_excel(writer4, 'TXbyproduct',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.RXbyproduct.to_excel(writer4, 'RXbyproduct',float_format=f"%.{numberofdecimal}f", startrow=1)
        self.IMbyproduct.to_excel(writer4, 'IMbyproduct',float_format=f"%.{numberofdecimal}f", startrow=1)

        writer4.save()

        # autofit the columns in the excel file
        try:
            #print('file test: ', os.getcwd())
            ex.addtitle(self.excelfile_name, self.name)
            ex.addcomma_align(self.excelfile_name)
            ex.autofit_wrap_industry(self.excelfile_name)
            ex.freezepane(self.excelfile_name)
            ex.addsource(self.excelfile_name)

            if alldata == True:
                [w.save() for w in allwriters]

        finally:
            os.chdir(original_path)

    def __str__(self):
        return "this class is %s, periods from %s-%s" % (self.name, self.periods[0], self.periods[-1])

# export data for each industry
if __name__ == '__main__':
    # calculate time spent
    start_time = time.time()

    dollar = {'HKD':1, 'USD':7.8}
    unit = {'THOUSAND':10**3, 'MILLION':10**6,'BILLION':10**9}
    currency = 'USD'
    money = 'MILLION'

    # set number of decimals
    numberofdecimal=3

    print(f"********* {currency} {money}")
    # input periods for the report
    startyear, endytd = 2016, 201908

    # decide to denote symbol or not
    needsymbol = True
    # acquire hsccit data from startyear to endyear and combine them into dataframe
    # acquire hscoit data from startyear to endyear and combine them into dataframe
    # acquire hscoccit data from startyear to endyear and combine them into dataframe
    df1,df2,df3 = [mergedf(startyear, endytd, type) for type in ["hsccit","hscoit","hscoccit"]]

    # sort the periods for functions use later
    periods = sorted(set(df1.reporting_time))

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
    #print("here")
    #print(industrycode)
    # add overall
    industrycode.update({'Overall':{'industry_name':'Overall', 'code_type':'Overall','codes':'Overall'}})

    # for loop to implement class for each industry
    for k, v in industrycode.items():
        #if k!='Overall':continue
        print("key: ",k)

        group_no = k
        name = v['industry_name']
        codetype =  'Overall' if k == 'Overall' else v['code_type'][0]
        product_code = v['codes']

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
            commodity_digit = [commodity_digit1,commodity_digit2,commodity_digit3]
            # sorted is prefered than sort
            commodity_digit = sorted(set(commodity_digit))

        elif len(len_pcode)<2:
            if k == 'Overall':
                commodity_digit = 'SITC-3'
                table1_all_periods = table1
                table2_all_periods = table2
                table3_all_periods = table3

            else:
                commodity_digit = codetype +'-'+ str(len_pcode[0])
                table1_all_periods = table1[table1[commodity_digit].isin(product_code)]
                table2_all_periods = table2[table2[commodity_digit].isin(product_code)]
                table3_all_periods = table3[table3[commodity_digit].isin(product_code)]
                #commodity_digit=commodity_digit
        print(commodity_digit)

        cols1 = ['DX','RX','TX','IM','TT','DX_Q','RX_Q','TX_Q','IM_Q','TT_Q','HS-2','HS-4','HS-6','HS-8','SITC-1','SITC-2','SITC-3','SITC-4','SITC-5','f1','f3_consignment','cty_name_destination','reporting_time']
        cols2 = ['IMbyO', 'IMbyO_Q','HS-2','HS-4','HS-6','HS-8','SITC-1','SITC-2','SITC-3','SITC-4','SITC-5','f1','f3_origin','cty_name_origin','reporting_time']
        cols3 = ['RXbyO','RXbyO_Q','HS-2','HS-4','HS-6','HS-8','SITC-1','SITC-2','SITC-3','SITC-4','SITC-5','f1','f3_origin','f4_destination','cty_name_origin','cty_name_destination','reporting_time']

        # implement class
        dataset = [table1_all_periods[cols1],table2_all_periods[cols2],table3_all_periods[cols3]]

        industrycode[k]['class']=Industry(group_no,name,periods,dataset, currency, money, needsymbol)

        #print no. of industry instance
        print("Industry no.: %d " % Industry.noofindustry)

        industrycode[k]['class'].run_df_table()
        df1_all_periods_table = industrycode[k]['class'].run_df_separate()

        #table 1 result
        industrycode[k]['class'].df_table1()

        #table 2 DX, TX, RX, IM, TX, TT by country
        industrycode[k]['class'].DXbycty=industrycode[k]['class'].analysis_bycty("DX")
        industrycode[k]['class'].TXbycty=industrycode[k]['class'].analysis_bycty("TX")
        industrycode[k]['class'].RXbyctyasDestination=industrycode[k]['class'].analysis_bycty("RX")
        industrycode[k]['class'].RXbyctyasOrigin=industrycode[k]['class'].analysis_bycty("RXbyO")
        industrycode[k]['class'].IMbyctyasConsignment=industrycode[k]['class'].analysis_bycty("IM")
        industrycode[k]['class'].IMbyctyasOrigin=industrycode[k]['class'].analysis_bycty("IMbyO")
        industrycode[k]['class'].TTbycty=industrycode[k]['class'].analysis_bycty("TT")

        # quantity
        industrycode[k]['class'].DXbycty_Q=industrycode[k]['class'].analysis_bycty("DX_Q")
        industrycode[k]['class'].TXbycty_Q=industrycode[k]['class'].analysis_bycty("TX_Q")
        industrycode[k]['class'].RXbyctyasDestination_Q=industrycode[k]['class'].analysis_bycty("RX_Q")
        industrycode[k]['class'].RXbyctyasOrigin_Q=industrycode[k]['class'].analysis_bycty("RXbyO_Q")
        industrycode[k]['class'].IMbyctyasConsignment_Q=industrycode[k]['class'].analysis_bycty("IM_Q")
        industrycode[k]['class'].IMbyctyasOrigin_Q=industrycode[k]['class'].analysis_bycty("IMbyO_Q")
        industrycode[k]['class'].TTbycty_Q=industrycode[k]['class'].analysis_bycty("TT_Q")

        #table 3 by region
        # EU
        industrycode[k]['class'].DXbyEU=industrycode[k]['class'].analysis_byregions('DX','EU')
        industrycode[k]['class'].TXbyEU=industrycode[k]['class'].analysis_byregions('TX','EU')
        industrycode[k]['class'].RXbyEUasDestination=industrycode[k]['class'].analysis_byregions("RX",'EU')
        industrycode[k]['class'].RXbyEUasOrigin=industrycode[k]['class'].analysis_byregions("RXbyO",'EU')
        industrycode[k]['class'].IMbyEUasConsignment=industrycode[k]['class'].analysis_byregions("IM",'EU')
        industrycode[k]['class'].IMbyEUasOrigin=industrycode[k]['class'].analysis_byregions("IMbyO",'EU')
        industrycode[k]['class'].TTbyEU=industrycode[k]['class'].analysis_byregions("TT",'EU')

        # Asean
        industrycode[k]['class'].DXbyAsean=industrycode[k]['class'].analysis_byregions('DX','Asean')
        industrycode[k]['class'].TXbyAsean=industrycode[k]['class'].analysis_byregions('TX','Asean')
        industrycode[k]['class'].RXbyAseanasDestination=industrycode[k]['class'].analysis_byregions("RX",'Asean')
        industrycode[k]['class'].RXbyAseanasOrigin=industrycode[k]['class'].analysis_byregions("RXbyO",'Asean')
        industrycode[k]['class'].IMbyAseanasConsignment=industrycode[k]['class'].analysis_byregions("IM",'Asean')
        industrycode[k]['class'].IMbyAseanasOrigin=industrycode[k]['class'].analysis_byregions("IMbyO",'Asean')
        industrycode[k]['class'].TTbyAsean=industrycode[k]['class'].analysis_byregions("TT",'Asean')

        # Asia
        industrycode[k]['class'].DXbyAsia=industrycode[k]['class'].analysis_byregions('DX','Asia')
        industrycode[k]['class'].TXbyAsia=industrycode[k]['class'].analysis_byregions('TX','Asia')
        # Europe
        industrycode[k]['class'].IMbyEuropeasOrigin=industrycode[k]['class'].analysis_byregions('IMbyO','Europe')

        #table 4 TX by product
        if isinstance(commodity_digit,str):
            industrycode[k]['class'].DXbyproduct=industrycode[k]['class'].analysis_byproducts('DX',commodity_digit)
            industrycode[k]['class'].TXbyproduct=industrycode[k]['class'].analysis_byproducts('TX',commodity_digit)
            industrycode[k]['class'].RXbyproduct=industrycode[k]['class'].analysis_byproducts('RX',commodity_digit)
            industrycode[k]['class'].IMbyproduct=industrycode[k]['class'].analysis_byproducts('IM',commodity_digit)

        if isinstance(commodity_digit,list):
            industrycode[k]['class'].DXbyproduct=industrycode[k]['class'].analysis_byproducts('DX',*commodity_digit)
            industrycode[k]['class'].TXbyproduct=industrycode[k]['class'].analysis_byproducts('TX',*commodity_digit)
            industrycode[k]['class'].RXbyproduct=industrycode[k]['class'].analysis_byproducts('RX',*commodity_digit)
            industrycode[k]['class'].IMbyproduct=industrycode[k]['class'].analysis_byproducts('IM',*commodity_digit)

        #set decimal space
        #saving to excel files
        outputexcel=True
        if outputexcel == True:
            industrycode[k]['class'].export_to_excel(numberofdecimal=numberofdecimal, periodsdata=False, alldata=False)

        #print(f'unsucessful for: {group_no} {name}')

#calculate time spent
elapsed_time = round(time.time() - start_time, 2)
print("time used: ", elapsed_time, " seconds")
