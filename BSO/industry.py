"""
Define functions to get product codes for each industry
The code file is imported from the folder "./metadata"
"""
import pprint
from xlrd import open_workbook
from collections import OrderedDict

industry_file = './metadata/industry.xlsx'

# return industry dictionery, industry group code as keys, product codes as values accordingly
def get_industry_code(excel_file=industry_file):

    book = open_workbook(excel_file)
    industrygroup = book.sheet_by_name('industrygroup')
    product_codes = book.sheet_by_name('product_codes')

    industry = OrderedDict()
    # get industry name
    for row in range(industrygroup.nrows):
        if row == 0 or row==1:
            continue
        industry[str(industrygroup.cell(row,0).value)]={}
        industry[str(industrygroup.cell(row,0).value)]['industry_name'] = str(industrygroup.cell(row,1).value)

    # get industry codes
    for k in industry.keys():
        industry[k]['code_type']=[]
        industry[k]['codes']=[]

    for row in range(product_codes.nrows):
        if row == 0:
            continue

        k = str(product_codes.cell(row,0).value)
        type = str(product_codes.cell(row,1).value)
        if k in industry.keys():
            if type not in industry[k]['code_type']:
                industry[k]['code_type'].append(type)
            if k not in industry[k]['codes']:
                industry[k]['codes'].append(str(product_codes.cell(row,2).value))

    return industry

if __name__ == '__main__':
    industry_dict = get_industry_code()
    for k, v in industry_dict.items():
        print("Industry group code: %s" % k)
        print("Industry group name: %s" % v['industry_name'])
        print("Industry group product codes' type: %s" % v['code_type'])
        pprint.pprint("codes %s" % (v['codes']))
        print("\n")
