"""
Define functions to get geography codes for individual country, region, area.
The code file is imported from the folder "./metadata"
"""
from xlrd import open_workbook

geo_file = './metadata/geography.xlsx'

# get the codes for individual country, default is from sheet("country")
def get_geography_code(sheet="country",bk=geo_file):
    book = open_workbook(bk)
    sheet = book.sheet_by_name(sheet)

    geography = {}
    for row in range(sheet.nrows):
        if row == 0:
            continue
        geography[int(sheet.cell(row,0).value)] = sheet.cell(row,1).value
    return geography

# get the geography code dictionery, default is from sheet("regcnty")
def get_geography_regcnty_code(region_key, sheet="regcnty",bk=geo_file):
    book = open_workbook(bk)
    sheet = book.sheet_by_name(sheet)

    geography = {}
    for k in region_key:
        geography[k]=[]

        for row in range(sheet.nrows):
            if row == 0:
                continue

            if int(sheet.cell(row,0).value)==k:
                geography[k].append(int(sheet.cell(row,1).value))

    return geography

if __name__ == '__main__':
    a = get_geography_code(sheet="region")
    for k, v in a.items():
        print(k, v)
    print('\n')

    b = get_geography_regcnty_code(a.keys(),sheet="regcnty")
    for k, v in b.items():
        print(k, v)
    print('\n')

    # get country codes for Asean
    c = b[300]
    print("country codes for Asean: %s" % c)
