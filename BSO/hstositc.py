"""
There are two versions(SITC, HS) for classifying trade products
The fuctions are for geting the product codes.
The files for conversion for SITC to HS, product code, are imported from the folder "./metadata"
"""
from xlrd import open_workbook

# define the path for sitc2hs, product_code
sitc2hs_file = './metadata/sitc2hs.xlsx'
product_code_file = './metadata/product_code.xlsx'

# make a dictionery(HS codes as keys, SITC code as values)
def get_hstositc_code(excelfile=sitc2hs_file, sheet="sitc2hs"):
    book = open_workbook(excelfile)
    sheet = book.sheet_by_name(sheet)
    hstositc = {}
    for row in range(sheet.nrows):
        if row == 0:
            continue
        hstositc[str(int(sheet.cell(row,1).value)).rjust(8,'0')] = str(int(sheet.cell(row,0).value)).rjust(5,'0')
    return hstositc

def get_sitc_name(excelfile=product_code_file, sheet_name="sitc3" ):
    """Attribute for sheet: 1)"sitc3", 2)"sitc5" """
    book = open_workbook(excelfile)
    sheet = book.sheet_by_name(sheet_name)
    sitc = {}
    for row in range(sheet.nrows):
        if row == 0 or row == 1:
            continue
        if sheet_name=="sitc3":
            sitc[str(int(sheet.cell(row,1).value)).rjust(3,'0')] = str(sheet.cell(row,2).value)
        elif sheet_name=="sitc5":
            sitc[str(int(sheet.cell(row,0).value)).rjust(3,'0')] = str(sheet.cell(row,1).value)
    return sitc


if __name__ == '__main__':
    adict = get_hstositc_code()
    print("1) example for conversion")
    print("conversion of HS 01011000: SITC %s" % adict['01011000'])
    print("conversion of HS 85285291: SITC %s" % adict['85285291'])

    print("2) example for geting SITC-3 product name")
    b = get_sitc3_name()
    print("SITC-3-digit '001' product name: %s" %b['001'])
