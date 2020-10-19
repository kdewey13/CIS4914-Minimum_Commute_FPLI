import pandas
import sqlite3
import xlrd

"""This is a script that was used to pre-process the MSID data taken from FL DOE and output a csv of the format 
expected by the minimum commute calculator function. It can be used again if desired/updated if the fields or
data codes have been changed."""

"""FL school data taken from FDoE->Accessibility->Master School ID Database. 
The data used in this script was downloaded using the 'all schools, all fields' option.
IMPORTANT: make sure to re-save the file as xlsx, not xls, the file formatting is messed up in the original download."""


# read in the excel file for processing
full_data = pandas.read_excel('MSID_all_schools.xlsx')

# remove all but the columns of interest
full_data = full_data[['TYPE', 'ACTIVITY_CODE', 'DISTRICT_NAME', 'SCHOOL_NAME_LONG', 'GRADE_CODE',
                       'PHYSICAL_ADDRESS', 'PHYSICAL_CITY', 'PHYSICAL_STATE', 'PHYSICAL_ZIP',
                       'FEDERAL_DIST_NO', 'FEDERAL_SCHL_NO', 'SCHL_FUNC_SETTING', 'LATITUDE', 'LONGITUDE']]
# remove all but active schools
full_data = full_data[full_data.ACTIVITY_CODE == 'A']
# remove all schools with types 'not assigned', 'adult', or 'other'
full_data = full_data[full_data['TYPE'].isin([1, 2, 3, 4])]
# remove all specialized schools (adult, DJJ, home, virtual, etc)
full_data = full_data[full_data.SCHL_FUNC_SETTING == 'Z']

# replace numerical type codes with english words
full_data.loc[full_data.TYPE == 1, 'TYPE'] = 'elementary'
full_data.loc[full_data.TYPE == 2, 'TYPE'] = 'middle'
full_data.loc[full_data.TYPE == 3, 'TYPE'] = 'high'
full_data.loc[full_data.TYPE == 4, 'TYPE'] = 'combination'

# create dictionaries for the combinations and their corresponding grade code values
levels = {
    'elementary, middle': [109, 27, ],
    'elementary, high': [],
    'elementary, middle, high': [],
    'middle, high': [],
}

wait = 3



