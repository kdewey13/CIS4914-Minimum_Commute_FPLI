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

# add a column for the grade level
full_data['level'] = 'bunny123'  # used for easy searching to make sure every line got replaced

# create dictionaries for the grade level combinations and their corresponding grade code values
levels = {
    'em': [109, 27, 120, 22, 25, 23, 21, 40, 101, 106, 102, 45, 57, 53, 118],
    'eh': [],
    'emh': [29, 14, 55, 26, 100, 47, 28, 24, 105, 108, 113, 114, 107, 77],
    'mh': [68, 76, 103, 62, 110],
}
# fill in the grade level column with the appropriate value
full_data.loc[full_data.TYPE == 1, 'level'] = 'elementary'
full_data.loc[full_data.TYPE == 2, 'level'] = 'middle'
full_data.loc[full_data.TYPE == 3, 'level'] = 'high'
full_data.loc[full_data['GRADE_CODE'].isin(levels['em']), 'level'] = 'elementary, middle'
full_data.loc[full_data['GRADE_CODE'].isin(levels['eh']), 'level'] = 'elementary, high'
full_data.loc[full_data['GRADE_CODE'].isin(levels['emh']), 'level'] = 'elementary, middle, high'
full_data.loc[full_data['GRADE_CODE'].isin(levels['mh']), 'level'] = 'middle, high'

# remove all but the columns of interest now that filtering is done and rename the columns to simpler titles
full_data = full_data[['DISTRICT_NAME', 'SCHOOL_NAME_LONG', 'level', 'PHYSICAL_ADDRESS', 'PHYSICAL_CITY',
                       'PHYSICAL_STATE', 'PHYSICAL_ZIP', 'FEDERAL_DIST_NO', 'FEDERAL_SCHL_NO', 'LATITUDE', 'LONGITUDE']]
full_data = full_data.rename(columns={'DISTRICT_NAME': 'district_name', 'SCHOOL_NAME_LONG': 'school_name',
                                      'PHYSICAL_ADDRESS': 'street_address', 'PHYSICAL_CITY': 'city',
                                      'PHYSICAL_STATE': 'state', 'PHYSICAL_ZIP': 'zip', 'FEDERAL_DIST_NO':
                                          'federal_district_number', 'FEDERAL_SCHL_NO': 'federal_school_number',
                                      'LATITUDE': 'latitude', 'LONGITUDE': 'longitude'})
# save the data to a csv file for input into the minimum commute calculator
full_data.to_csv(path_or_buf='input_data.csv', index=False)



