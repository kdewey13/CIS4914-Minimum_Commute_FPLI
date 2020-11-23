import pandas
import xlrd
import xlsxwriter
import requests
import os


def download_data():
    """Downloads the FL MSID data from FDoE->Accessibility->Master School ID Database->'all schools, all fields'
    and saves it into an excel file, which it sends to the preprocessing function"""
    response = requests.post(url="http://doeweb-prd.doe.state.fl.us/EDS/MasterSchoolID/Downloads/All_schools.cfm?"
                                 "CFID=14378813&CFTOKEN=7094b0d8c1fa469b-4367B8AA-5056-AAE8-B05DA3D0074B0496",
                             headers={'Host': 'doeweb-prd.doe.state.fl.us', 'Connection': 'keep-alive',
                                      'Content-Length': '0', 'Cache-Control': 'max-age=0',
                                      'Upgrade-Insecure-Requests': '1', 'Origin': 'http://doeweb-prd.doe.state.fl.us',
                                      'Content-Type': 'application/x-www-form-urlencoded',
                                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                    'Chrome/86.0.4240.198 Safari/537.36',
                                      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,'
                                                'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;'
                                                'q=0.9',
                                      'Referer': 'http://doeweb-prd.doe.state.fl.us/EDS/MasterSchoolID/Downloads.cfm?'
                                                 'CFID=14378813&CFTOKEN=7094b0d8c1fa469b-4367B8AA-5056-AAE8-'
                                                 'B05DA3D0074B0496', 'Accept-Encoding': 'gzip, deflate',
                                      'Accept-Language': 'en-US,en;q=0.9'})
    # write the data to a file
    with open('response.txt', 'wb') as f:
        f.write(response.content)
    # convert the text file to an xlsx file
    pandas.read_csv('response.txt', sep='\t').to_excel('MSID_data.xlsx', index=False)
    # delete the unneeded file
    if os.path.exists('response.txt'):
        os.remove('response.txt')
    # send to pre-process
    preprocess_fl_msid_data(data_excel_file='MSID_data.xlsx')


def preprocess_fl_msid_data(data_excel_file=None):
    """This function will pre-process the MSID data taken from FL DOE and output a csv of the format expected by the
    minimum commute calculator function.
    Will need to be updated if the fields or data codes have been changed.
    Taken from FDoE->Accessibility->Master School ID Database.
    The data used in this script was downloaded using the 'all schools, all fields' option.

    IMPORTANT: if the download is made manually (not using download_data()) re-save the download (as xlsx).
    As of 2020, the file formatting is messed up in the download and doesn't permit opening as this function attempts."""

    # read in the excel file for processing, creates a data frame of the data
    full_data = pandas.read_excel(data_excel_file)

    # select only the columns of interest from the data frame
    full_data = full_data[['TYPE', 'ACTIVITY_CODE', 'DISTRICT_NAME', 'SCHOOL_NAME_LONG', 'GRADE_CODE',
                           'PHYSICAL_ADDRESS', 'PHYSICAL_CITY', 'PHYSICAL_STATE', 'PHYSICAL_ZIP',
                           'SCHL_FUNC_SETTING', 'LATITUDE', 'LONGITUDE', 'CHARTER_SCHL_STAT']]

    # remove all but active schools
    # this syntax filters full_data, only keeping rows w/ ACTIVITY_CODE equal to A
    full_data = full_data[full_data.ACTIVITY_CODE == 'A']
    # remove all schools with types 'not assigned', 'adult', or 'other'
    # (keep only elem (TYPE code=1), middle (2), high (3), and combo (4))
    full_data = full_data[full_data['TYPE'].isin([1, 2, 3, 4])]
    # remove all specialized schools (adult, DJJ, home, virtual, etc), keep only N/A
    full_data = full_data[full_data.SCHL_FUNC_SETTING == 'Z']

    # add a column to store the grade level
    full_data['level'] = 'bunny123'  # used weird string for easy searching to make sure every line got replaced [works]

    # Create dictionaries for the grade level combinations and their corresponding grade code values. The combination
    # that each code corresponds to were determined manually using the grade code Appendix in the docs for the
    # FL MSID data (saved in project as PDF, available on FLDOE).
    levels = {
        'em': [109, 27, 120, 22, 25, 23, 21, 40, 101, 106, 102, 45, 57, 53, 118],
        'eh': [],
        'emh': [29, 14, 55, 26, 100, 47, 28, 24, 105, 108, 113, 114, 107, 77],
        'mh': [68, 76, 103, 62, 110],
    }

    # fill in the grade level column with the appropriate value
    # if type is 1, 2, or 3, we know directly which levels are served
    full_data.loc[full_data.TYPE == 1, 'level'] = 'elementary'
    full_data.loc[full_data.TYPE == 2, 'level'] = 'middle'
    full_data.loc[full_data.TYPE == 3, 'level'] = 'high'
    # everything that remains is a combo and must be determined using the grade code
    full_data.loc[full_data['GRADE_CODE'].isin(levels['em']), 'level'] = 'elementary, middle'
    full_data.loc[full_data['GRADE_CODE'].isin(levels['eh']), 'level'] = 'elementary, high'
    full_data.loc[full_data['GRADE_CODE'].isin(levels['emh']), 'level'] = 'elementary, middle, high'
    full_data.loc[full_data['GRADE_CODE'].isin(levels['mh']), 'level'] = 'middle, high'

    # translate charter school status codes into booleans, don't switch the order this goes in
    full_data.loc[full_data.CHARTER_SCHL_STAT != 'Z', 'CHARTER_SCHL_STAT'] = True
    full_data.loc[full_data.CHARTER_SCHL_STAT == 'Z', 'CHARTER_SCHL_STAT'] = False

    # remove all but the columns of interest now that filtering is done and rename the columns to simpler titles
    full_data = full_data[['DISTRICT_NAME', 'SCHOOL_NAME_LONG', 'level', 'PHYSICAL_ADDRESS', 'PHYSICAL_CITY',
                           'PHYSICAL_STATE', 'PHYSICAL_ZIP', 'LATITUDE', 'LONGITUDE', 'CHARTER_SCHL_STAT']]
    full_data = full_data.rename(columns={'DISTRICT_NAME': 'district_name', 'SCHOOL_NAME_LONG': 'school_name',
                                          'PHYSICAL_ADDRESS': 'street_address', 'PHYSICAL_CITY': 'city',
                                          'PHYSICAL_STATE': 'state', 'PHYSICAL_ZIP': 'zip',
                                          'LATITUDE': 'latitude', 'LONGITUDE': 'longitude',
                                          'CHARTER_SCHL_STAT': 'charter'})
    # save the data to a csv file for input into the minimum commute calculator
    full_data.to_csv(path_or_buf='input_data.csv', index=False)
