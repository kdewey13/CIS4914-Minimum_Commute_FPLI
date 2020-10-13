import pandas
import xlrd

full_data = pandas.read_excel('MSID_active_schools_10-13-20.xlsx')  # read the excel file for processing

"""create a dictionary of all the data we care about, the key will be the district name and the values will be """

data_of_interest ={}

for row in full_data:
    wait =1

wait = 3



