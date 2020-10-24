import geopy.distance  # python module with function for accurate straight line distance calculation
import pandas
import sqlite3  # in-app database to make querying for minumums easy
from sqlite3 import Error

"""---------------------------------------------------------------------------------"""
"""For this program to work, you must input a file in the same format as the example"""
"""---------------------------------------------------------------------------------"""

# Create an in-memory database; only exists when program is running
connection = sqlite3.connect(':memory:')

# Read in the input file and put it in the db
pandas.read_csv('input_data.csv').to_sql(name='school_info', con=connection, if_exists='replace', index=True,
                                         index_label='id')

# create a cursor to the
if connection is not None:
    try:
        cursor = connection.cursor()
        # create a list of all the school data information
        school_list = list(cursor.execute('Select * FROM school_info').fetchall())
    except Error as e:
        print(e)

# get the indices that the values of interest are at (if the input file is in the correct order this should
# be the same each time, but this doesn't hurt anything to check)
id_index, lat_index, long_index, level_index, district_index = '', '', '', '', ''
for index in range(0, len(cursor.description)):
    if cursor.description[index][0] == 'id':
        id_index = index
    elif cursor.description[index][0] == 'latitude':
        lat_index = index
    elif cursor.description[index][0] == 'longitude':
        long_index = index
    elif cursor.description[index][0] == 'level':
        level_index = index
    elif cursor.description[index][0] == 'district_name':
        district_index = index

# create a table to store the pairs of schools that are less than 100 miles apart
if connection is not None:
    try:
        cursor = connection.cursor()
        # create a list of all the school data information
        cursor.execute("CREATE TABLE IF NOT EXISTS straight_line_pairs("
                       "school_1_id INTEGER NOT NULL, "
                       "school_2_id INTEGER NOT NULL, "
                       "distance_between INTEGER NOT NULL, "
                       "PRIMARY KEY(school_1_id, school_2_id), "
                       "FOREIGN KEY (school_1_id) REFERENCES school_info (id), "
                       "FOREIGN KEY (school_2_id) REFERENCES school_info (id));")
        connection.commit()
    except Error as e:
        print(e)

# find the straight line distance for each school pair and save to table
for school in school_list:  # for each school in the list
    for other_school in school_list:  # compare it to every othr school
        if school != other_school:  # don't compare to self:
            # if they are the same level (or in case of combos, one of the levels is the same) find the distance
            if (school[level_index] in other_school[level_index] or other_school[level_index] in school[level_index]) \
                    and school[district_index] != other_school[district_index]:
                distance_between = geopy.distance.vincenty((school[lat_index], school[long_index]),
                                                           (other_school[lat_index], other_school[long_index])).miles
                #  if distance between is less than the threshold, save the pair
                if distance_between <= 100:
                    if connection is not None:
                        try:
                            cursor = connection.cursor()
                            # save the record to the pairs table
                            cursor.execute("INSERT INTO straight_line_pairs(school_1_id, school_2_id, "
                                           "distance_between) VALUES ({0},{1},{2})".
                                           format(school[id_index], other_school[id_index], distance_between))
                            connection.commit()
                        except Error as e:
                            print(e)

    # now that we have compared to every school, remove it from the list to avoid duplicate comparisons
    school_list.remove(school)
if connection is not None:
    try:
        cursor = connection.cursor()
        # save the record to the pairs table
        all_pairs = cursor.execute("SELECT * FROM straight_line_pairs")
        wait = 3
    except Error as e:
        print(e)
