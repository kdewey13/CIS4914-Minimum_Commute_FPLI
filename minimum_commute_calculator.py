import geopy.distance  # python module with function for accurate straight line distance calculation
import pandas
import sqlite3  # in-app database to make querying for minumums easy
from sqlite3 import Error
import datetime
import config

"""---------------------------------------------------------------------------------"""
"""For this program to work, you must input a file in the same format as the example"""
"""---------------------------------------------------------------------------------"""

"""PARAMETERS: REMOVE WHEN FUNCTION"""
max_radius_to_consider = 50
circuituity_factor = 2
slow_commute_speed = 30
fast_commute_speed = 60
number_of_minimums_per_disct_pair = 3
school_levels = ['elementary', 'middle', 'high']
api_key = config.distance_key
path_to_input_file = 'input_data.csv'
path_to_distance_pairs_file = 'distance_pairs.csv'
path_to_output_file = ''
"""PARAMETERS: REMOVE WHEN FUNCTION"""


print(datetime.datetime.now())
# Create an in-memory database; only exists when program is running
connection = sqlite3.connect(':memory:')

# Read in the input file and put it in the db
pandas.read_csv(path_to_input_file).to_sql(name='school_info', con=connection, if_exists='replace', index=True,
                                           index_label='id')

# create a cursor to the school_info table we just created
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
        connection.cursor().execute("CREATE TABLE IF NOT EXISTS straight_line_pairs("
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
                if distance_between <= max_radius_to_consider:
                    if connection is not None:
                        try:
                            cursor = connection.cursor()
                            # save the record to the pairs table, estimate the min and max commutes
                            cursor.execute("INSERT INTO straight_line_pairs(school_1_id, school_2_id, "
                                           "distance_between) VALUES ({0},{1},{2})".
                                           format(school[id_index], other_school[id_index], distance_between))
                            connection.commit()
                        except Error as e:
                            print(e)

    # now that we have compared to every school, remove it from the list to avoid duplicate comparisons
    school_list.remove(school)
print(datetime.datetime.now())
if connection is not None:
    try:
        # save the distance pairs for interim examining
        pandas.DataFrame(pandas.read_sql_query("SELECT school_1_id, school_2_id, School_1_Name, "
                                               "School_1_District, school_name as 'School_2_Name', "
                                               "district_name as 'School_2_District', distance_between "
                                               "FROM (SELECT school_1_id, school_2_id, school_name as 'School_1_Name', "
                                               "district_name as 'School_1_District', distance_between "
                                               "FROM straight_line_pairs JOIN school_info on school_1_id = id) "
                                               "JOIN school_info on school_2_id = id", connection)).\
            to_csv(path_to_distance_pairs_file)
        wait = 3
    except Error as e:
        print(e)

# get a list of the districts
if connection is not None:
    try:
        districts = list(connection.cursor().execute("SELECT DISTINCT district_name FROM school_info").fetchall())
    except Error as e:
        print(e)

# for each district pair and each school level, narrow the list of candidates for min commute based on following:
# find the minimum distances (as many as the desired number of minimums), take the largest of these
# the maximum estimate for the commute time for this distance is: (circuituity_factor/slow_commute_speed)*distance
# considering that the minimum estimated commute for any distance is: distance/fast_commute_speed
# it is not worth considering distances where their minimum estimated commute is
# longer than the maximum estimated commute thus, we wish to exclude distances longer than:
# largest_min_dist*fast_commute_speed*(circuituity_factor/slow_commute_speed) from the accurate commute analysis

for district in districts:
    for other_district in districts:
        if district != other_district:
            for level in school_level:
