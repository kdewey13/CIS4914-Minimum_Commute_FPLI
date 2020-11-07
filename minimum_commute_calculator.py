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
elementary, middle, high = True, True, True
e_num_unique_schools, e_num_pairs_per_school, m_num_unique_schools, m_num_pairs_per_school, \
h_num_unique_schools, h_num_pairs_per_school = 3, 3, 2, 2, 2, 2
charter = True
api_key = config.distance_key
path_to_input_file = 'input_data.csv'
path_to_distance_pairs_file = 'distance_pairs.csv'
path_to_output_file = ''
"""PARAMETERS: REMOVE WHEN FUNCTION"""

print(datetime.datetime.now())
# get a dictionary of the desired school levels with level serving as the keys and the value as a list
# where the first value in the list is the number of unique schools to make pairs with per district and the
# second value is the number of school connections to make per school from the first value
# i.e. if elementary_pairs = 3 and elementary_schools = will yield 9 pairs of elementary
# schools per district, 3 schools, each paired with 3 schools in the other district
school_levels = {}
if elementary:
    school_levels['elementary'] = [e_num_unique_schools, e_num_pairs_per_school]
if middle:
    school_levels['middle'] = [m_num_unique_schools, m_num_pairs_per_school]
if high:
    school_levels['high'] = [h_num_unique_schools, h_num_pairs_per_school]

# Create an in-memory database; only exists when program is running
connection = sqlite3.connect(':memory:')

# Read in the input file
input_data = pandas.read_csv(path_to_input_file)

# if not considering all 3 school levels, remove the ones we don't want
if len(school_levels) != 3:
    filter_level_by = school_levels[:]
    if elementary:
        filter_level_by.extend(['elementary, middle', 'elementary, high'])
    if middle:
        filter_level_by.extend(['elementary, middle', 'middle, high'])
    if high:
        filter_level_by.extend(['elementary, high', 'middle, high'])
    input_data = input_data[input_data['level'].isin(filter_level_by)]

# if not considering charter schools, remove them
if not charter:
    input_data = input_data[input_data['charter'] == False]

# put all the info from the input file into the DB
input_data.to_sql(name='school_info', con=connection, if_exists='replace', index=True, index_label='id')

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

# create a table to store the pairs of schools that are less than max_radius_to_consider miles apart
if connection is not None:
    try:
        connection.cursor().execute("CREATE TABLE IF NOT EXISTS straight_line_pairs("
                                    "school_1_id INTEGER NOT NULL, "
                                    "school_2_id INTEGER NOT NULL, "
                                    "distance_between FLOAT NOT NULL, "
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
                            # save the record to the pairs table, estimate the min and max commutes
                            connection.cursor().execute("INSERT INTO straight_line_pairs(school_1_id, school_2_id, "
                                                        "distance_between) VALUES ({0},{1},{2})".
                                                        format(school[id_index], other_school[id_index],
                                                               distance_between))
                            connection.commit()
                        except Error as e:
                            print(e)
    # now that we have compared to every school, remove it from the list to avoid duplicate comparisons
    school_list.remove(school)

"""REMOVE THIS BIT WHEN DONE, JUST FOR CHECKING IN MEANTIME"""
print("Finished straightline distance {0}".format(datetime.datetime.now()))
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
"""REMOVE THIS BIT WHEN DONE, JUST FOR CHECKING IN MEANTIME"""

# get a list of the districts
if connection is not None:
    try:
        district_list = list(connection.cursor().execute("SELECT DISTINCT district_name FROM school_info").fetchall())
    except Error as e:
        print(e)

# For districts A and B find the closest pair in A-B. For the school a1 in A, find the next 2 closest schools in B.
# That is 3 connections, a1b11, a1b12, a1b13. Find the closest pair in A-B that does not involve a1,
# let a2 be the school in A in that pair. Find the closest 3 schools in B to a2. That is three connections,
# a2b21, a2b22, a2b23. Find the closest pair in A-B that does not involve a1 or a2, let a3 be the school in A in
# that pair. Find the closest 3 schools in B to a3. That is three connections, a3b31, a3b32, a3b33.

# create a table to store the pairs for which to gather actual commute estimates from API
if connection is not None:
    try:
        connection.cursor().execute("CREATE TABLE IF NOT EXISTS pairs_to_find_commute_between("
                                    "school_1_id INTEGER NOT NULL, "
                                    "school_2_id INTEGER NOT NULL, "
                                    "origin_district varchar NOT NULL, "
                                    "destination_district varchar NOT NULL, "
                                    "comparison_level varchar NOT NULL, "
                                    "PRIMARY KEY(school_1_id, school_2_id, comparison_level, origin_district), "
                                    "FOREIGN KEY (school_1_id) REFERENCES school_info (id), "
                                    "FOREIGN KEY (school_2_id) REFERENCES school_info (id));")
        connection.commit()
    except Error as e:
        print(e)

for district in district_list:
    for other_district in district_list:
        if district != other_district:  # don't compare to self
            for level in school_levels.keys():
                min_school_ids = []  # list of the school ids to find pairs for
                for index in range(0, school_levels[level][0]):  # loop the same # of times as unique schools we need
                    if connection is not None:
                        try:
                            exclusion_clause = ''
                            # get the school with nth minimum distance (where n = loop iteration count)
                            if len(min_school_ids) > 0:
                                exclusion_clause = 'AND school_1_id != {0}'.format(min_school_ids[0])
                                if len(min_school_ids) > 1:
                                    for id in range(1, len(min_school_ids)):
                                        exclusion_clause += ' AND school_1_id != {0}'.format(min_school_ids[id])
                            nth_min_dist_school = connection.cursor().execute(
                                "SELECT school_1_id FROM (SELECT school_1_id, school_2_id, distance_between "
                                "FROM straight_line_pairs JOIN school_info on school_1_id = id "
                                "WHERE level LIKE '%{0}%' AND district_name LIKE '{1}') "
                                "JOIN school_info on school_2_id = id WHERE level LIKE '%{0}%' "
                                "AND district_name LIKE '{2}' {3}} ORDER BY distance_between ASC Limit 1".
                                    format(level, district[0], other_district[0], exclusion_clause)).fetchone()
                            # if the above returned a school, add it to the list of schools.
                            if nth_min_dist_school is not None:
                                min_school_ids.append(nth_min_dist_school)
                        except Error as e:
                            print(e)
                # now that we have the needed number of schools, we find the desired number of minimal pairs for each
                # and save them in the table for API commute calculations
                for id in min_school_ids:
                    if connection is not None:
                        try:
                            # get the desired number of minimum pairs for this school to schools in the other county
                            connection.cursor().execute("INSERT INTO pairs_to_find_commute_between "
                                                        "SELECT school_1_id, school_2_id, origin_district, "
                                                        "district_name AS destination_level, '{0}' AS comparison_level "
                                                        "FROM (SELECT school_1_id, school_2_id, district_name "
                                                        "AS origin_district, distance_between FROM straight_line_pairs "
                                                        "JOIN school_info on school_1_id = id "
                                                        "WHERE level LIKE '%{0}%' AND district_name LIKE '{1}') "
                                                        "JOIN school_info on school_2_id = id WHERE level LIKE '%{0}%' "
                                                        "AND district_name LIKE '{2}' AND school_1_id == {3} "
                                                        "ORDER BY distance_between ASC Limit {4}".
                                                        format(level, district[0], other_district[0], id,
                                                               school_levels[level][1]))
                        except Error as e:
                            print(e)

"""REMOVE THIS BIT WHEN DONE, JUST FOR CHECKING IN MEANTIME"""
print("Finished pairs determination {0}".format(datetime.datetime.now()))
if connection is not None:
    try:
        # save the optimized distance pairs for interim examining
        pandas.DataFrame(pandas.read_sql_query("SELECT op.school_1_id, op.school_2_id, comparison_level, School_1_Name, "
                                               "School_1_District, School_2_Name, School_2_District, distance_between "
                                               "FROM (SELECT school_1_id, school_2_id, comparison_level, School_1_Name, "
                                               "School_1_District, school_name as 'School_2_Name', "
                                               "district_name as 'School_2_District' "
                                               "FROM (SELECT school_1_id, school_2_id, comparison_level, "
                                               "school_name as 'School_1_Name', "
                                               "district_name as 'School_1_District' "
                                               "FROM pairs_to_find_commute_between "
                                               "JOIN school_info on school_1_id = id) "
                                               "JOIN school_info on school_2_id = id) AS op "
                                               "JOIN straight_line_pairs AS slp ON "
                                               "op.school_1_id = slp.school_1_id AND "
                                               "op.school_2_id = slp.school_2_id", connection)).\
            to_csv("optimized_pairs.csv")
    except Error as e:
        print(e)
"""REMOVE THIS BIT WHEN DONE, JUST FOR CHECKING IN MEANTIME"""

"""This pre-processing method was abandoned in favor of that implememented, but was largely complete at the time 
that was decided, so here is the code in case it is useful to anybody
# for each district pair and each school level, narrow the list of candidates for min commute based on following:
# find the minimum distances (as many as the desired number of minimums), take the largest of these
# the maximum estimate for the commute time for this distance is: (circuituity_factor/slow_commute_speed)*distance
# considering that the minimum estimated commute for any distance is: distance/fast_commute_speed
# it is not worth considering distances where their minimum estimated commute is
# longer than the maximum estimated commute thus, we wish to exclude distances longer than:
# largest_min_dist*fast_commute_speed*(circuituity_factor/slow_commute_speed) from the accurate commute analysis

# create a table to store the pairs for which to gather actual commute estimates from API
if connection is not None:
    try:
        connection.cursor().execute("CREATE TABLE IF NOT EXISTS pairs_to_find_commute_between("
                                    "school_1_id INTEGER NOT NULL, "
                                    "school_2_id INTEGER NOT NULL, "
                                    "comparison_level varchar NOT NULL, "
                                    "PRIMARY KEY(school_1_id, school_2_id, comparison_level), "
                                    "FOREIGN KEY (school_1_id) REFERENCES school_info (id), "
                                    "FOREIGN KEY (school_2_id) REFERENCES school_info (id));")
        connection.commit()
    except Error as e:
        print(e)

for district in district_list:
    for other_district in district_list:
        if district != other_district:
            for level in school_levels:
                if connection is not None:
                    try:
                        # get the nth minimum distance (where n = # of desired minimums)
                        nth_min_dist = connection.cursor().execute(
                            "SELECT distance_between FROM "
                            "(SELECT school_2_id, distance_between "
                            "FROM straight_line_pairs JOIN school_info on school_1_id = id "
                            "WHERE level LIKE '%{0}%' AND district_name LIKE '{1}') "
                            "JOIN school_info on school_2_id = id "
                            "WHERE level LIKE '%{0}%' AND district_name LIKE '{2}' "
                            "ORDER BY distance_between ASC Limit 1 offset {3}".format(
                                level, district[0], other_district[0],
                                number_of_minimums_per_disct_pair - 1)).fetchone()
                    except Error as e:
                        print(e)
                        # as long as a distance was returned, proceed to find the pairs that are reasonable to calculate
                        # (the above would return 0 items if there are no pairs between the 2 districts)
                    if nth_min_dist is not None:
                        max_dist_to_consider = nth_min_dist[0]*fast_commute_speed*\
                                               (float(circuituity_factor)/slow_commute_speed)
                        if connection is not None:
                            try:
                                connection.cursor().execute("INSERT INTO pairs_to_find_commute_between "
                                                            "SELECT school_1_id, school_2_id, "
                                                            "'{0}' as comparison_level "
                                                            "FROM (SELECT school_1_id, school_2_id, "
                                                            "distance_between FROM straight_line_pairs "
                                                            "JOIN school_info on school_1_id = id "
                                                            "WHERE level LIKE '%{0}%' "
                                                            "AND district_name LIKE '{1}') "
                                                            "JOIN school_info on school_2_id = id "
                                                            "WHERE level LIKE '%{0}%' AND district_name LIKE '{2}' "
                                                            "AND distance_between < {3} "
                                                            "ORDER BY distance_between ASC".
                                                            format(level, district[0], other_district[0],
                                                                   max_dist_to_consider))
                            except Error as e:
                                print(e)
    # now that we have compared this district to all the others, remove it from the list to avoid duplicates
    district_list.remove(district)"""
