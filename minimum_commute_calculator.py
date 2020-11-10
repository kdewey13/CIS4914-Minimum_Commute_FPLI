import geopy.distance  # python module with function for accurate straight line distance calculation
import pandas  # module for easy spreadsheet/csv read/write & dataframe manipulation
import sqlite3  # in-app database to make querying for minumums easy
from sqlite3 import Error  # allows output of error if occurs
import datetime  # times program run
import config  # local API key
import requests   # module to make HTTP requests


"""---------------------------------------------------------------------------------"""
"""For this program to work, you must input a file in the same format as the example"""
"""---------------------------------------------------------------------------------"""

"""INPUT PARAMETERS"""
max_radius_to_consider = 50
# desired_level_details consists of 3 lists for elementary, middle, high  (in order). Each list is composed of
# [boolean, int_1, int_2] where boolean is whether the level is desired in the analysis, int_1 is the # of unique
# schools to make connections with per district for this level, and int_2 is the number of connections to make with
# those schools. If level not desired, int_1/2 can be anything.
desired_level_details = [[True, 3, 3], [True, 2, 2], [True, 2, 2]]
charter = True
api_key = config.distance_key
input_file = 'input_data.csv'
distance_pairs_file = 'distance_pairs.csv'
output_file = ''
"""INPUT PARAMETERS"""

print("Start time: {0}".format(datetime.datetime.now()))


# create a dictionary of the desired school levels with level serving as the key and the values as a list
# (makes iterating later much clearer). The first value in the list is the number of unique schools to make pairs
# with per district and the second value is the number of school connections to make per school from the first value
# i.e. if elementary_pairs = 3 and elementary_schools = will yield 9 pairs of elementary schools per district
# -> 3 schools, each paired with 3 schools in the other district
# level_strings is a set of the possible level used for filtering if not all school levels are wanted
school_levels, level_strings = {}, set()
if desired_level_details[0][0]:
    school_levels['elementary'] = [desired_level_details[0][1], desired_level_details[0][2]]
    level_strings.update(['elementary', 'elementary, middle', 'elementary, high'])
if desired_level_details[1][0]:
    school_levels['middle'] = [desired_level_details[1][1], desired_level_details[1][2]]
    level_strings.update(['middle', 'elementary, middle', 'middle, high'])
if desired_level_details[2][0]:
    school_levels['high'] = [desired_level_details[2][1], desired_level_details[2][2]]
    level_strings.update(['high', 'elementary, high', 'middle, high'])

# Create an in-memory database; only exists when program is running (no need for persistence)
connection = sqlite3.connect(':memory:')

# Read in the input file
input_data = pandas.read_csv(input_file)

# if not considering all 3 school levels, remove the ones we don't want. Remove charter schools if not considering them
if len(school_levels) != 3:
    input_data = input_data[input_data['level'].isin(level_strings)]
if not charter:
    input_data = input_data[input_data['charter'] == False]

# put all the info from the input file into the DB and save it
input_data.to_sql(name='school_info', con=connection, if_exists='replace', index=True, index_label='id')
connection.commit()

# create a database cursor and get a list of all the schools
school_list = []
if connection is not None:
    try:
        cursor = connection.cursor()
        school_list = list(cursor.execute("Select * FROM school_info").fetchall())
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
"""
# create a table to store the pairs of schools that are less than max_radius_to_consider miles apart
if connection is not None:
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS straight_line_pairs("
                       "school_1_id INTEGER NOT NULL, school_2_id INTEGER NOT NULL, distance_between FLOAT NOT NULL, "
                       "PRIMARY KEY(school_1_id, school_2_id), "
                       "FOREIGN KEY (school_1_id) REFERENCES school_info (id), "
                       "FOREIGN KEY (school_2_id) REFERENCES school_info (id));")
        connection.commit()
    except Error as e:
        print(e)

# find the straight line distance for each school pair and save to table if less than the radius we're considering
checked = []
for school in school_list:  # for each school in the list
    for other_school in school_list:  # compare it to every other school
        # don't compare to self nor bidirectionally (i.e. if we have a->b distance, don't need b->a)
        if school != other_school and other_school[id_index] not in checked:
            # if they are the same level (or in case of combos, one of the levels is the same) find the distance
            if (school[level_index] in other_school[level_index] or other_school[level_index] in school[level_index]) \
                    and school[district_index] != other_school[district_index]:
                distance_between = geopy.distance.vincenty((school[lat_index], school[long_index]),
                                                           (other_school[lat_index], other_school[long_index])).miles
                #  if distance between is less than the threshold, save the pair
                if distance_between <= max_radius_to_consider:
                    if connection is not None:
                        try:
                            # save the record to the pairs table
                            cursor.execute("INSERT INTO straight_line_pairs(school_1_id, school_2_id, "
                                           "distance_between) VALUES ({0},{1},{2})".
                                           format(school[id_index], other_school[id_index], distance_between))
                            connection.commit()
                        except Error as e:
                            print(e)
    # now that we have compared to every school, add it to the checked list to avoid bidirectional calculations
    checked.append(school[id_index])

print("Finished straightline distance {0}".format(datetime.datetime.now()))

# TODO: Save a csv of the distance pairs for interim examining+debugging, remove when done
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
            to_csv(distance_pairs_file)
    except Error as e:
        print(e)"""
"""Load up distance pairs to avoid recalculating while debugging"""
data = pandas.read_csv("distance_pairs.csv")
data.to_sql(name='straight_line_pairs', con=connection, if_exists='replace', index=False)

# get a list of the districts to iterate over
district_list = []
if connection is not None:
    try:
        district_list = list(cursor.execute("SELECT DISTINCT district_name FROM school_info").fetchall())
    except Error as e:
        print(e)

"""Minimum and commute determination"""
# For districts A and B find the closest pair in A->B, school pair = a1b11. For the school a1 in A,
# find the next n-1 closest schools in B (where n = desired total number of connections per school).
# Will have n connections; a1b11, a1b12, a1b13. Repeat for next closest pair a2-b21 until desired number of unique
# schools in A have been found and compared with n schools in B. (schools selected in b need not be unique, i.e. while
# a1 != a2 != a3....b11 = b21 or b22 or b31...etc is allowed. Thus this is unidirectional; A->B != B->A).

# create a table to store the pairs and their commute estimates from API
if connection is not None:
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS commute_pairs("
                       "origin_school INTEGER NOT NULL, "
                       "destination_school INTEGER NOT NULL, "
                       "comparison_level varchar NOT NULL, "
                       "commute_time FLOAT, "
                       "PRIMARY KEY(origin_school, destination_school, comparison_level), "
                       "FOREIGN KEY (origin_school) REFERENCES school_info (id), "
                       "FOREIGN KEY (destination_school) REFERENCES school_info (id));")
        connection.commit()
    except Error as e:
        print(e)

for district in district_list:
    # get the districts for which pairs exist for this county
    paired_districts = list(cursor.execute("SELECT DISTINCT district_name FROM (SELECT district_name "
                                           "FROM (SELECT school_2_id, district_name as 'School_1_District' "
                                           "FROM straight_line_pairs JOIN school_info on school_1_id = id) "
                                           "JOIN school_info on school_2_id = id WHERE School_1_District LIKE '%{0}%' "
                                           "UNION SELECT district_name FROM "
                                           "(SELECT school_1_id, district_name as 'School_2_District' "
                                           "FROM straight_line_pairs JOIN school_info on school_2_id = id) "
                                           "JOIN school_info on school_1_id = id WHERE School_2_District LIKE '%{0}%')"
                                           .format(district[0])).fetchall())
    for other_district in paired_districts:
        if district != other_district:  # don't compare to self
            for level in school_levels.keys():
                min_schools = []
                if connection is not None:
                    try:
                        min_schools = cursor.execute("SELECT origin_school, MIN(distance_between) "
                                                     "FROM (SELECT school_1_id as origin_school, distance_between "
                                                     "FROM (SELECT school_1_id, school_2_id, distance_between "
                                                     "FROM straight_line_pairs JOIN school_info on school_1_id = id "
                                                     "WHERE level LIKE '%{0}%' AND district_name LIKE '{1}') "
                                                     "JOIN school_info on school_2_id = id WHERE level LIKE '%{0}%' "
                                                     "AND district_name LIKE '{2}' "
                                                     "UNION SELECT school_2_id as origin_school, distance_between "
                                                     "FROM (SELECT school_1_id, school_2_id, distance_between "
                                                     "FROM straight_line_pairs JOIN school_info on school_1_id = id "
                                                     "WHERE level LIKE '%{0}%' AND district_name LIKE '{2}') "
                                                     "JOIN school_info on school_2_id = id WHERE level LIKE '%{0}%' "
                                                     "AND district_name LIKE '{1}' ) "
                                                     "GROUP BY origin_school ORDER BY distance_between Limit {3}".
                                                     format(level, district[0], other_district[0],
                                                            school_levels[level][0])).fetchall()
                    except Error as e:
                            print(e)
                # now that we have the needed number of schools, we find the desired number of minimal pairs for each
                # and use the API to find their distance
                for tup in min_schools:
                    if connection is not None:
                        try:
                            # get the desired number of minimum pairs for this school to schools in the other county
                            cursor.execute("INSERT INTO commute_pairs "
                                           "SELECT origin_school, destination_school, "
                                           "'{0}' AS comparison_level, {5} as commute_time "
                                           "FROM (SELECT school_1_id as origin_school, school_2_id as "
                                           "destination_school, distance_between FROM (SELECT "
                                           "school_1_id, school_2_id, distance_between "
                                           "FROM straight_line_pairs JOIN school_info on school_1_id = id "
                                           "WHERE level LIKE '%{0}%' AND district_name LIKE '{1}') "
                                           "JOIN school_info on school_2_id = id WHERE level LIKE '%{0}%' "
                                           "AND district_name LIKE '{2}' "
                                           "UNION SELECT school_2_id as origin_school, "
                                           "school_1_id as destination_school, distance_between FROM "
                                           "(SELECT school_1_id, school_2_id, distance_between "
                                           "FROM straight_line_pairs JOIN school_info on school_1_id = id "
                                           "WHERE level LIKE '%{0}%' AND district_name LIKE '{2}') "
                                           "JOIN school_info on school_2_id = id WHERE level LIKE '%{0}%' "
                                           "AND district_name LIKE '{1}') "
                                           "WHERE origin_school = {3} ORDER BY distance_between LIMIT {4}".
                                           format(level, district[0], other_district[0], tup[0],
                                                  school_levels[level][1], 0.234))  # garbage number, try with API function
                            connection.commit()
                        except Error as e:
                            print(e)

print("Finished pairs determination {0}".format(datetime.datetime.now()))
if connection is not None:
    try:
        # save the pairs for interim examining
        pandas.DataFrame(pandas.read_sql_query(
            "SELECT * FROM (SELECT op.origin_school, op.destination_school, comparison_level, op.Origin_School_Name, "
            "op.Origin_District, op.Destination_School_Name, op.Destination_District, distance_between, commute_time "
            "FROM (SELECT origin_school, destination_school, comparison_level, commute_time, Origin_School_Name, "
            "Origin_District, school_name as 'Destination_School_Name', district_name as 'Destination_District' "
            "FROM (SELECT origin_school, destination_school, comparison_level, commute_time, school_name as "
            "'Origin_School_Name', district_name as 'Origin_District' "
            "FROM commute_pairs JOIN school_info on origin_school = id) "
            "JOIN school_info on destination_school = id) AS op JOIN straight_line_pairs AS slp "
            "ON op.origin_school = slp.school_1_id AND op.destination_school = slp.school_2_id "
            "UNION SELECT op2.origin_school, op2.destination_school, comparison_level, op2.Origin_School_Name, "
            "op2.Origin_District, op2.Destination_School_Name, op2.Destination_District, distance_between, "
            "commute_time FROM (SELECT origin_school, destination_school, comparison_level, commute_time, "
            "Origin_School_Name, Origin_District, school_name as 'Destination_School_Name', district_name as "
            "'Destination_District' FROM (SELECT origin_school, destination_school, comparison_level, commute_time, "
            "school_name as 'Origin_School_Name', district_name as 'Origin_District' "
            "FROM commute_pairs JOIN school_info on origin_school = id) "
            "JOIN school_info on destination_school = id) AS op2 JOIN straight_line_pairs AS slp "
            "ON op2.origin_school = slp.school_2_id AND op2.destination_school = slp.school_1_id) "
            "ORDER BY Origin_District, Destination_District, comparison_level, origin_school, distance_between",
            connection)).to_csv("pairs_for_API.csv")
    except Error as e:
        print(e)

"""This pre-processing optimization method was abandoned in favor of that implememented, but was largely complete 
at the time that was decided, so here is the code in case it is useful to anybody
# for each district pair and each school level, narrow the list of candidates for min commute based on following:
# find the minimum distances (as many as the desired number of minimums), take the largest of these
# the maximum estimate for the commute time for this distance is: (circuituity_factor/slow_commute_speed)*distance
# considering that the minimum estimated commute for any distance is: distance/fast_commute_speed
# it is not worth considering distances where their minimum estimated commute is
# longer than the maximum estimated commute thus, we wish to exclude distances longer than:
# largest_min_dist*fast_commute_speed*(circuituity_factor/slow_commute_speed) from the accurate commute analysis

# ADDITIONAL INPUT PARAMETERS NEEDED
# circuituity_factor = 2
# slow_commute_speed = 30
# fast_commute_speed = 60
# number_of_minimums_per_disct_pair = 3

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
