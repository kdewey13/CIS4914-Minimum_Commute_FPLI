import geopy.distance  # module with function for accurate straight line distance calculation
import pandas  # module for spreadsheet/csv read/write & dataframe manipulation
import sqlite3  # in-app database to make querying for minumums easy
from sqlite3 import Error  # allows output of error if occurs
import datetime  # used to manipulate times
import requests   # module to make HTTP requests
import pytz  # module to handle time zone
import tzlocal  # module to handle time zone
import FPLI_Minimum_Commutes.process_data as process_data


def commute_calculator(optimization_radius=70, max_radius_to_consider=50,
                       distance_pairs_determination=True, distance_pairs_csv='distance_pairs.csv',
                       desired_level_details=([True, 3, 3], [True, 2, 2], [True, 2, 2]), charter=True,
                       input_csv='fpli_min_com_input_data.csv', output_csv='fpli_min_commute_pairs.csv',
                       download_msid=False, preprocess=False, unprocessed_excel_file=None,
                       api_key=None, make_api_calls=False):

    print("Start time: {0}".format(datetime.datetime.now()))

    if download_msid:
        process_data.download_data(input_csv=input_csv)
        print("Finished data download and preprocess: {0}".format(datetime.datetime.now()))
    elif preprocess and unprocessed_excel_file is not None:
        process_data.preprocess_fl_msid_data(data_excel_file=unprocessed_excel_file, input_csv=input_csv)
        print("Finished preprocess: {0}".format(datetime.datetime.now()))

    # Create a dictionary of the desired school levels (makes iterating later much cleaner).
    # The keys will be the levels and their values will be a list with the number of unique schools to make pairs
    # with per district as the first item in the list and the second being the number of school connections to
    # make per school from the first value
    # i.e. if elementary_pairs = 3 and elementary_schools = 3, the function will yield 9 pairs of elementary schools
    # per district -> 3 schools, each paired with 3 schools in the other district
    # The variable level_strings is a set (prevents duplication of items) of the possible levels. Used for
    # filtering if not all school levels are wanted.
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

    # Read in the input file into a data frame
    input_data = pandas.read_csv(input_csv)

    # if not considering all 3 school levels, remove undesired ones and remove charter schools if not considering them
    if len(school_levels) != 3:
        input_data = input_data[input_data['level'].isin(level_strings)]
    if not charter:
        input_data = input_data[input_data['charter'] == False]

    # put all the info from the input file into the database and save it
    input_data.to_sql(name='school_info', con=connection, if_exists='replace', index=True, index_label='id')
    connection.commit()

    # create a database cursor (allows us to get around the db) and get a list of all the schools
    school_list = []
    if connection is not None:
        try:
            cursor = connection.cursor()
            school_list = list(cursor.execute("Select * FROM school_info ORDER BY id").fetchall())
        except Error as e:
            print(e)

    # get the indices that the values of interest are at for each school (if the input file is in the same order every
    # time this should be the same, but this doesn't hurt anything to check)
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

    # get a list of the districts to iterate over
    district_list = []
    if connection is not None:
        try:
            district_list = list(cursor.execute("SELECT DISTINCT district_name FROM school_info").fetchall())
        except Error as e:
            print(e)

    """Straight Line Distance Determination"""
    if distance_pairs_determination:
        # if we need to calculate and store the distances between all (viable) school pairs in the state, do this bit
        # (viable=viable for minimal commute; determined by the optimization radius)
        # don't need to repeat if already done for the input data set and saved in a csv, see corresponding else
        if connection is not None:
            try:
                cursor.execute("CREATE TABLE IF NOT EXISTS distance_pairs("
                               "school_1_id INTEGER NOT NULL, "
                               "school_2_id INTEGER NOT NULL, "
                               "distance_between FLOAT NOT NULL, "
                               "PRIMARY KEY(school_1_id, school_2_id), "
                               "FOREIGN KEY (school_1_id) REFERENCES school_info (id), "
                               "FOREIGN KEY (school_2_id) REFERENCES school_info (id));")
                connection.commit()
            except Error as e:
                print(e)

        # get the schools with the max/min latitudes and longitudes per district, these will constitute a 'border' of
        # the schools in that district, use these border schools to determine if the two counties are worth deeper
        # comparison (i.e. if no border schools between the two counties are close enough to be worth comparison, we
        # know no other schools between that pair of counties will be worth comparison either, since all other schools
        # will be further interior and thus even further apart)
        max_min_schools = cursor.execute("SELECT *, MAX(latitude) FROM school_info GROUP BY district_name "
                                         "UNION SELECT *, MIN(latitude) FROM school_info GROUP BY district_name "
                                         "UNION SELECT *, MAX(longitude) FROM school_info GROUP BY district_name "
                                         "UNION SELECT *, MIN(longitude) FROM school_info GROUP BY district_name ").\
            fetchall()
        # translate the list into a dict for easier iteration
        border_schools = {}
        for district in district_list:
            border_schools[district] = []
            for school in max_min_schools:
                if school[district_index] in district:
                    border_schools[district].append(school)
        # determine if any of the border schools fall within the optimization radius per district pair
        checked = []
        for district in district_list:  # for each district in the list
            for other_district in district_list:  # compare it to every district school
                # don't compare to self nor bidirectionally (i.e. if we have a->b distance, don't need b->a)
                if district != other_district and other_district not in checked:
                    # create a variable to tell us if we have found a pair of schools within the optimization radius
                    # between the 2 districts. Begin as false.
                    close_enough = False
                    # find the distances between the border schools between the 2 districts (up to 4 schools ea)
                    for school in border_schools[district]:
                        for other_school in border_schools[other_district]:
                            distance_between = geopy.distance.geodesic((school[lat_index], school[long_index]),
                                                                       (other_school[lat_index],
                                                                        other_school[long_index])).miles
                            # if a pair is within the optimization radius
                            if distance_between <= optimization_radius:
                                # we have found a close enough pair to know it is worth checking all the school pairs
                                # for these 2 counties, break b/c we only need 1 to do full county pair comparison
                                close_enough = True
                                break
                        # break from the outer loop if we found one, no need to keep checking border pairs
                        if close_enough:
                            break
                    if close_enough:
                        # if here, then at least one pair of schools between the two counties are within the
                        # optimization radius, so compare all of the schools in the two counties and save them to the
                        # distance table if they are within the radius

                        # first get all the schools for each county
                        district_1_schools = cursor.execute("SELECT * FROM school_info "
                                                            "WHERE district_name LIKE '%{0}%'".
                                                            format(district[0])).fetchall()
                        district_2_schools = cursor.execute("SELECT * FROM school_info "
                                                            "WHERE district_name LIKE '%{0}%'".
                                                            format(other_district[0])).fetchall()
                        for school in district_1_schools:  # for each school in the list
                            for other_school in district_2_schools:  # compare it to every other school
                                if school != other_school:  # don't compare to self
                                    # find the distance if they are the same level (or for combos, at least 1 level is)
                                    if (school[level_index] in other_school[level_index]
                                        or other_school[level_index] in school[level_index]) \
                                            and school[district_index] != other_school[district_index]:
                                        distance_between = geopy.distance.geodesic((school[lat_index],
                                                                                    school[long_index]),
                                                                                   (other_school[lat_index],
                                                                                    other_school[long_index])).miles
                                        #  if distance between is less than the threshold, save the pair
                                        if distance_between <= optimization_radius:
                                            if connection is not None:
                                                try:
                                                    # save the record to the pairs table
                                                    cursor.execute(
                                                        "INSERT INTO distance_pairs(school_1_id, school_2_id, "
                                                        "distance_between) VALUES ({0},{1},{2})".
                                                        format(school[id_index], other_school[id_index],
                                                               distance_between))
                                                    connection.commit()
                                                except Error as e:
                                                    print(e)
            # have compared to every other district, so add to the checked list to avoid bidirectional calculations
            checked.append(district)

        print("Finished straight line distance {0}".format(datetime.datetime.now()))
        # Saves a csv of the distance pairs with details for interim examining and a csv of the distance pairs table
        # for reloading without doing the above calculations
        if connection is not None:
            try:
                # save the distance pairs with their details for interim examining
                pandas.DataFrame(pandas.read_sql_query(
                    "SELECT school_1_id, school_2_id, School_1_Name, School_1_District, "
                    "school_name as 'School_2_Name', district_name as 'School_2_District', distance_between "
                    "FROM (SELECT school_1_id, school_2_id, "
                    "school_name as 'School_1_Name', district_name as 'School_1_District', distance_between "
                    "FROM distance_pairs JOIN school_info on school_1_id = id) "
                    "JOIN school_info on school_2_id = id",
                    connection)).to_csv('distance_pairs_details.csv', index=False)
            except Error as e:
                print(e)
            try:
                # save the distance pairs table for reloading
                pandas.DataFrame(pandas.read_sql_query("SELECT * from distance_pairs", connection)).\
                    to_csv(distance_pairs_csv, index=False)
            except Error as e:
                print(e)

    else:
        # If distance_pairs_determination was not selected when the function was run, means we already have made
        # those calculations and have them stored in a csv, all we need to do is load back up and save in db
        pandas.read_csv(distance_pairs_csv).to_sql(name='distance_pairs', con=connection, if_exists='replace',
                                                    index=False)
        connection.commit()

    # create a table to store pairs with distance_between < max_radius_to_consider and insert the appropriate pairs
    if connection is not None:
        try:
            cursor.execute("CREATE TABLE IF NOT EXISTS straight_line_pairs("
                           "school_1_id INTEGER NOT NULL, "
                           "school_2_id INTEGER NOT NULL, "
                           "distance_between FLOAT NOT NULL, "
                           "PRIMARY KEY(school_1_id, school_2_id), "
                           "FOREIGN KEY (school_1_id) REFERENCES school_info (id), "
                           "FOREIGN KEY (school_2_id) REFERENCES school_info (id));")
            connection.commit()
        except Error as e:
            print(e)
        try:
            cursor.execute("INSERT INTO straight_line_pairs SELECT * FROM distance_pairs "
                           "WHERE distance_between < {0}".format(max_radius_to_consider))
            connection.commit()
        except Error as e:
            print(e)

    """Minimum and commute determination"""
    # For districts A and B find the closest pair in A->B, that school pair = a1b11. For the school a1 in A,
    # find the next n-1 closest schools in B (where n = desired total number of connections per school).
    # Will have n connections; a1b11, a1b12, a1b13. Repeat for next closest pair a2-b21 until desired number of unique
    # schools in A have been found and compared with n schools in B. (schools selected in b need not be unique,
    # i.e. while a1 != a2 != a3....b11 can = b21 or b22 or b31...etc. Thus this is unidirectional; A->B != B->A).

    # create a table to store the pairs and their commute estimates from API, values related to commute may be
    # NULL if not making API calls
    if connection is not None:
        try:
            cursor.execute("CREATE TABLE IF NOT EXISTS commute_pairs("
                           "origin_school INTEGER NOT NULL, "
                           "destination_school INTEGER NOT NULL, "
                           "comparison_level varchar NOT NULL, "
                           "commute_distance FLOAT, "
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
                                               "JOIN school_info on school_2_id = id "
                                               "WHERE School_1_District LIKE '%{0}%' "
                                               "UNION SELECT district_name FROM "
                                               "(SELECT school_1_id, district_name as 'School_2_District' "
                                               "FROM straight_line_pairs JOIN school_info on school_2_id = id) "
                                               "JOIN school_info on school_1_id = id "
                                               "WHERE School_2_District LIKE '%{0}%')"
                                               .format(district[0])).fetchall())
        # store the api calls made to avoid duplicate calls ($), stored as dictionary with keys = pair of school ids in
        # tuple form -> (id_1, id_2). Use tuple b/c its immutable (required for dict keys) and ordered (important b/c
        # (id_1, id_2) != (id_2, id_1). The values will be another tuple consisting of (commute distance, commute time)
        api_calls_made = {}
        for other_district in paired_districts:
            if district != other_district:  # don't compare to self
                for level in school_levels.keys():
                    min_schools = []
                    if connection is not None:
                        try:
                            min_schools = cursor.execute("SELECT origin_school, MIN(distance_between) "
                                                         "FROM (SELECT school_1_id as origin_school, distance_between "
                                                         "FROM (SELECT school_1_id, school_2_id, distance_between "
                                                         "FROM straight_line_pairs "
                                                         "JOIN school_info on school_1_id = id "
                                                         "WHERE level LIKE '%{0}%' AND district_name LIKE '{1}') "
                                                         "JOIN school_info on school_2_id = id "
                                                         "WHERE level LIKE '%{0}%' "
                                                         "AND district_name LIKE '{2}' "
                                                         "UNION SELECT school_2_id as origin_school, distance_between "
                                                         "FROM (SELECT school_1_id, school_2_id, distance_between "
                                                         "FROM straight_line_pairs "
                                                         "JOIN school_info on school_1_id = id "
                                                         "WHERE level LIKE '%{0}%' AND district_name LIKE '{2}') "
                                                         "JOIN school_info on school_2_id = id "
                                                         "WHERE level LIKE '%{0}%' "
                                                         "AND district_name LIKE '{1}' ) "
                                                         "GROUP BY origin_school ORDER BY distance_between Limit {3}".
                                                         format(level, district[0], other_district[0],
                                                                school_levels[level][0])).fetchall()
                        except Error as e:
                            print(e)
                    # now that we have the needed number of schools, we find the desired # of minimal pairs for each
                    for tup in min_schools:
                        other_mins = []  # list to store the next closest school pairs
                        # if making API calls is selected, perform the request and store in db after finding pairs
                        if make_api_calls:
                            if connection is not None:
                                try:
                                    # get the desired # of minimum pairs for this school to schools in the other county
                                    other_mins = cursor.execute(
                                        "SELECT origin_school, destination_school "
                                        "FROM (SELECT school_1_id as origin_school, school_2_id as destination_school, "
                                        "distance_between FROM (SELECT school_1_id, school_2_id, distance_between "
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
                                                   school_levels[level][1])).fetchall()
                                except Error as e:
                                    print(e)
                                # get the commute times between the schools
                                time = get_time()   # gets 7 am tomorrow in proper format for API
                                url = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial" \
                                      "{0}&key={1}&mode=driving&language=en&departure_time={2}&traffic_model=best_guess"
                                location_string = ""
                                # list to store the destination school ids in the order they were placed in the
                                # request so we know the order of the response
                                order_of_responses = []
                                # use find_school to get the entry for this school from the school list (~line 63)
                                origin_school = find_school(other_mins[0][0], id_index, school_list)
                                if origin_school:
                                    location_string = "&origins={0}%2C{1}&destinations=".\
                                        format(origin_school[lat_index], origin_school[long_index])
                                for pair in other_mins:
                                    school = find_school(pair[1], id_index, school_list)
                                    if school:
                                        # check to see if we already have the commute for this pair
                                        if (origin_school[id_index], school[id_index]) in api_calls_made.keys():
                                            try:
                                                # save the record to the commute_pairs table
                                                cursor.execute(
                                                    "INSERT INTO commute_pairs(origin_school, destination_school, "
                                                    "comparison_level, commute_distance, commute_time ) "
                                                    "VALUES ({0},{1},'{2}',{3}, {4})".
                                                        format(origin_school[id_index], school[id_index],
                                                               level, api_calls_made[(origin_school[id_index],
                                                                                      school[id_index])][0],
                                                               api_calls_made[(origin_school[id_index],
                                                                               school[id_index])][1]))
                                                connection.commit()
                                            except Error as e:
                                                print(e)
                                        else:
                                            # add this school to the list that went in the request
                                            order_of_responses.append(school[id_index])
                                            # and add it to the location string to insert into the API call
                                            location_string += "{0}%2c{1}%7C".format(school[lat_index],
                                                                                     school[long_index])
                                # make the request to the API; remove the trailing %7C (separator between destination
                                # lat/long pairs) from the location string and convert the time to an integer
                                commutes = requests.get(url.format(location_string[:-3], api_key, int(time)))
                                if commutes.json()['status'] == 'OK':
                                    for result, dest_id in zip(commutes.json()['rows'][0]['elements'],
                                                               order_of_responses):
                                        # if the calculation was successful
                                        if result['status'] == 'OK':
                                            try:
                                                # save the record to the commute_pairs table
                                                # converting meters to miles and seconds to minutes
                                                cursor.execute(
                                                    "INSERT INTO commute_pairs(origin_school, destination_school, "
                                                    "comparison_level, commute_distance, commute_time ) "
                                                    "VALUES ({0},{1},'{2}',{3}, {4})".
                                                        format(other_mins[0][0], dest_id,
                                                               level, (result['distance']['value']/1609.344),
                                                               (result['duration_in_traffic']['value']/60.0)))
                                                connection.commit()
                                                # save the value to the api_calls_made dictionary to avoid duplicates
                                                api_calls_made[(other_mins[0][0], dest_id)] = \
                                                    ((result['distance']['value']/1609.344),
                                                     (result['duration_in_traffic']['value']/60.0))
                                            except Error as e:
                                                print(e)
                        # if making API calls is NOT selected, only store the pairs. This can be used to determine the
                        # price of making the API calls before you do so
                        else:
                            if connection is not None:
                                try:
                                    # get the desired # of minimum pairs for this school to schools in the other county
                                    # since we're not making any calculations with the info, we can directly store the
                                    # selections into the results table
                                    cursor.execute(
                                        "INSERT INTO commute_pairs "
                                        "SELECT origin_school, destination_school, "
                                        "'{0}' as comparison_level, NULL as commute_distance, NULL as commute_time "
                                        "FROM (SELECT school_1_id as origin_school, school_2_id as destination_school, "
                                        "distance_between FROM (SELECT school_1_id, school_2_id, distance_between "
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
                                                   school_levels[level][1]))
                                except Error as e:
                                    print(e)
    print("Finished pairs determination {0}".format(datetime.datetime.now()))
    if connection is not None:
        try:
            # save the pairs and their commutes to the output file
            pandas.DataFrame(pandas.read_sql_query(
                "SELECT * FROM (SELECT op.origin_school as 'Origin_School_ID', op.destination_school as "
                "'Destination_School_ID', comparison_level, distance_between, commute_distance, commute_time, "
                "op.Origin_School_Name, op.Origin_District, op.Origin_Grade_Levels, op.Origin_Address, "
                "op.Origin_Latitude, op.Origin_Longitude, op.Origin_Charter_Status, "
                "op.Destination_School_Name, op.Destination_District, op.Destination_Grade_Levels, "
                "op.Destination_Address, op.Destination_Latitude, op.Destination_Longitude, "
                "op.Destination_Charter_Status "
                "FROM (SELECT origin_school, destination_school, comparison_level, commute_distance, commute_time, "
                "Origin_School_Name, Origin_District, Origin_Grade_Levels, Origin_Address, Origin_Latitude, "
                "Origin_Longitude, Origin_Charter_Status, school_name as 'Destination_School_Name', "
                "district_name as 'Destination_District', level as 'Destination_Grade_Levels', "
                "street_address || ' ' || city || ' ' || state || ' ' || zip as 'Destination_Address', "
                "latitude as 'Destination_Latitude', longitude as 'Destination_Longitude', "
                "charter as 'Destination_Charter_Status' "
                "FROM (SELECT origin_school, destination_school, comparison_level, commute_distance, commute_time, "
                "school_name as 'Origin_School_Name', district_name as 'Origin_District', "
                "level as 'Origin_Grade_Levels', "
                "street_address || ' ' || city || ' ' || state || ' ' || zip as 'Origin_Address', "
                "latitude as 'Origin_Latitude', longitude as 'Origin_Longitude', charter as 'Origin_Charter_Status' "
                "FROM commute_pairs JOIN school_info on origin_school = id) "
                "JOIN school_info on destination_school = id) AS op JOIN straight_line_pairs AS slp "
                "ON op.origin_school = slp.school_1_id AND op.destination_school = slp.school_2_id "
                "UNION SELECT op2.origin_school as 'Origin_School_ID', op2.destination_school as "
                "'Destination_School_ID', comparison_level, distance_between, commute_distance, commute_time, "
                "op2.Origin_School_Name, op2.Origin_District, op2.Origin_Grade_Levels, op2.Origin_Address, "
                "op2.Origin_Latitude, op2.Origin_Longitude, op2.Origin_Charter_Status, "
                "op2.Destination_School_Name, op2.Destination_District, op2.Destination_Grade_Levels, "
                "op2.Destination_Address, op2.Destination_Latitude, op2.Destination_Longitude, "
                "op2.Destination_Charter_Status "
                "FROM (SELECT origin_school, destination_school, comparison_level, commute_distance, commute_time, "
                "Origin_School_Name, Origin_District, Origin_Grade_Levels, Origin_Address, Origin_Latitude, "
                "Origin_Longitude, Origin_Charter_Status, school_name as 'Destination_School_Name', "
                "district_name as 'Destination_District', level as 'Destination_Grade_Levels', "
                "street_address || ' ' || city || ' ' || state || ' ' || zip as 'Destination_Address', "
                "latitude as 'Destination_Latitude', longitude as 'Destination_Longitude', "
                "charter as 'Destination_Charter_Status' "
                "FROM (SELECT origin_school, destination_school, comparison_level, "
                "commute_distance, commute_time, school_name as 'Origin_School_Name', "
                "district_name as 'Origin_District', level as 'Origin_Grade_Levels', "
                "street_address || ' ' || city || ' ' || state || ' ' || zip as 'Origin_Address', "
                "latitude as 'Origin_Latitude', longitude as 'Origin_Longitude', charter as 'Origin_Charter_Status' "
                "FROM commute_pairs JOIN school_info on origin_school = id) "
                "JOIN school_info on destination_school = id) AS op2 JOIN straight_line_pairs AS slp "
                "ON op2.origin_school = slp.school_2_id AND op2.destination_school = slp.school_1_id) "
                "ORDER BY Origin_District, Destination_District, comparison_level, Origin_School_ID, distance_between",
                connection)).to_csv(output_csv, index=False)
        except Error as e:
            print(e)


def find_school(school_id, id_index, school_list):
    """Returns the school tuple that matches 'school_id' from 'school_list'."""
    # by our definition of school_list/the school ids, the index of the school in the list
    # should always also be its id, but check jic to make sure we don't break anything
    if school_list[school_id] == school_id:
        return school_list[school_id]
    else:
        for school in school_list:
            if school[id_index] == school_id:
                return school
        return None


def get_time():
    """Returns a time string in the appropriate format (seconds since epoch) for the Google Distance Matrix API
     Will correspond to 7 AM on the day after the program is being run (in the time zone of the machine running it)."""
    # Reference: https://stackoverflow.com/questions/30822699/how-to-convert-tomorrows
    # -at-specific-time-date-to-a-timestamp
    local_timezone = tzlocal.get_localzone()
    now = datetime.datetime.now(local_timezone)
    naive_dt7 = datetime.datetime.combine(datetime.datetime.now(tzlocal.get_localzone()), datetime.time(7))
    try:
        dt7 = tzlocal.get_localzone().localize(naive_dt7, is_dst=None)
    except pytz.NonExistentTimeError:  # no such time today
        pass
    except pytz.AmbiguousTimeError:  # DST transition
        dst = local_timezone.localize(naive_dt7, is_dst=True)
        std = local_timezone.localize(naive_dt7, is_dst=False)
        if now < min(dst, std):
            dt7 = min(dst, std)
        elif now < max(dst, std):
            dt7 = max(dst, std)
    else:
        if now < dt7:
            pass
    return ((dt7 + datetime.timedelta(days=1)) - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()