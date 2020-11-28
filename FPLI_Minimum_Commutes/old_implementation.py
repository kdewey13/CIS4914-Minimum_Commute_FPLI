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