import requests   # module to make HTTP requests
import json       # module for handling the data
import config     # keep the API key in a file outside of git so it can't be stolen


distance_matrix_api_url = "https://maps.googleapis.com/maps/api/distancematrix/json?{0}"
test_origin_lat_long = "7219 Gennaker Drive Tampa Florida 33607"
test_destination_lat_long = "5415 Neff Lake Rd Brooksville Florida 34601"

# built the string that contains the needed url parameters
url_parameter_string = 'units=imperial&origins={0}&destinations={1}&mode={2}&language=en&key={3}'.\
    format(test_origin_lat_long, test_destination_lat_long, 'driving', config.distance_key)
# make the request to get the commute time by doing a GET request to
# the google distance matrix API, passing in the needed parameters

result = requests.get(distance_matrix_api_url.format(url_parameter_string))

wait = 3

