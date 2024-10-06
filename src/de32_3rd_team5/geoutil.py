from geopy.geocoders import Nominatim

def loc_trans(location):
	geolocoder = Nominatim(user_agent = 'South Korea', timeout=None)
	address = geolocoder.reverse(location)

	return address
