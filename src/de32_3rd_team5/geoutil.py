from geopy.geocoders import Nominatim

def loc_trans(location):
	geolocoder = Nominatim(user_agent = 'South Korea', timeout=None)
	address = geolocoder.reverse(location)

	return address

address = loc_trans('36.5441, 128.5775')
print(address)
