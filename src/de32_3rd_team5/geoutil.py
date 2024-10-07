from geopy.geocoders import Nominatim

def loc_trans(location):
	geolocoder = Nominatim(user_agent = 'South Korea', timeout=None)
	address = geolocoder.reverse(location)

	str_address=str(address)
	address_list = str_address.split(',')

	# 리스트를 딕셔너리로 변환
	address_dict = {
    	'address1': address_list[0].strip(),
    	'address2': address_list[1].strip(),
    	'address3': address_list[2].strip(),
    	'address4': address_list[3].strip(),
    	'address5': address_list[4].strip()
	}
	result = f"{address_dict['address4']} {address_dict['address3']} {address_dict['address2']} {address_dict['address1']} {address_dict['address5']}" # 4 3 2 1 5
	return result

a = loc_trans(' -19.039959,  -44.305202')
print(a)
