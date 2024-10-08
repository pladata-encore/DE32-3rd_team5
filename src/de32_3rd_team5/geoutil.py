import googlemaps

def reverse_geo(latitude: str, longitude: str):
    gmaps = googlemaps.Client(key="AIzaSyDl4Nte4s05r0q1CrPcUjyD-aZHudnGiOs")

    result = gmaps.reverse_geocode((latitude, longitude), language='ko')
    if result:
        return result[0]['formatted_address']
    else:
        return None
