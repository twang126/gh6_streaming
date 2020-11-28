import geohash


def lat_lng_to_geohash(lat: float, lng: float, precision: int = 5) -> str:
    return geohash.encode(lat, lng, precision)
