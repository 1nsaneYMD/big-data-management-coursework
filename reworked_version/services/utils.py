import pycountry_convert as pc
import pycountry

def get_continent_udf(code):
    if code == 'Global':
        return 'Global'
    try:
        continent_code = pc.country_alpha2_to_continent_code(code)
        continent_name_map = {
            'AF': 'Africa',
            'AS': 'Asia',
            'EU': 'Europe',
            'NA': 'North America',
            'SA': 'South America',
            'OC': 'Oceania',
            'AN': 'Antarctica'
        }
        return continent_name_map[continent_code]
    except Exception:
        return None

def iso_to_country_name(code):
    if code == 'Global':
        return 'Global'
    try:
        return pycountry.countries.get(alpha_2=code).name
    except AttributeError:
        return None
