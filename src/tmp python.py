# import urllib.request
# import urllib.parse
# import encodings.idna
#
# username = 'kd'
# password = 'Baewongohwug3Ao'
#
# url = 'https://gtfs.i.kiedyprzyjedzie.pl/kd/google_transit.zip'
#
# hr_auth = encodings.idna.ToASCII('Basic realm="Restricted Koleje Dolnoskie GTFS" ').decode('latin-1')
#
# p = urllib.parse.urlencode({'username': username, 'password': password})
# request = urllib.request.Request(url)
# request.add_header('Authorization', 'Basic realm="Restricted Koleje Dolnośląskie GTFS" ' + p)
# #request.add_header('Authorization', hr_auth + ' ' + p)
#
#
#
# response = urllib.request.urlopen(request)
# data = response.read()

import requests

username = 'kd'
password = 'Baewongohwug3Ao'

url = 'https://gtfs.i.kiedyprzyjedzie.pl/kd/google_transit.zip'

headers = {'Authorization':'Basic realm="Restricted Koleje Dolnośląskie GTFS"','username': username, 'password': password}


import httpx

r = httpx.get(url, headers=headers)

