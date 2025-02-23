import pandas as pd
import geopandas as gpd
import os
import shutil
from zipfile import ZipFile
from datetime import datetime
from urllib.request import urlretrieve
import warnings

warnings.simplefilter(action='ignore')
czas = datetime.now().strftime("%Y%m%d %H-%M-%S")

# pobieranie wykazu

os.chdir('src')
nazwa_wykaz = 'irmir-wykaz-gtfs-prod'
wykaz = pd.read_excel(nazwa_wykaz + '.xlsx', sheet_name=None)
wykaz = wykaz.get(list(wykaz.keys())[0])
wykaz = wykaz.dropna()

# pobieranie GTFS bądź kopiowanie istniejących

os.mkdir(czas)
os.chdir(czas)

for i in range(0, len(wykaz)):
    match wykaz.Typ_zrodla.iloc[i]:
        case 'U':
            print('Pobieram GTFS dla:', wykaz.Skrot.iloc[i])
            urlretrieve(wykaz.Adres.iloc[i], wykaz.Skrot.iloc[i] + '.zip')
            with ZipFile(wykaz.Skrot.iloc[i] + ".zip", 'r') as zObject:
                zObject.extractall(wykaz.Skrot.iloc[i])
            print('===')
        case 'F':
            print('Kopiuję istniejący GTFS dla:', wykaz.Skrot.iloc[i])
            os.chdir('..')
            shutil.copyfile(wykaz.Adres.iloc[i] + '.zip', czas + '/' + wykaz.Skrot.iloc[i] + '.zip')
            os.chdir(czas)
            with ZipFile(wykaz.Skrot.iloc[i] + ".zip", 'r') as zObject:
                zObject.extractall(wykaz.Skrot.iloc[i])
            print('===')

# scalanie GTFS
## nowe tabele
new_stops = pd.DataFrame(columns=['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
new_routes = pd.DataFrame(columns=['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type'])
new_trips = pd.DataFrame(columns=['route_id', 'service_id', 'trip_id', 'trip_headsign', 'shape_id'])
new_agency = pd.DataFrame(columns=['agency_id', 'agency_name','agency_url','agency_timezone'])
new_stop_times = pd.DataFrame(columns=['trip_id','arrival_time','departure_time','stop_id','stop_sequence'])
new_calendar_dates = pd.DataFrame(columns=['service_id' ,'date', 'exception_type'])
new_calendar = pd.DataFrame(columns=['service_id','monday','tuesday','wednesday','thursday','friday','saturday','sunday','start_date','end_date'])
new_shapes = pd.DataFrame(columns=['shape_id','shape_pt_lat','shape_pt_lon','shape_pt_sequence'])

## wyciaganie danych z istniejecych gtfs i scalanie do jednego pliku
lst_gtfs = next(os.walk('.'))[1]
bool_cal = False
bool_caldates = False
bool_shp = False

for el in lst_gtfs:
    os.chdir(el)
    print('Scalam GTFS dla: ', el)
    tmp_stops = pd.read_csv('stops.txt').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    tmp_stops = tmp_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    tmp_routes = pd.read_csv('routes.txt').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    tmp_agency = pd.read_csv('agency.txt').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    if 'agency_id' in tmp_routes.columns.tolist():
        tmp_routes = tmp_routes[['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']]
        if 'agency_id' in tmp_agency.columns.tolist():
            tmp_agency = tmp_agency[['agency_id', 'agency_name', 'agency_url', 'agency_timezone']]
            tmp_agency.agency_url = 'https://irmir.pl/'
        else:
            tmp_agency = tmp_agency[['agency_name', 'agency_url', 'agency_timezone']]
            tmp_agency['agency_id'] = el
            tmp_agency = tmp_agency[['agency_id', 'agency_name', 'agency_url', 'agency_timezone']]
            tmp_agency.agency_url = 'https://irmir.pl/'
    else:
        if 'agency_id' in tmp_agency.columns.tolist():
            tmp_agency = tmp_agency[['agency_id', 'agency_name', 'agency_url', 'agency_timezone']]
            tmp_agency['agency_id'] = el
            tmp_agency.agency_url = 'https://irmir.pl/'
            tmp_routes = tmp_routes[['route_id', 'route_short_name', 'route_long_name', 'route_type']]
            tmp_routes['agency_id'] = el
            tmp_routes = tmp_routes[['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']]
        else:
            tmp_agency = tmp_agency[['agency_name', 'agency_url', 'agency_timezone']]
            tmp_agency['agency_id'] = el
            tmp_agency = tmp_agency[['agency_id', 'agency_name', 'agency_url', 'agency_timezone']]
            tmp_agency.agency_url = 'https://irmir.pl/'
            tmp_routes = tmp_routes[['route_id', 'route_short_name', 'route_long_name', 'route_type']]
            tmp_routes['agency_id'] = el
            tmp_routes = tmp_routes[['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']]
    # if 'agency_id' in tmp_agency.columns.tolist():
    #     tmp_agency = tmp_agency[['agency_id','agency_name','agency_url','agency_timezone']]
    #     #tmp_agency['agency_id'] = el # czy na pewno potrzebne
    #     tmp_agency.agency_url = 'https://irmir.pl/'
    # else:
    #     tmp_agency = tmp_agency[['agency_name','agency_url','agency_timezone']]
    #     tmp_agency['agency_id'] = el
    #     tmp_agency = tmp_agency[['agency_id','agency_name','agency_url','agency_timezone']]
    #     tmp_agency.agency_url = 'https://irmir.pl/'
    tmp_stop_times = pd.read_csv('stop_times.txt', dtype={"trip_id": "string"}).apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    tmp_stop_times = tmp_stop_times[['trip_id','arrival_time','departure_time','stop_id','stop_sequence']]
    if (os.path.isfile('calendar_dates.txt')):
        bool_caldates = True
        tmp_calendar_dates = pd.read_csv('calendar_dates.txt').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        tmp_calendar_dates = tmp_calendar_dates[['service_id' ,'date', 'exception_type']]
    if (os.path.isfile('calendar.txt')):
        bool_cal = True
        tmp_calendar = pd.read_csv('calendar.txt').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        tmp_calendar = tmp_calendar[['service_id','monday','tuesday','wednesday','thursday','friday','saturday','sunday','start_date','end_date']]

    tmp_trips = pd.read_csv('trips.txt', dtype={"trip_id": "string", "shape_id": "string"}).apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    if (os.path.isfile('shapes.txt')):
        tmp_shapes = pd.read_csv('shapes.txt', dtype={"shape_id": "string"}).apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        tmp_shapes = tmp_shapes[['shape_id','shape_pt_lat','shape_pt_lon','shape_pt_sequence']]
        print(len(tmp_shapes))
        if (len(tmp_shapes)>0):
            bool_shp = True
            tmp_trips = tmp_trips[['route_id', 'service_id', 'trip_id', 'trip_headsign', 'shape_id']]
    else:
        tmp_trips = tmp_trips[['route_id', 'service_id', 'trip_id', 'trip_headsign']]
        tmp_trips['shape_id'] = ''


    ### filtrowanie
    #### tylko trasy kolejowe (czyli route_type = 2)
    tmp_routes = tmp_routes[tmp_routes.route_type == 2]
    #### specjalnie dla SKM Warszawa wyfiltrowanie tras SKM z pliku dla ZTMu Waw.
    if el=='SKMW':
        tmp_routes['coltmp'] = tmp_routes.route_short_name.astype(str).str[0]
        tmp_routes = tmp_routes[tmp_routes.coltmp == 'S']
        tmp_routes = tmp_routes[['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']]
    tmp_trips = tmp_trips[tmp_trips.route_id.isin(tmp_routes.route_id.tolist())]
    tmp_stop_times = tmp_stop_times[tmp_stop_times.trip_id.isin(tmp_trips.trip_id.tolist())]
    tmp_stops = tmp_stops[tmp_stops.stop_id.isin(tmp_stop_times.stop_id.tolist())]
    tmp_agency = tmp_agency[tmp_agency.agency_id.isin(tmp_routes.agency_id.tolist())]

    ### dodawanie skrotu do GTFS
    tmp_stops.stop_id = el + tmp_stops.stop_id.astype(str)
    tmp_routes.route_id = el + tmp_routes.route_id.astype(str)
    tmp_routes.agency_id = el
    tmp_trips.route_id = el + tmp_trips.route_id.astype(str)
    tmp_trips.service_id = el + tmp_trips.service_id.astype(str)
    tmp_trips.trip_id = el + tmp_trips.trip_id.astype(str)
    tmp_agency.agency_id = el
    tmp_stop_times.trip_id = el + tmp_stop_times.trip_id.astype(str)
    tmp_stop_times.stop_id = el + tmp_stop_times.stop_id.astype(str)
    if bool_caldates:
        tmp_calendar_dates.service_id = el + tmp_calendar_dates.service_id.astype(str)
    if bool_cal:
        tmp_calendar.service_id = el + tmp_calendar.service_id.astype(str)
    if bool_shp:
        tmp_trips.shape_id = el + tmp_trips.shape_id.astype(str)
        tmp_shapes.shape_id = el + tmp_shapes.shape_id.astype(str)

    ### scalanie z główna tabelą
    new_stops = pd.concat([new_stops, tmp_stops])
    new_routes = pd.concat([new_routes, tmp_routes])
    new_trips = pd.concat([new_trips, tmp_trips])
    new_agency = pd.concat([new_agency, tmp_agency])
    new_stop_times = pd.concat([new_stop_times, tmp_stop_times])
    if bool_caldates:
        new_calendar_dates = pd.concat([new_calendar_dates, tmp_calendar_dates])
    if bool_cal:
        new_calendar = pd.concat([new_calendar, tmp_calendar])
    if bool_shp:
        new_shapes = pd.concat([new_shapes, tmp_shapes])

    print('=== scalone ===')

    bool_cal = False
    bool_caldates = False
    bool_shp = False
    os.chdir('..')


## centroidy przystanków
gdf_stops = gpd.GeoDataFrame(new_stops, geometry=gpd.points_from_xy(new_stops.stop_lon, new_stops.stop_lat), crs="EPSG:4326")
gdf_stops.geometry = gdf_stops.geometry.to_crs('EPSG:2180')
gdf_stops.geometry = gdf_stops.geometry.buffer(130)

gdf_intersects = gdf_stops.sjoin(gdf_stops, how="left", predicate="intersects")
gdf_intersects_diss = gdf_intersects.dissolve("stop_id_right",aggfunc="min")
gdf_intersects_diss = gdf_intersects_diss.reset_index().dissolve("stop_id_left",aggfunc="min")

gdf_intersects_diss['centroid'] = gdf_intersects_diss.centroid

gdf_intersects_a = gdf_stops.sjoin(gdf_intersects_diss, how="left", predicate="intersects")
gdf_intersects_a['new_stop_id'] = 'n_' + gdf_intersects_a.stop_id_left.astype(str)
slownik = pd.DataFrame(gdf_intersects_a[['stop_id', 'new_stop_id']])
slownik.drop_duplicates(subset=['stop_id'], inplace=True)
gdf_przystanki = gdf_intersects_a[['new_stop_id', 'stop_name_left', 'centroid']]

gdf_przystanki.geometry = gdf_przystanki.centroid
gdf_przystanki.drop_duplicates(inplace=True)
gdf_przystanki.geometry = gdf_przystanki.geometry.to_crs('EPSG:4326')
gdf_przystanki['stop_lon'] = gdf_przystanki.geometry.x
gdf_przystanki['stop_lat'] = gdf_przystanki.geometry.y
df_przystanki = pd.DataFrame(gdf_przystanki)
df_przystanki = df_przystanki[['new_stop_id','stop_name_left','stop_lat','stop_lon']]
df_przystanki.columns = ['stop_id','stop_name','stop_lat','stop_lon']
df_przystanki.stop_name = df_przystanki.stop_name.str.title()
df_przystanki_a = df_przystanki.drop_duplicates()

new_stop_times = pd.merge(left=new_stop_times, right=slownik, left_on='stop_id', right_on='stop_id', how='left')
new_stop_times = new_stop_times[['trip_id', 'arrival_time', 'departure_time', 'new_stop_id', 'stop_sequence']]
new_stop_times.columns = ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']

## czyszczenie
for el in lst_gtfs:
    shutil.rmtree(el)

## eksport do wspólnego GTFSa
os.chdir('..')
os.chdir('..')
os.chdir('exports')

os.mkdir('_scalony ' + czas)
os.chdir('_scalony ' + czas)

new_agency.to_csv('agency.txt', index = False)
print('Wygenerowano: agency')
new_routes.to_csv('routes.txt', index = False)
print('Wygenerowano: routes')
new_stop_times.to_csv('stop_times.txt', index = False)
print('Wygenerowano: stop_times')
new_trips.to_csv('trips.txt', index = False)
print('Wygenerowano: trips')
df_przystanki.to_csv('stops.txt', index = False)
print('Wygenerowano: stops')
new_calendar.to_csv('calendar.txt', index = False)
print('Wygenerowano: calendar')
new_calendar_dates.to_csv('calendar_dates.txt', index = False)
print('Wygenerowano: calendar_dates')
new_shapes.to_csv('shapes.txt', index = False)
print('Wygenerowano: shapes')

lista_pl_sklad = os.listdir()

with ZipFile("_scalony GTFS " + czas + ".zip", 'w') as zObject2:
    for el in lista_pl_sklad:
        zObject2.write(el)

