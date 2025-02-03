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
nazwa_wykaz = 'irmir-wykaz-gtfs-tmp-2'
wykaz = pd.read_excel(nazwa_wykaz + '.xlsx', sheet_name=None)
wykaz = wykaz.get(list(wykaz.keys())[0]).dropna()

# pobieranie GTFS bądź kopiowanie istniejących
os.mkdir(czas)
os.chdir(czas)

for _, row in wykaz.iterrows():
    print(f"Przetwarzam GTFS dla: {row.Skrot}")
    if row.Typ_zrodla == 'U':
        urlretrieve(row.Adres, f"{row.Skrot}.zip")
    else:
        shutil.copyfile(f"../{row.Adres}.zip", f"{row.Skrot}.zip")
    with ZipFile(f"{row.Skrot}.zip", 'r') as zObject:
        zObject.extractall(row.Skrot)
    print("===")

# scalanie GTFS
## nowe tabele
tables = {
    "stops": ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
    "routes": ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type'],
    "trips": ['route_id', 'service_id', 'trip_id', 'trip_headsign'],
    "agency": ['agency_id', 'agency_name', 'agency_url', 'agency_timezone'],
    "stop_times": ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'],
    "calendar_dates": ['service_id', 'date', 'exception_type'],
    "calendar": ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
                 'start_date', 'end_date']
}

data = {k: pd.DataFrame(columns=v) for k, v in tables.items()}
lst_gtfs = next(os.walk('.'))[1]

for el in lst_gtfs:
    os.chdir(el)
    print(f'Scalam GTFS dla: {el}')
    tmp = {k: pd.read_csv(f"{k}.txt").apply(lambda x: x.str.strip() if x.dtype == 'object' else x) for k in tables if
           os.path.isfile(f"{k}.txt")}

    if 'agency_id' not in tmp['routes'].columns:
        tmp['routes']['agency_id'] = el
    if 'agency_id' not in tmp['agency'].columns:
        tmp['agency']['agency_id'] = el

    ### filtrowanie
    tmp['routes'] = tmp['routes'][tmp['routes'].route_type == 2]
    if el == 'SKMW':
        tmp['routes'] = tmp['routes'][tmp['routes'].route_short_name.str.startswith('S')]

    tmp['trips'] = tmp['trips'][tmp['trips'].route_id.isin(tmp['routes'].route_id)]
    tmp['stop_times'] = tmp['stop_times'][tmp['stop_times'].trip_id.isin(tmp['trips'].trip_id)]
    tmp['stops'] = tmp['stops'][tmp['stops'].stop_id.isin(tmp['stop_times'].stop_id)]
    tmp['agency'] = tmp['agency'][tmp['agency'].agency_id.isin(tmp['routes'].agency_id)]

    ### dodawanie skrotu do GTFS
    for t in ['stops', 'routes', 'trips', 'stop_times']:
        tmp[t].iloc[:, 0] = el + tmp[t].iloc[:, 0].astype(str)
    tmp['routes']['agency_id'] = el
    tmp['trips']['service_id'] = el + tmp['trips']['service_id'].astype(str)
    tmp['agency']['agency_id'] = el
    if 'calendar_dates' in tmp:
        tmp['calendar_dates']['service_id'] = el + tmp['calendar_dates']['service_id'].astype(str)
    if 'calendar' in tmp:
        tmp['calendar']['service_id'] = el + tmp['calendar']['service_id'].astype(str)

    for k in tmp:
        data[k] = pd.concat([data[k], tmp[k]])

    os.chdir('..')
    print("=== scalone ===")

## centroidy przystanków
gdf_stops = gpd.GeoDataFrame(data['stops'], geometry=gpd.points_from_xy(data['stops'].stop_lon, data['stops'].stop_lat),
                             crs="EPSG:4326")
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

data['stop_times'] = data['stop_times'].merge(slownik, on='stop_id', how='left').drop(columns=['stop_id']).rename(
    columns={'new_stop_id': 'stop_id'})

## czyszczenie
for el in lst_gtfs:
    shutil.rmtree(el)

## eksport do wspólnego GTFSa
os.chdir('../../exports')
os.mkdir(f'_scalony {czas}')
os.chdir(f'_scalony {czas}')

for k, df in data.items():
    df.to_csv(f'{k}.txt', index=False)
    print(f'Wygenerowano: {k}')

with ZipFile(f"_scalony GTFS {czas}.zip", 'w') as zObject2:
    for el in os.listdir():
        zObject2.write(el)

print('=== KONIEC ===')