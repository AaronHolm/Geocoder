import pandas as pd
import geocoder
import os
from simple_salesforce import Salesforce, SalesforceLogin
import sqlalchemy as sa
from io import StringIO
from config import *

def bing_api(df):
  bing_key = BING_KEY
  addresses = []
  for i, row in df.iterrows():
    if(row['street'] and row['city'] and row['state']):
      address = row['street'] + ' ' + row['city'] + ', ' + row['state']
      g = geocoder.bing(address, key=bing_key)
      addresses.append([row['Id'], row['account'], g.json])
  addy_list = []
  for add in addresses:
    add_id = add[0]
    add_acct = add[1]
    add_data = add[2]
    print(add_data)
    street = add_data['raw']['address']['addressLine']
    city = add_data['raw']['address']['locality']
    state = add_data['raw']['address']['adminDistrict']
    county = add_data['raw']['address']['adminDistrict2']
    zipcode = add_data['raw']['address']['postalCode']
    country = add_data['raw']['address']['countryRegion']
    lat = add_data['raw']['point']['coordinates'][0]
    lon = add_data['raw']['point']['coordinates'][1]    
    confidence = add_data['confidence']
    add_row = [add_id, add_acct, street, city, state, county, zipcode, country, lat, lon, confidence]
    addy_list.append(add_row)
  full_address = pd.DataFrame(addy_list, columns=['id', 'acct_name', 'shipping_street', 'city', 'state', 'county', 'zipcode', 'country', 'lat', 'lon', 'confidence'])
  return full_address

def sql_processing(df, engine, schema, table, if_exists):
  df[:0].to_sql(table, engine, schema=schema, if_exists=if_exists, index=False)

  output = StringIO()
  df.to_csv(output, sep='\t', header=False, encoding='utf8', index=False)
  output.seek(0)

  connection = engine.raw_connection()
  cursor = connection.cursor()
  cursor.copy_from(output, schema+'.'+table, sep='\t', null='')

  # Create geom column
  sql_geom = '''ALTER TABLE ''' + schema+'.'+table + ''' ADD COLUMN geom geometry(Geometry,3857);'''
  cursor.execute(sql_geom)
  sql_update_geom = '''UPDATE ''' + schema+'.'+table+ ''' SET geom = st_SetSrid(ST_MakePoint(lon, lat), 3857);'''
  cursor.execute(sql_update_geom)
  sql_alter_epsg = '''ALTER TABLE ''' + schema+'.'+table + ''' ALTER COLUMN geom TYPE geometry(POINT, 4269)
    USING ST_SetSRID(geom,4269);'''
  cursor.execute(sql_alter_epsg)

  ###################################
  # Spatial Join SQL Statement Update
  # Congressional District 116
  sql_add_columns = 'ALTER TABLE ' + schema+'.'+table+ ''' ADD COLUMN cd116 VARCHAR,
                        ADD COLUMN cd116_name VARCHAR,
                        ADD COLUMN state_upper VARCHAR, 
                        ADD COLUMN state_lower VARCHAR,
                        ADD COLUMN county_new VARCHAR,
                        ADD COLUMN metro_area VARCHAR,
                        ADD COLUMN elec_plan_area VARCHAR,
                        ADD COLUMN elec_retail_area VARCHAR,
                        ADD COLUMN zip_new VARCHAR;'''
  cursor.execute(sql_add_columns)
  
  sql_update_cd116 = '''UPDATE ''' + schema+'.'+table + ''' SET cd116 = a.cd116,
                            cd116_name = a.cd116_name
                        FROM
                            (SELECT
                               nsd.acct_name AS acct_name,
                               districts.cd116fp AS cd116,
                               districts.namelsad as cd116_name
                             FROM tiger.congress_us_116 AS districts
                             JOIN markets.nsd_testing AS nsd
                             ON ST_Contains(districts.geom, nsd.geom)) as a;'''
  cursor.execute(sql_update_cd116)

  sql_update_statelower = '''UPDATE ''' + schema+'.'+table + ''' SET state_lower = a.sldlst
                        FROM
                            (SELECT
                               nsd.acct_name AS acct_name,
                               districts.sldlst AS sldlst,
                               districts.namelsad as namelsad
                             FROM tiger.state_lower_2018 AS districts
                             JOIN markets.nsd_testing AS nsd
                             ON ST_Contains(districts.geom, nsd.geom)) as a;'''
  cursor.execute(sql_update_statelower)

  sql_update_stateupper = '''UPDATE ''' + schema+'.'+table + ''' SET state_upper = a.sldust
                        FROM
                            (SELECT
                               nsd.acct_name AS acct_name,
                               districts.sldust AS sldust,
                               districts.namelsad as namelsad
                             FROM tiger.state_upper_2018 AS districts
                             JOIN markets.nsd_testing AS nsd
                             ON ST_Contains(districts.geom, nsd.geom)) as a;'''
  cursor.execute(sql_update_stateupper)

  #sql_update_stateupper = '''UPDATE ''' + schema+'.'+table + ''' SET county = a.name
  #                      FROM
  #                          (SELECT
  #                             nsd.acct_name AS acct_name,
  #                             counties.name AS name,
  #                             counties.namelsad as namelsad
  #                           FROM tiger.tl_2016_us_county AS counties
  #                           JOIN markets.nsd_testing AS nsd
  #                           ON ST_Contains(counties.geom, nsd.geom)) as a;'''
  #cursor.execute(sql_update_stateupper)

  connection.commit()
  cursor.close()
  return

def getEngine():
  address = 'postgresql://'+DB_USER+':'+DB_PASS+'@'+DB_URL+':'+DB_PORT+'/'+DB_NAME
  engine = sa.create_engine(address)
  return engine

def project_address_processing():
  engine = getEngine()
  raw_nsd = getNSD()
  geocoded_nsd = bing_api(raw_nsd)
  sql_processing(geocoded_nsd, engine, 'markets', 'project_testing', 'replace')
  return



if __name__ == '__main__':
  project_address_processing()
