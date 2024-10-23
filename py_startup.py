# this file is a kind of python startup module used for manual unit testing
#from util.db_connection import Db_Connection
from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
from extract.per_staging import persistir_staging
from extract.ext_addresses import extraer_addresses
from extract.ext_cities import extraer_cities
from transform.tra_stores import transformar_stores
from load.load_stores import cargar_stores
from extract.ext_dates import extraer_dates
from extract.ext_films import extraer_films
from transform.tra_films import transformar_films
from extract.ext_inventories import extraer_inventories
from load.load_films import cargar_films
from transform.tra_dates import transformar_dates
from load.load_dates import cargar_dates
from transform.tra_inventories import transformar_inventory
from load.load_inventories import cargar_inventory

import traceback
import pandas as pd


try:
    con_db = Db_Connection('mysql','localhost','3306','root','123123','oltp')
    ses_db = con_db.start()
    if ses_db == -1:
       raise Exception("El tipo de base de datos dado no es válido")
    elif ses_db == -2:
       raise Exception("Error tratando de conectarse a la base de datos ")
    
    databases = pd.read_sql ('SELECT COUNT(*) FROM oltp.customer', ses_db)
    print (databases)
    
    print("Extrayendo datos de countries desde un csv")
    countries = extraer_countries()
    
    print("Guardando en staging datos de countries")
    persistir_staging(countries, 'ext_country')
    
    
    print("Extrayendo datos de stores desde uana db")
    stores = extraer_stores()
    #print(stores)
    print("Guardando en staging datos de stores")
    persistir_staging(stores, 'ext_store')
    
    print("Transformando datos de store en el staging")
    tra_stores= transformar_stores()
    #print (tra_stores)
    print("Persistiendo en stagging datos transformados de stores")
    persistir_staging(tra_stores, 'tra_store')
    
    
    print("Cargando datos de stores en sor")
    cargar_stores()
    
    #############################
    
    
    print("Extrayendo datos de address desde una db")
    addresses= extraer_addresses() 
    #print(addresses)
    print("Guardando en staging datos de addrress")
    persistir_staging(addresses, 'ext_address')
    
    
    print("Extrayendo datos de cities desde una db")
    cities= extraer_cities() 
    #print(cities)
    print("Guardando en staging datos de cities")
    persistir_staging(cities, 'ext_city')
    
 
    

    
    
    print("Extrayendo datos de dates desde un CSV")
    dates = extraer_dates()
    print(dates)
    print("Guardando en staging datos de dates")
    persistir_staging(dates, 'ext_date')
    
    dates_transformed = transformar_dates()
    persistir_staging(dates_transformed, 'tra_dates')
    cargar_dates()
    
    
    print("Extrayendo datos de films desde la base de datos OLTP")
    films = extraer_films()
    print("Guardando en Staging los datos extraídos de films")
    persistir_staging(films, 'ext_film')
    
    print("Transformando datos de films en Staging")
    films_transformed = transformar_films()
    print("Guardando los datos transformados en tra_films")
    persistir_staging(films_transformed, 'tra_films')
    cargar_films()
    
    
    

    print("Extrayendo datos de inventory desde la base de datos OLTP")
    inventories = extraer_inventories()
    print("Guardando en Staging los datos extraídos de inventory")
    persistir_staging(inventories, 'ext_inventory')

    print("Transformando datos de inventory en el staging")
    tra_inventories = transformar_inventory()
    persistir_staging(tra_inventories, 'tra_inventory')

    print("Cargando datos de inventory en SOR")
    cargar_inventory()

    
except:
    traceback.print_exc()
finally:
    None
    #ses_db_oltp = con_db.stop()