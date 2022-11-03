import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');










def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category_id, int):
        return None


    sql = f"SELECT  f.title as title ,l.name as languge , c.name as category FROM film f" \
          f" INNER JOIN film_category fc ON fc.film_id=f.film_id" \
          f" INNER JOIN category c ON c.category_id=fc.category_id " \
          f" INNER JOIN language l ON f.language_id=l.language_id" \
          f" GROUP BY f.title,l.name, c.name,c.category_id"\
          f" HAVING c.category_id = {category_id}"\
          f" ORDER BY f.title,l.name "



    return pd.read_sql(sql, con=connection)

def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category_id, int):
        return None

    sql = f"SELECT category.name as category, COUNT(film_category.film_id) as count FROM film_category, category " \
          f"WHERE category.category_id={category_id} and category.category_id=film_category.category_id " \
          f"GROUP BY category.name"


    return pd.read_sql(sql, con=connection)

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not (isinstance(min_length, int,)or isinstance(min_length, float)):
        return None
    if not (isinstance(max_length, int) or isinstance(max_length, float)):
        return None
    if max_length < min_length:
        return None

    sql = f"SELECT film.length as length, COUNT(film.film_id)as count FROM film WHERE film.length BETWEEN {min_length} AND {max_length} GROUP BY film.length "


    return pd.read_sql(sql, con=connection)



def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(city,str):
        return None

    sql= f"SELECT  city.city as city,  customer.first_name as first_name, customer.last_name as last_name" \
         f" FROM city, address, customer " \
         f"WHERE city.city_id=address.city_id AND address.address_id=customer.address_id AND city.city IN ('{city}') " \
         f"GROUP BY city.city,customer.first_name,customer.last_name " \
         f"ORDER BY customer.last_name"
    return pd.read_sql(sql, con=connection)

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia
     filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość
    wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not (isinstance(length,float) or isinstance(length,int)):
        return None
    sql=f" SELECT f.length as length, AVG(p.amount) as avg FROM film f" \
        f" INNER JOIN inventory i ON f.film_id=i.film_id" \
        f" INNER JOIN rental r ON i.inventory_id=r.inventory_id" \
        f" INNER JOIN payment p ON p.rental_id=r.rental_id" \
        f" WHERE f.length={length} " \
        f" GROUP BY f.length "

    return pd.read_sql(sql, con=connection)

def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not (isinstance(sum_min,float) or isinstance(sum_min,int)):
        return None

    sql=f" SELECT c.first_name as first_name, c.last_name as last_name, SUM(f.length) as sum " \
        f" FROM customer c " \
        f" INNER JOIN rental r ON c.customer_id=r.customer_id " \
        f" INNER JOIN inventory i ON i.inventory_id=r.inventory_id " \
        f" INNER JOIN film f ON i.film_id=f.film_id " \
        f"  GROUP BY c.customer_id" \
        f" HAVING SUM(f.length)>{sum_min}" \
        f"ORDER BY SUM(f.length), c.last_name "



    return pd.read_sql(sql, con=connection)

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów
    w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(name,str) or name == ' ' :
        return None
    sql=f"SELECT c.name as category,AVG(f.length), SUM(f.length),MIN(f.length),MAX(f.length) FROM film f " \
               f" INNER JOIN film_category fc ON f.film_id =fc.film_id " \
               f" INNER JOIN category c ON c.category_id=fc.category_id " \
               f" GROUP BY c.name " \
               f" HAVING c.name ='{name}'" \


    return pd.read_sql(sql, con=connection)