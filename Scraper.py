from bs4 import BeautifulSoup
import requests
import time
import csv
from itertools import zip_longest
import pandas as pd
import mysql.connector
from mysql.connector import Error


titres = []
Dates = []
types = []
liens = []


def Article_Cnbc():
    ArticleNumber = 0
    for x in range( 1, 8):

        url = 'https://www.cnbc.com/economy/?page='
        r = requests.get( url + str( x ) )
        soup = BeautifulSoup( r.content, 'html.parser' )

        tis = soup.find_all( 'div', class_='Card-titleAndFooter' )

        for ti in tis:
            titre = ti.find_all( 'div', class_='Card-titleContainer' )
            lien = ti.find( 'a', class_='Card-title' ).attrs['href']

            date = ti.find_all( 'span', class_='Card-time' )

            typs = ti.find_all( 'div', class_='Card-eyebrowContainer' )
            for typ in typs:
                type = typ.find_all( 'a', class_='Card-eyebrow' )

                ArticleNumber = ArticleNumber + 1
                for i in range( len( titre ) ):
                    titres.append( titre[i].text.strip() )
                    Dates.append( date[i].text.strip() )
                    types.append( type[i].text.strip() )
                    liens.append( lien )

    print( titres, Dates, types, liens )
    file_list = [titres, Dates, types, liens]
    exported = zip_longest( *file_list )
    with open( "F:\projet\cnbc1.csv", "w", encoding='UTF8' ) as file:
        wr = csv.writer( file )
        wr.writerow( ["title", " Date_publication", "type", "Read more..."] )
        wr.writerows( exported )
        wait = 3
        time.sleep( wait )
        if x < 2:
            print( f'waiting for {wait}  seconds........'"\n" )
        else:
            print( " ******* " f'{ArticleNumber} article was returned successfully " ******* " ' )


Article_Cnbc()

Data = pd.read_csv( 'F:\projet\cnbc1.csv', index_col=False )
Data.head()

try:
    connection = mysql.connector.connect( host='127.0.0.1',
                                          database='data',
                                          user='root',
                                          password='arezki123' )
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print( "Connected to MySQL Server version ", db_Info )
        cursor = connection.cursor()
        cursor.execute( "select database();" )
        record = cursor.fetchone()
        print( "You're connected to database: ", record )

except Error as e:
    print( "Error while connecting to MySQL", e )

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print( "MySQL connection is closed" )

try:
    conn = mysql.connector.connect( host='127.0.0.1',
                                    database='data', user='root',
                                    password='arezki123' )
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute( "select database();" )
        record = cursor.fetchone()
        print( "You're connected to database: ", record )
        cursor.execute( 'DROP TABLE IF EXISTS article_cnbc;' )
        print( 'Creating table....' )

        cursor.execute( "CREATE TABLE data.article_cnbc("
                        "Title VARCHAR(150) NOT NULL, "
                        "Date VARCHAR(20) NOT NULL, "
                        "Type VARCHAR(50) NOT NULL, "
                        "Link VARCHAR(300) NOT NULL )" )
        print( "Article table is created...." )

        for i, row in Data.iterrows():
            sql = "INSERT INTO data.article_cnbc VALUES (%s,%s,%s,%s)"
            cursor.execute( sql, tuple( row ) )
            print( "Record inserted" )
            conn.commit()
        print( " ******* " f'{i + 1} Article was saved in Database' " ******* " )
except Error as e:
    print( "Error while connecting to MySQL", e )