from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension, CachedDimension, FactTable, TypeOneSlowlyChangingDimension
import pygrametl
import psycopg2 # pip install psycopg2-binary
from bs4 import BeautifulSoup # pip install beautifulsoup4


def main():
    con = psycopg2.connect(database="stregsystem", user="postgres", password="admin", host="127.0.0.1")

    membersSource   =   SQLSource(connection=con, query="SELECT id, year, gender FROM stregsystem.stregsystem_member", names=('sourceid', 'year_created', 'gender'))
    roomSource      =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_room")

    productSource = SQLSource(connection=con, query=
    """SELECT 
        t1.id,
        t1.pname AS product_name,
        t1.active,
        t1.alcohol_content_ml,
        max(CASE WHEN rn = 1 THEN t1.cname END) cat_01,
        max(CASE WHEN rn = 2 THEN t1.cname END) cat_02, 
        max(CASE WHEN rn = 3 THEN t1.cname END) cat_03
        FROM (
            select p.id, p.name AS pname, c.name AS cname, p.active, p.alcohol_content_ml ,Row_number() over(partition by p.id, p.name order by (select 1)) rn
            FROM stregsystem.stregsystem_product p, stregsystem.stregsystem_category c, stregsystem.stregsystem_product_categories pc
            WHERE p.id = pc.product_id
                AND c.id = pc.category_id
        ) t1
        GROUP BY t1.id, t1.pname, t1.active, t1.alcohol_content_ml""")

    salesSource = SQLSource(connection=con, query=
    """SELECT f.member_id,
                f.product_id,
                room_id,
                f.year,
                f.month,
                f.season,
                f.day,
                f.day_of_week,
                f.time_of_day,
                ROUND((SUM(f.price)::float / 100)::numeric, 2) AS kroner_sales,
                COUNT(*) AS unit_sales
        FROM (SELECT id, member_id, product_id, room_id, price,
                DATE_PART('year',timestamp) AS year, DATE_PART('month',timestamp) AS month, DATE_PART('day',timestamp) AS day, DATE_PART('hour',timestamp) AS hour,
                CASE
                WHEN DATE_PART('hour',timestamp) IN (6, 7, 8, 9, 10) THEN 'Morning'
                WHEN DATE_PART('hour',timestamp) IN (11, 12, 13) THEN 'Noon'
                WHEN DATE_PART('hour',timestamp) IN (13, 14, 15, 16) THEN 'Afternoon'
                ELSE 'Night'
                END AS time_of_day,
                to_char(s.timestamp, 'Day') AS day_of_week,
                CASE
                WHEN DATE_PART('month',timestamp) IN (12, 1, 2) THEN 'Winter'
                WHEN DATE_PART('month',timestamp) IN (3, 4, 5) THEN 'Spring'
                WHEN DATE_PART('month',timestamp) IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
                END as season
                FROM stregsystem.stregsystem_sale s) f
        GROUP BY f.member_id, f.product_id, room_id, f.year, f.month, f.season, f.day, f.day_of_week, f.time_of_day""")

    dwconn = psycopg2.connect(database="fklubdw", user="postgres", password="admin", host="127.0.0.1")
    conn = pygrametl.ConnectionWrapper(connection=dwconn)

    productDimension = TypeOneSlowlyChangingDimension(
        name='product',
        key='productid',
        attributes=['product_type', 'category', 'subcategory', 'product_name', 'status', 'alcohol_content_ml'],
        lookupatts=['productid'],
        type1atts=['status']
    )

    memberDimension = Dimension(
        name='member',
        key='memberid',
        attributes=['year_created', 'gender', 'sourceid'],
        lookupatts=['sourceid'],
        defaultidvalue = 1
    )

    roomDimension = Dimension(
        name='room',
        key='roomid',
        attributes=['name'],
        lookupatts=['roomid'],
        defaultidvalue = 1
    )

    timeDimension = CachedDimension(
        name='time',
        key='timeid',
        attributes=['year', 'month', 'day', 'time_of_day', 'season', 'day_of_week', 'is_weekday', 'holiday', 'event'],
        lookupatts=['year', 'month', 'day', 'time_of_day'],
    )

    salesFact = FactTable(
        name='salesfact',
        keyrefs=['memberid', 'productid', 'timeid', 'roomid'],
        measures=['unit_sales', 'kroner_sales']
    )

    categoryDict = {
        'types': ['Drikke', 'Miscellaneous', 'Spiselige varer', 'Events'],
        'categories': ['Sodavand', 'Vitamin vand', 'Kaffe', 'Alkoholdie varer', 'Energidrik'],
        'subCategories': ['Øl', 'Special øl', 'Hård spiritus', 'Spiritus']
    }

    productMappingDict = {}

    for product in productSource:
        product['product_type'] = None
        product['category'] = None
        product['subcategory'] = None
        categorizeCategory(product, product['cat_01'], categoryDict)
        categorizeCategory(product, product['cat_02'], categoryDict)
        categorizeCategory(product, product['cat_03'], categoryDict)
        product['status'] = 'active' if product['active'] == True else 'inactive'
        product['product_name'] = BeautifulSoup(product['product_name'], features="html.parser").text
        dwkey = productDimension.insert(product)
        productMappingDict[product['id']] = dwkey

    # Dict used for mapping datasource gender format to DW gender format
    genderDict = {
        'M': 'Male',
        'F': 'Female',
        'U': 'Undefined'
    }
    for member in membersSource:
        # Map gender format using genderDict
        member['gender'] = genderDict[member['gender']]
        memberDimension.insert(member)

    roomMappingDict = {}

    for room in roomSource:
        dwkey = roomDimension.insert(room)
        roomMappingDict[room['id']] = dwkey

    for sale in salesSource:
        time = extractTimeFromSale(sale)

        sale['timeid'] = timeDimension.ensure(time)
        sale['productid'] = productMappingDict[sale['product_id']]
        sale['memberid'] = memberDimension.lookup(sale, {'sourceid': 'member_id'})
        sale['roomid'] = roomMappingDict[sale['room_id']]
        salesFact.insert(sale)

    conn.commit()
    conn.close()

def categorizeCategory(product, category, categoryTypes):
    if category in categoryTypes['types']:
        product['product_type'] = category
    elif category in categoryTypes['categories']:
        product['category'] = category
    elif category in categoryTypes['subCategories']:
        product['subcategory'] = category

def extractTimeFromSale(sale):
    sale['is_weekday'] = sale['day_of_week'] in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
    sale['holiday'] = 'Not a holiday'
    sale['event'] = 'No event'

    return sale

def extractTimeOfDay(hour : int):
    if hour in range(6, 11):
        return 'Morning'
    elif hour in range(11, 13):
        return 'Noon'
    elif hour in range(13, 17):
        return 'Afternoon'
    else:
        return 'Night'

if __name__ == "__main__":
    main()