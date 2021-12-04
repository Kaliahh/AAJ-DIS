from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension, CachedDimension, FactTable, TypeOneSlowlyChangingDimension
import pygrametl
import psycopg2 # pip install psycopg2-binary
from bs4 import BeautifulSoup # pip install beautifulsoup4


def main():
    con = psycopg2.connect(database="stregsystem", user="postgres", password="admin", host="127.0.0.1")

    membersSource   =   SQLSource(connection=con, query="SELECT gender, active FROM stregsystem.stregsystem_member", names=('gender', 'is_active'))
    roomSource      =   SQLSource(connection=con, query="SELECT name AS room_name FROM stregsystem.stregsystem_room")

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
    """SELECT 
                f.year,
                f.month,
                f.season,
                f.day,
                f.day_of_week,
                f.time_of_day,
				p.name AS product_name,
				r.name AS room_name,
				m.gender,
				m.active as is_active,
                ROUND((SUM(f.price)::float / 100)::numeric, 2) AS kroner_sales,
                COUNT(*) AS unit_sales
        FROM (SELECT s.id, member_id, product_id, room_id, s.price,
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
        JOIN stregsystem.stregsystem_product AS p ON p.id = f.product_id
		JOIN stregsystem.stregsystem_room AS r ON r.id = f.room_id
		JOIN stregsystem.stregsystem_member AS m ON m.id = f.member_id
        GROUP BY m.active, m.gender, p.name, r.name, f.year, f.season, f.month, f.day, f.day_of_week, f.time_of_day""")

    dwconn = psycopg2.connect(database="fklubdw", user="postgres", password="admin", host="127.0.0.1")
    conn = pygrametl.ConnectionWrapper(connection=dwconn)

    productDimension = TypeOneSlowlyChangingDimension(
        name='product',
        key='productid',
        attributes=['product_type', 'category', 'subcategory', 'product_name', 'status', 'alcohol_content_ml'],
        lookupatts=['product_name'],
        type1atts=['status']
    )

    memberDimension = Dimension(
        name='member',
        key='memberid',
        attributes=['gender', 'is_active'],
        lookupatts=['gender', 'is_active'],
        defaultidvalue = 1
    )

    roomDimension = Dimension(
        name='room',
        key='roomid',
        attributes=['room_name'],
        lookupatts=['room_name'],
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

    for product in productSource:
        product['product_type'] = None
        product['category'] = None
        product['subcategory'] = None
        categorizeCategory(product, product['cat_01'], categoryDict)
        categorizeCategory(product, product['cat_02'], categoryDict)
        categorizeCategory(product, product['cat_03'], categoryDict)
        addDefaultCategories(product)
        product['status'] = 'active' if product['active'] == True else 'inactive'
        product['product_name'] = BeautifulSoup(product['product_name'], features="html.parser").text
        productDimension.insert(product)

    # Dict used for mapping datasource gender format to DW gender format
    genderDict = {
        'M': 'Male',
        'F': 'Female',
        'U': 'Undefined'
    }
    for member in membersSource:
        # Map gender format using genderDict
        member['gender'] = genderDict[member['gender']]
        memberDimension.ensure(member)

    for room in roomSource:
        roomDimension.insert(room)

    for sale in salesSource:
        time = extractTimeFromSale(sale)

        # Product lookup skal være navn. Join sales med product table

        # Room lookup skal være navn

        # Vi er faktisk ikke intereserede i de enkelte brugere, vi kan nøjes med gender og is_active (Det giver 3 kategorier)

        sale['timeid'] = timeDimension.ensure(time)
        # sale['productid'] = productMappingDict[sale['product_id']]
        sale['product_name'] = BeautifulSoup(sale['product_name'], features="html.parser").text
        sale['productid'] = productDimension.lookup(sale)
        sale['gender'] = genderDict[sale['gender']]
        sale['memberid'] = memberDimension.lookup(sale)
        # sale['roomid'] = roomMappingDict[sale['room_id']]
        sale['roomid'] = roomDimension.lookup(sale)
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

defaultCategories = [('product_type', 'No product_type'), ('category', 'No category'), ('subcategory', 'No subcategory')]

def addDefaultCategories(product):
    for item in defaultCategories:
        if product[fst(item)] is None:
            product[fst(item)] = snd(item)

def extractTimeFromSale(sale):
    sale['is_weekday'] = 'Yes' if sale['day_of_week'] in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'] else 'No'
    sale['holiday'] = 'Not a holiday'
    sale['event'] = 'No event'

    return sale

def fst(tuple):
    return tuple[0]

def snd(tuple):
    return tuple[1]

if __name__ == "__main__":
    main()