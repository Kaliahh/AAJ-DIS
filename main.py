from typing import Type
from pygrametl.datasources import SQLSource, MergeJoiningSource
from pygrametl.tables import Dimension, CachedDimension, FactTable, TypeOneSlowlyChangingDimension
import pygrametl
import psycopg2 # pip install psycopg2-binary
import datetime


def main():
    con = psycopg2.connect(database="stregsystem", user="postgres", password="admin", host="127.0.0.1")

    categorySource  =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_category")
    salesSource     =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_sale")
    membersSource   =   SQLSource(connection=con, query="SELECT id, year, gender FROM stregsystem.stregsystem_member", names=('sourceid', 'year_created', 'gender'))
    roomSource      =   SQLSource(connection=con, query="SELECT name FROM stregsystem.stregsystem_room")

    productSource = SQLSource(connection=con, query=
    """SELECT 
        t1.pname AS product_name,
        t1.active AS is_active,
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

    # productCategorySource = SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_product_categories")
    # productRoomSource = SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_product_rooms")

    # productWithCategoryIntermediateSource = MergeJoiningSource(src1=productSource, src2=productCategorySource, 
    #                                                 key1="id", key2="product_id")

    # productWithCategorySource = MergeJoiningSource(src1=productWithCategoryIntermediateSource, key1="category_id", src2=categorySource, key2="id")

    dwconn = psycopg2.connect(database="fklubdw", user="postgres", password="admin", host="127.0.0.1")
    conn = pygrametl.ConnectionWrapper(connection=dwconn)

    # productDimension = Dimension(
    # name='product',
    # key='productid',
    # attributes=['name', 'category', 'price'],
    # lookupatts=['name'])

    # # Filling a dimension is simply done by using the insert method
    # for row in products:
    #     productDimension.insert(row)

    # # Ensures that the data is committed and the connection is closed correctly
    # conn.commit()
    # conn.close()

    productDimension = TypeOneSlowlyChangingDimension(
        name='product',
        key='productid',
        attributes=['product_type', 'category', 'subcategory', 'product_name', 'is_active', 'alcohol_content_ml'],
        lookupatts=['productid'],
        type1atts=['is_active']
    )

    memberDimension = Dimension(
        name='member',
        key='memberid',
        attributes=['year_created', 'gender', 'sourceid'],
        lookupatts=['memberid'],
        defaultidvalue = 1
    )

    roomDimension = Dimension(
        name='room',
        key='roomid',
        attributes=['name'],
        lookupatts=['roomid']
    )

    timeDimension = CachedDimension(
        name='time',
        key='timeid',
        attributes=['year', 'month', 'day', 'time_of_day', 'season', 'day_of_week', 'is_weekday', 'holiday', 'event'],
        lookupatts=['year', 'month', 'day', 'time_of_day']
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
        productDimension.insert(product)

    # Dict used for mapping datasource gender format to DW gender format
    genderDict = {
        'M': 'male',
        'F': 'female',
        'U': 'undefined'
    }
    for member in membersSource:
        # Map gender format using genderDict
        member['gender'] = genderDict[member['gender']]
        memberDimension.insert(member)

    #for room in roomSource:
    #    roomDimension.insert(room)

    # one line version of above code
    [roomDimension.insert(room) for room in roomSource]

    for sale in salesSource:
        time = extractTimeFromTimestamp(sale['timestamp'])
        timeDimension.ensure(time)
        sale['timeid'] = timeDimension.lookup(time)
        sale['productid'] = productDimension.lookup(sale, {'productid': 'product_id'})
        sale['memberid'] = memberDimension.lookup(sale, {'memberid': 'member_id'})
        sale['roomid'] = roomDimension.lookup(sale, {'roomid': 'room_id'})
        sale['unit_sales'] = 1
        salesFact.ensure(sale, False, {'kroner_sales': 'price'})

    conn.commit()
    conn.close()

    # We need a staging area for handling types, categories, and subcategories!!!

def categorizeCategory(product, category, categoryTypes):
    if category in categoryTypes['types']:
        product['product_type'] = category
    elif category in categoryTypes['categories']:
        product['category'] = category
    elif category in categoryTypes['subCategories']:
        product['subcategory'] = category

def extractTimeFromTimestamp(timestamp):
    seasonDict = {
        1: 'Winter',
        2: 'Spring',
        3: 'Summer',
        4: 'Fall'
    }

    time = dict()
    #attributes=['year', 'month', 'day', 'time_of_day', 'season', 'day_of_week', 'is_weekday', 'holiday', 'event']
    time['year'] = timestamp.year
    #time['month'] = timestamp.date.strftime("%B")
    time['month'] = timestamp.month
    time['day'] = timestamp.day
    time['time_of_day'] = extractTimeOfDay(timestamp.hour)
    time['season'] = seasonDict[timestamp.month%12 // 3 + 1]
    time['day_of_week'] = timestamp.strftime('%A')
    time['is_weekday'] = timestamp.weekday in range(0,5)
    time['holiday'] = 'Not a holiday'
    time['event'] = 'No event'

    return time

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