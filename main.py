from pygrametl.datasources import SQLSource, MergeJoiningSource
from pygrametl.tables import Dimension
import pygrametl
import psycopg2 # pip install psycopg2-binary


def main():
    con = psycopg2.connect(database="stregsystem", user="postgres", password="admin", host="127.0.0.1")

    categorySource  =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_category")
    salesSource     =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_sale")
    membersSource   =   SQLSource(connection=con, query="SELECT id, year, gender FROM stregsystem.stregsystem_member", names=('sourceid', 'year_created', 'gender'))
    roomSource      =   SQLSource(connection=con, query="SELECT name FROM stregsystem.stregsystem_room")

    productSource = SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_product")

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

    types = ['Drikke', 'Miscellaneous', 'Spiselige varer', 'Events']
    categories = ['Sodavand', 'Vitamin vand', 'Kaffe', 'Alkoholdige varer', 'Energidrik']
    subCategories = ['Øl', 'Special øl', 'Hård spiritus', 'Spiritus']

    

    productDimension = Dimension(
        name='product',
        key='productid',
        attributes=['product_type', 'category', 'subcategory', 'product_name', 'isActive', 'alcohol_content_ml'],
        lookupatts=['productid']
    )

    memberDimension = Dimension(
        name='member',
        key='memberid',
        attributes=['year_created', 'gender', 'sourceid'],
        lookupatts=['memberid']
    )

    roomDimension = Dimension(
        name='room',
        key='roomid',
        attributes=['name'],
        lookupatts=['roomid']
    )

    timeDimension = Dimension(
        name='time',
        key='timeid',
        attributes=['year', 'month', 'day', 'time_of_day', 'season', 'day_of_week', 'isWeekday', 'holiday', 'event']
    )

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
        timestamp = getTimestamp(sale['timestamp'])
        timeDimension.insert(timestamp)

    conn.commit()
    conn.close()

    # We need a staging area for handling types, categories, and subcategories!!!


if __name__ == "__main__":
    main()