from pygrametl.datasources import SQLSource, MergeJoiningSource
from pygrametl.tables import Dimension
import pygrametl
import psycopg2 # pip install psycopg2-binary


def main():
    con = psycopg2.connect(database="stregsystem", user="dwuser", password="12345", host="127.0.0.1")

    categorySource  =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_category")
    salesSource     =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_sale")
    membersSource   =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_member")
    roomSource      =   SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_room")

    productSource = SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_product")

    # productCategorySource = SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_product_categories")
    # productRoomSource = SQLSource(connection=con, query="SELECT * FROM stregsystem.stregsystem_product_rooms")

    # productWithCategoryIntermediateSource = MergeJoiningSource(src1=productSource, src2=productCategorySource, 
    #                                                 key1="id", key2="product_id")

    # productWithCategorySource = MergeJoiningSource(src1=productWithCategoryIntermediateSource, key1="category_id", src2=categorySource, key2="id")

    dwconn = psycopg2.connect(database="fklubdw", user="dwuser", password="12345", host="127.0.0.1")
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



    productDimension = Dimension(
        name='product',
        key='productid',
        attributes=['product_type', 'category', 'subcategory', 'product_name', 'isActive', 'alcohol_pct'],
        lookupatts=['productid']
    )

    for row in productSource:
        productDimension.insert(row)

    a = 0

    # We need a staging area for handling types, categories, and subcategories!!!



if __name__ == "__main__":
    main()