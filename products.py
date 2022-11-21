import concurrent.futures
from multiprocessing import Process
import mysql.connector
import requests
from bs4 import BeautifulSoup

mydb = mysql.connector.connect(
    host="localhost",
    user="egeria_admin",
    password="innovation@1995",
    database="egeria_db"
)
mycursor = mydb.cursor(buffered=True)
s = "CREATE TABLE IF NOT EXISTS Products(id INT AUTO_INCREMENT PRIMARY KEY,website_id int,product_title_name TEXT," \
    "country TEXT,language TEXT,general_currency TEXT, "
s = s + "product_meta_description TEXT, product_share_title TEXT,"
s = s + "product_share_description TEXT,product_web_sku TEXT, product_brand TEXT,"
s = s + "product_retailer_item_id TEXT,product_currency TEXT,product_share_image_1 TEXT,"
s = s + "product_price TEXT,productName TEXT,Product TEXT,Product_ID TEXT,ProductReference TEXT,"
s = s + "brand TEXT,brandID TEXT,Product_categories TEXT,sellingPrice TEXT,EAN TEXT,promotionID TEXT)"
mycursor.execute(s)
mydb.commit()


def main_metro():
    def metro(x):
        mydb = mysql.connector.connect(
            host="localhost",
            user="egeria_admin",
            password="innovation@1995",
            database="egeria_db"
        )
        mycursor = mydb.cursor(buffered=True)
        sql = "INSERT INTO Products"
        sql = sql + "(website_id,product_title_name,country,language,general_currency,product_meta_description," \
                    "product_share_title,product_share_description,product_web_sku," \
                    "product_brand,product_retailer_item_id,product_currency,product_share_image_1," \
                    "product_price,productName,Product,Product_ID," \
                    "ProductReference,brand,brandID,Product_categories,sellingPrice,EAN,promotionID) VALUES( %s, %s, %s, " \
                    "%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s) "
        response = requests.get(f"https://www.metro.pe/api/catalog_system/pub/products/search?fq=productId:{x}&sc=19")
        try:
            q = ""
            data = response.json()
            p_link = data[0]["link"]

            r = requests.get(url=p_link)
            soup = BeautifulSoup(r.text, 'html.parser')
            id = 1
            product_title_name = data[0]["productName"]
            country = [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "country"][
                0]
            language = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "language"][0]
            general_currency = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "currency"][0]
            product_meta_description = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "description"][0]
            product_share_title = data[0]["productTitle"]
            product_share_description = data[0]["description"]
            product_web_sku = data[0]["productId"]
            product_brand = data[0]["brand"]
            product_retailer_item_id = data[0]["productReference"]
            product_currency = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "currency"][0]
            product_share_image_1 = data[0]["items"][0]["images"][0]['imageUrl']
            product_price = data[0]["items"][0]["sellers"][0]['commertialOffer']['Price']
            productName = soup.find("title").get_text()
            Product = data[0]["linkText"]
            Product_ID = data[0]["productId"]
            ProductReference = data[0]["productReference"]
            brand = data[0]["brand"]
            brandID = data[0]["brandId"]
            Product_categories = data[0]["categories"][0]
            sellingPrice = data[0]["items"][0]["sellers"][0]['commertialOffer']['ListPrice']
            EAN = data[0]["items"][0]["ean"]
            promotionID = data[0]["productClusters"].keys()
            for i in promotionID:
                q += str(i) + ","
        except:
            pass
        else:
            p_data = (id,
                      product_title_name,
                      country,
                      language,
                      general_currency,
                      product_meta_description,
                      product_share_title,
                      product_share_description,
                      product_web_sku,
                      product_brand,
                      product_retailer_item_id,
                      product_currency,
                      product_share_image_1,
                      product_price,
                      productName,
                      Product,
                      Product_ID,
                      ProductReference,
                      brand,
                      brandID,
                      Product_categories,
                      sellingPrice,
                      EAN,
                      q)

            mycursor.execute(sql, p_data)
            mydb.commit()
            print(mycursor.rowcount, "lines were inserted.")

    with concurrent.futures.ThreadPoolExecutor(32) as e:
        e.map(metro, range(1, 500000))
        # ---------------------vivanda-----------------------


def main_vivanda():
    def vivanda(x):
        mydb = mysql.connector.connect(
            host="localhost",
            user="egeria_admin",
            password="innovation@1995",
            database="egeria_db"
        )
        mycursor = mydb.cursor(buffered=True)
        sql = "INSERT INTO Products"
        sql = sql + "(website_id,product_title_name,country,language,general_currency,product_meta_description," \
                    "product_share_title,product_share_description,product_web_sku," \
                    "product_brand,product_retailer_item_id,product_currency,product_share_image_1," \
                    "product_price,productName,Product,Product_ID," \
                    "ProductReference,brand,brandID,Product_categories,sellingPrice,EAN,promotionID) VALUES( %s, %s, %s, " \
                    "%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s) "

        response = requests.get(f"https://www.vivanda.com.pe/api/catalog_system/pub/products/search?fq=productId:{x}")
        try:
            q = ""
            data = response.json()
            p_link = data[0]["link"]

            r = requests.get(url=p_link)
            soup = BeautifulSoup(r.text, 'html.parser')
            id = 2
            product_title_name = data[0]["productName"]
            country = [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "country"][
                0]
            language = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "language"][0]
            general_currency = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "currency"][0]
            product_meta_description = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "description"][0]
            product_share_title = data[0]["productTitle"]
            product_share_description = data[0]["description"]
            product_web_sku = data[0]["productId"]
            product_brand = soup.find(property="product:brand").get("content")
            product_retailer_item_id = data[0]["productReference"]
            product_currency = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "currency"][0]
            product_share_image_1 = data[0]["items"][0]["images"][0]['imageUrl']
            product_price = data[0]["items"][0]["sellers"][0]['commertialOffer']['Price']
            productName = soup.find("title").get_text()
            Product = data[0]["linkText"]
            Product_ID = data[0]["productId"]
            ProductReference = data[0]["productReference"]
            brand = data[0]["brand"]
            brandID = data[0]["brandId"]
            Product_categories = data[0]["categories"][0]
            sellingPrice = data[0]["items"][0]["sellers"][0]['commertialOffer']['ListPrice']
            EAN = data[0]["items"][0]["ean"]
            promotionID = data[0]["productClusters"].keys()
            for i in promotionID:
                q += str(i) + ","
        except:
            pass
        else:
            p_data = (id,
                      product_title_name,
                      country,
                      language,
                      general_currency,
                      product_meta_description,
                      product_share_title,
                      product_share_description,
                      product_web_sku,
                      product_brand,
                      product_retailer_item_id,
                      product_currency,
                      product_share_image_1,
                      product_price,
                      productName,
                      Product,
                      Product_ID,
                      ProductReference,
                      brand,
                      brandID,
                      Product_categories,
                      sellingPrice,
                      EAN,
                      q)

            mycursor.execute(sql, p_data)
            mydb.commit()
            print(mycursor.rowcount, "lines were inserted.")

    with concurrent.futures.ThreadPoolExecutor(32) as e:
        e.map(vivanda, range(1, 500000))
    # ---------------wong.pe-----------------------------


def main_wong():
    def wong(x):
        mydb = mysql.connector.connect(
            host="localhost",
            user="egeria_admin",
            password="innovation@1995",
            database="egeria_db"
        )
        mycursor = mydb.cursor(buffered=True)
        sql = "INSERT INTO Products"
        sql = sql + "(website_id,product_title_name,country,language,general_currency,product_meta_description," \
                    "product_share_title,product_share_description,product_web_sku," \
                    "product_brand,product_retailer_item_id,product_currency,product_share_image_1," \
                    "product_price,productName,Product,Product_ID," \
                    "ProductReference,brand,brandID,Product_categories,sellingPrice,EAN,promotionID) VALUES( %s, %s, %s, " \
                    "%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s) "

        response = requests.get(f"https://www.wong.pe/api/catalog_system/pub/products/search?sc=70&fq=productId:{x}")
        try:
            q = ""
            data = response.json()
            p_link = data[0]["link"]
            r = requests.get(url=p_link)
            soup = BeautifulSoup(r.text, 'html.parser')
            id = 3
            product_title_name = soup.find("title").get_text()
            country = [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "country"][
                0]
            language = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "language"][0]
            general_currency = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "currency"][0]
            product_meta_description = soup.find(property="og:description").get("content")

            product_share_title = data[0]["productTitle"]
            product_share_description = soup.find(property="og:description").get("content")
            product_web_sku = soup.find(property="product:sku").get("content")

            product_brand = soup.find(property="product:brand").get("content")
            product_retailer_item_id = soup.find(property="product:retailer_item_id").get("content")
            product_currency = soup.find(property="product:price:currency").get("content")
            product_share_image_1 = soup.find(property="og:image").get("content")

            product_price = data[0]["items"][0]["sellers"][0]['commertialOffer']['Installments'][0]['Value']
            productName = soup.find("title").get_text()
            Product = data[0]["linkText"]
            Product_ID = data[0]["productId"]
            ProductReference = data[0]["productReference"]
            brand = data[0]["brand"]
            brandID = data[0]["brandId"]
            Product_categories = data[0]["categories"][0]
            sellingPrice = data[0]["items"][0]["sellers"][0]['commertialOffer']['Installments'][0]['Value']
            EAN = data[0]["items"][0]["ean"]
            promotionID = data[0]["productClusters"].keys()
            for i in promotionID:
                q += str(i) + ","
        except:
            pass
        else:
            p_data = (id,
                      product_title_name,
                      country,
                      language,
                      general_currency,
                      product_meta_description,
                      product_share_title,
                      product_share_description,
                      product_web_sku,
                      product_brand,
                      product_retailer_item_id,
                      product_currency,
                      product_share_image_1,
                      product_price,
                      productName,
                      Product,
                      Product_ID,
                      ProductReference,
                      brand,
                      brandID,
                      Product_categories,
                      sellingPrice,
                      EAN,
                      q,

                      )

            mycursor.execute(sql, p_data)
            mydb.commit()
            print(mycursor.rowcount, "lines were inserted.")

    with concurrent.futures.ThreadPoolExecutor(32) as e:
        e.map(wong, range(1, 500000))
    # ----------------------Plazavea-------------------------


def main_Plazavea():
    def plazavea(x):
        mydb = mysql.connector.connect(
            host="localhost",
            user="egeria_admin",
            password="innovation@1995",
            database="egeria_db"
        )
        mycursor = mydb.cursor(buffered=True)
        sql = "INSERT INTO Products"
        sql = sql + "(website_id,product_title_name,country,language,general_currency,product_meta_description," \
                    "product_share_title,product_share_description,product_web_sku," \
                    "product_brand,product_retailer_item_id,product_currency,product_share_image_1," \
                    "product_price,productName,Product,Product_ID," \
                    "ProductReference,brand,brandID,Product_categories,sellingPrice,EAN,promotionID) VALUES( %s, %s, %s, " \
                    "%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s) "

        response = requests.get(f"https://www.plazavea.com.pe/api/catalog_system/pub/products/search?&fq=productId:{x}")
        try:

            q = ""
            data = response.json()
            p_link = data[0]["link"]
            r = requests.get(url=p_link)
            soup = BeautifulSoup(r.text, 'html.parser')
            id = 4
            product_title_name = data[0]["productName"]
            country = [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "country"][
                0]
            language = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "language"][0]
            general_currency = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "currency"][0]
            product_meta_description = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "description"][0]
            product_share_title = data[0]["productTitle"]
            product_share_description = data[0]["metaTagDescription"]
            product_web_sku = data[0]["productId"]
            product_brand = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "author"][0]
            product_retailer_item_id = data[0]["productReference"]
            product_currency = \
                [tag.get("content", None) for tag in soup.find_all("meta") if tag.get("name", None) == "currency"][0]
            product_share_image_1 = data[0]["items"][0]["images"][0]['imageUrl']
            product_price = data[0]["items"][0]["sellers"][0]['commertialOffer']['Installments'][0][
                'Value']
            productName = soup.find("title").get_text()
            Product = data[0]["linkText"]
            Product_ID = data[0]["productId"]
            ProductReference = data[0]["productReference"]
            brand = data[0]["brand"]
            brandID = data[0]["brandId"]
            Product_categories = data[0]["categories"][0]
            sellingPrice = data[0]["items"][0]["sellers"][0]['commertialOffer']['Installments'][0]['Value']
            EAN = data[0]["items"][0]["ean"]
            promotionID = data[0]["productClusters"].keys()
            for i in promotionID:
                q += str(i) + ","
        except:
            pass
        else:
            p_data = (id,
                      product_title_name,
                      country,
                      language,
                      general_currency,
                      product_meta_description,
                      product_share_title,
                      product_share_description,
                      product_web_sku,
                      product_brand,
                      product_retailer_item_id,
                      product_currency,
                      product_share_image_1,
                      product_price,
                      productName,
                      Product,
                      Product_ID,
                      ProductReference,
                      brand,
                      brandID,
                      Product_categories,
                      sellingPrice,
                      EAN,
                      q)
            mycursor.execute(sql, p_data)
            mydb.commit()
            print(mycursor.rowcount, "lines were inserted.")

    with concurrent.futures.ThreadPoolExecutor(32) as e:
        e.map(plazavea, range(1, 500000))


# -----------------------------ripley-------------------

def main_ripley():
    def ripley(x):
        mydb = mysql.connector.connect(
            host="localhost",
            user="egeria_admin",
            password="innovation@1995",
            database="egeria_db"
        )
        mycursor = mydb.cursor(buffered=True)
        sql = "INSERT INTO Products"
        sql = sql + "(website_id,product_title_name,country,language,general_currency,product_meta_description," \
                    "product_share_title,product_share_description,product_web_sku," \
                    "product_brand,product_retailer_item_id,product_currency,product_share_image_1," \
                    "product_price,productName,Product,Product_ID," \
                    "ProductReference,brand,brandID,Product_categories,sellingPrice,EAN,promotionID) VALUES( %s, %s, %s, " \
                    "%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s) "

        response = requests.get(f"https://api-pe.ripley.com/marketplace/ecommerce/search/v1/pe/products/by-sku/{x}")
        try:
            data = response.json()

            id = 5
            product_title_name = data["name"]
            country = "PER"
            language = "ES-PE"
            general_currency = data["parentpricestock"]["price"]['master']['currency']
            product_meta_description = data["shortDescription"]
            product_share_title = data["title"]
            product_share_description = data["longDescription"]
            product_web_sku = None
            product_brand = data["manufacturer"]
            product_retailer_item_id = data["partNumber"]
            product_currency = general_currency
            product_share_image_1 = data["fullImage"]
            product_price = data["parentpricestock"]["price"]['master']["value"]
            productName = product_title_name
            Product = data["productType"]
            Product_ID = data["parentProductID"]
            ProductReference = None
            brand = product_brand
            brandID = data["storeID"]
            Product_categories = data["parentCategoryId"]
            sellingPrice = product_price
            EAN = None
            promotionID = None
        except:
            pass
        else:
            p_data = (id,
                      product_title_name,
                      country,
                      language,
                      general_currency,
                      product_meta_description,
                      product_share_title,
                      product_share_description,
                      product_web_sku,
                      product_brand,
                      product_retailer_item_id,
                      product_currency,
                      product_share_image_1,
                      product_price,
                      productName,
                      Product,
                      Product_ID,
                      ProductReference,
                      brand,
                      brandID,
                      Product_categories,
                      sellingPrice,
                      EAN,
                      promotionID)
            mycursor.execute(sql, p_data)
            mydb.commit()
            print(mycursor.rowcount, "lines were inserted.")

    with concurrent.futures.ThreadPoolExecutor(32) as e:
        e.map(ripley, range(1, 500000))


m = Process(target=main_metro)
v = Process(target=main_vivanda)
w = Process(target=main_wong)
p = Process(target=main_Plazavea)
m.start()
v.start()
w.start()
p.start()
