from datetime import timedelta
from datetime import date
import requests
import mysql.connector
from bs4 import BeautifulSoup


def products():
    mydb = mysql.connector.connect(
        host="localhost",
        user="egeria_admin",
        password="innovation@1995",
        database="egeria_db"
    )
    mycursor = mydb.cursor(buffered=True)

    a = "CREATE TABLE IF NOT EXISTS WEBSITES(id INT AUTO_INCREMENT PRIMARY KEY,Websites TEXT)"
    mycursor.execute(a)
    mydb.commit()

    web_list = ["https://www.metro.pe/", "https://www.vivanda.com.pe/", "https://www.wong.pe/",
                "https://www.plazavea.com.pe/"]
    for web in web_list:
        s = "INSERT INTO WEBSITES(Websites) VALUES( %s)"
        data = (web,)
        mycursor.execute(s, data)
        mydb.commit()


    s = "CREATE TABLE IF NOT EXISTS Products(id INT AUTO_INCREMENT PRIMARY KEY,website_id int,FOREIGN KEY (website_id) REFERENCES WEBSITES(id),product_title_name TEXT,country TEXT,language TEXT,general_currency TEXT, "
    s = s + "product_meta_description TEXT, product_share_title TEXT,"
    s = s + "product_share_description TEXT,product_web_sku TEXT, product_brand TEXT,"
    s = s + "product_retailer_item_id TEXT,product_currency TEXT,product_share_image_1 TEXT,"
    s = s + "product_price TEXT,productName TEXT,Product TEXT,Product_ID TEXT,ProductReference TEXT,"
    s = s + "brand TEXT,brandID TEXT,Product_categories TEXT,sellingPrice TEXT,EAN TEXT,promotionID TEXT)"
    mycursor.execute(s)
    mydb.commit()

    sql = "INSERT INTO Products"
    sql = sql + "(website_id,product_title_name,country,language,general_currency,product_meta_description," \
                "product_share_title,product_share_description,product_web_sku," \
                "product_brand,product_retailer_item_id,product_currency,product_share_image_1," \
                "product_price,productName,Product,Product_ID," \
                "ProductReference,brand,brandID,Product_categories,sellingPrice,EAN,promotionID) VALUES( %s, %s, %s, " \
                "%s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s) "
    for x in range(1, 100000):
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

        # ---------------------vivanda-----------------------

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

        # ---------------wong.pe-----------------------------

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

        # ----------------------Plazavea-------------------------

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


def track():
    mydb = mysql.connector.connect(
        host="localhost",
        user="egeria_admin",
        password="innovation@1995",
        database="egeria_db"
    )
    mycursor = mydb.cursor(buffered=True)

    a = "CREATE TABLE IF NOT EXISTS track(product_id INT)"
    mycursor.execute(a)
    mydb.commit()

    sql = "delete from track"
    mycursor.execute(sql)
    mydb.commit()

    try:
        global date, d
        date_last = date.today() - timedelta(days=1)
        d = f"{date_last.day}_{date_last.month}_{date_last.year}"
        sql_date = f"alter table track drop column {d} "
        mycursor.execute(sql_date)
        mydb.commit()
        print("Old Date Column Deleted")
    except:
        pass
    try:
        date = date.today()
        d = f"{date.day}_{date.month}_{date.year}"
        sql = f"alter table track add column {d} TEXT"
        mycursor.execute(sql)
        mydb.commit()
        print("New Date Column Added")
    except Exception as e:
        print(e)
        pass

    sql = "select product_web_sku from products where website_id = 1"
    mycursor.execute(sql)
    a = mycursor.fetchall()
    l1 = [i[0] for i in a]
    for x in l1:
        try:
            response = requests.get(
                f"https://www.metro.pe/api/catalog_system/pub/products/search?fq=productId:{x}&sc=19")
            data = response.json()
            p_link = data[0]["link"]
            r = requests.get(url=p_link)
            soup = BeautifulSoup(r.text, 'html.parser')
            product_price = float(data[0]["items"][0]["sellers"][0]['commertialOffer']['Price'])
            productName = soup.find("title").get_text()
            sql = f"select product_price,id from products where productName = '{productName}'"
            mycursor.execute(sql)
            a = mycursor.fetchall()
            for i in a:
                sales1 = float(i[0])
                id = i[1]
                track_price = product_price - sales1
                sql1 = f"insert into track(product_id,{d}) values(%s,%s)"
                data = (id, track_price)
                mycursor.execute(sql1, data)
                mydb.commit()

        except:
            pass
        else:
            print(mycursor.rowcount, "datelines were inserted.")

    sql = "select product_web_sku,product_share_title from products where website_id = 2"
    mycursor.execute(sql)
    a = mycursor.fetchall()
    l1 = [i[0] for i in a]
    for x in l1:
        try:
            response = requests.get(
                f"https://www.vivanda.com.pe/api/catalog_system/pub/products/search?fq=productId:{x}")
            data = response.json()
            product_price = float(data[0]["items"][0]["sellers"][0]['commertialOffer']['Price'])
            product_share_title = data[0]["productTitle"]
            sql = f"select product_price,id from products where product_share_title = '{product_share_title}'"
            mycursor.execute(sql)
            a = mycursor.fetchall()
            for i in a:
                sales1 = float(i[0])
                id = i[1]
                track_price = product_price - sales1
                sql1 = f"insert into track(product_id,{d}) values(%s,%s)"
                data = (id, track_price)
                mycursor.execute(sql1, data)
                mydb.commit()
        except:
            pass
        else:
            print(mycursor.rowcount, "datelines were inserted.")

    sql = "select product_web_sku from products where website_id = 3"
    mycursor.execute(sql)
    a = mycursor.fetchall()
    l1 = [i[0] for i in a]
    for x in l1:
        try:
            response = requests.get(
                f"https://www.wong.pe/api/catalog_system/pub/products/search?sc=70&fq=productId:{x}")
            data = response.json()
            p_link = data[0]["link"]
            r = requests.get(url=p_link)
            soup = BeautifulSoup(r.text, 'html.parser')
            product_price = float(data[0]["items"][0]["sellers"][0]['commertialOffer']['Installments'][0]['Value'])
            productname = soup.find("title").get_text()
            sql = f"select product_price,id from products where productname = '{productname}'"
            mycursor.execute(sql)
            a = mycursor.fetchall()
            for i in a:
                sales1 = float(i[0])
                id = i[1]
                track_price = product_price - sales1
                sql1 = f"insert into track(product_id,{d}) values(%s,%s)"
                data = (id, track_price)
                mycursor.execute(sql1, data)
                mydb.commit()

        except:
            pass
        else:
            print(mycursor.rowcount, "datelines were inserted.")

    sql = "select product_web_sku from products where website_id = 4"
    mycursor.execute(sql)
    a = mycursor.fetchall()
    l1 = [i[0] for i in a]
    for x in l1:
        try:
            response = requests.get(
                f"https://www.plazavea.com.pe/api/catalog_system/pub/products/search?&fq=productId:{x}")
            data = response.json()
            product_price = float(data[0]["items"][0]["sellers"][0]['commertialOffer']['Installments'][0][
                                      'Value'])
            product_title_name = data[0]["productName"]
            sql = f"select product_price,id from products where product_title_name = '{product_title_name}'"
            mycursor.execute(sql)
            a = mycursor.fetchall()
            for i in a:
                try:
                    sales1 = float(i[0])
                    id = i[1]
                    track_price = product_price - sales1
                    sql1 = f"insert into track(product_id,{d}) values(%s,%s)"
                except:
                    pass
                else:
                    data = (id, track_price)
                    mycursor.execute(sql1, data)
                    mydb.commit()

        except:
            pass
        else:
            print(mycursor.rowcount, "datelines were inserted.")


