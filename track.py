from datetime import timedelta

import mysql.connector
import requests
from bs4 import BeautifulSoup


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
