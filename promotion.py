import concurrent.futures

import mysql.connector
import requests

mydb = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="db"
)
mycursor = mydb.cursor(buffered=True)
s = "CREATE TABLE IF NOT EXISTS Promotion(id INT AUTO_INCREMENT PRIMARY KEY,Website_id TEXT,Promotion_id TEXT)"
mycursor.execute(s)
mydb.commit()


def Promotion(x):
    q =""
    mydb = mysql.connector.connect(
        host="localhost",
        user="admin",
        password="admin",
        database="db"
    )
    mycursor = mydb.cursor(buffered=True)

    sql = "INSERT INTO Promotion"
    sql = sql + "(website_id,Promotion_id) VALUES( %s, %s) "
    response = requests.get(f"https://www.metro.pe/api/catalog_system/pub/products/search?fq=productId:{x}&sc=19")
    try:
        data = response.json()

        id = 1
        promotionID = data[0]["productClusters"]
        for i in promotionID:
            q += str(i) + ":" + promotionID[i] + ","
    except:
        pass
    else:
        p_data = (id,
                  q)

        mycursor.execute(sql, p_data)
        mydb.commit()
        print(mycursor.rowcount, "lines were inserted.")

    sql = "INSERT INTO Promotion"
    sql = sql + "(website_id,Promotion_id) VALUES( %s, %s) "
    response = requests.get(f"https://www.vivanda.com.pe/api/catalog_system/pub/products/search?fq=productId:{x}")
    try:
        data = response.json()

        id = 2
        promotionID = data[0]["productClusters"]
        for i in promotionID:
            q += str(i) + ":" + promotionID[i] + ","
    except:
        pass
    else:
        p_data = (id,
                  q)

        mycursor.execute(sql, p_data)
        mydb.commit()
        print(mycursor.rowcount, "lines were inserted.")

    sql = "INSERT INTO Promotion"
    sql = sql + "(website_id,Promotion_id) VALUES( %s, %s) "
    response = requests.get(f"https://www.wong.pe/api/catalog_system/pub/products/search?sc=70&fq=productId:{x}")
    try:
        data = response.json()

        id = 3
        promotionID = data[0]["productClusters"]
        for i in promotionID:
            q += str(i) + ":" + promotionID[i] + ","
    except:
        pass
    else:
        p_data = (id,
                  q)

        mycursor.execute(sql, p_data)
        mydb.commit()
        print(mycursor.rowcount, "lines were inserted.")

    sql = "INSERT INTO Promotion"
    sql = sql + "(website_id,Promotion_id) VALUES( %s, %s) "
    response = requests.get(f"https://www.plazavea.com.pe/api/catalog_system/pub/products/search?&fq=productId:{x}")
    try:
        data = response.json()

        id = 4
        promotionID = data[0]["productClusters"]
        for i in promotionID:
            q += str(i) + ":" + promotionID[i] + ","
    except:
        pass
    else:
        p_data = (id,
                  q)

        mycursor.execute(sql, p_data)
        mydb.commit()
        print(mycursor.rowcount, "lines were inserted.")

with concurrent.futures.ThreadPoolExecutor(1) as e:
    e.map(Promotion, range(1, 100000))
