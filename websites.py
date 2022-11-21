import mysql.connector


def websites():
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

    a = "delete from WEBSITES"
    mycursor.execute(a)
    mydb.commit()


    web_list = ["https://www.metro.pe/", "https://www.vivanda.com.pe/", "https://www.wong.pe/",
                "https://www.plazavea.com.pe/","https://www.ripley.com/"]
    for web in web_list:
        s = "INSERT INTO WEBSITES(Websites) VALUES( %s)"
        data = (web,)
        mycursor.execute(s, data)
        mydb.commit()
