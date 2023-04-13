import pymysql


def getCursor(host="localhost", user="root", password="123456", db="booking"):
    try:
        con = pymysql.connect(host=host,
                              user=user,
                              password=password,
                              db=db)
        cur = con.cursor()
        print("数据库连接成功！")
        return con, cur
    except Exception as e:
        print("数据库连接失败：", e)
