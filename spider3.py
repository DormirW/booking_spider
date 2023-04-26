import re

import requests
from bs4 import BeautifulSoup
from datashape import null
import time
from DAO import getCursor
import threading

MAX_RETRIES = 3


def get_res(url, header):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, headers=header)
            # 检查响应状态码，如果为200表示请求成功
            if response.status_code == 200:
                return response.text
            else:
                print("请求失败，状态码：{}".format(response.status_code))
        except requests.exceptions.RequestException as e:
            print("请求异常：{}".format(e))
        # 连接失败时等待一段时间后进行重试
        time.sleep(5)
        retries += 1
    print("连接失败，达到最大重试次数")
    return None


def crawl(offset, datestr):
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "Cookie": "cnfunco=1; cors_js=1; bkng_sso_session=e30; _pxvid=66b7ad47-d9a2-11ed-b782-6f536853454d; _gcl_au=1.1.1488254222.1681352718; OptanonAlertBoxClosed=2023-04-13T16:51:26.428Z; bkng_sso_ses=eyJib29raW5nX2dsb2JhbCI6W3siYSI6MSwiaCI6IjlLNUZPWkFqRFNLUCtmSzdwUWVZQXhlSDVRUzBNSW40ajVEd0NVdGY5S28ifV19; _gid=GA1.2.166187746.1681702448; _ga_FPD6YLJCJ7=GS1.1.1681702519.5.0.1681702519.0.0.0; _ga=GA1.2.391215452.1681352718; aliyungf_tc=1a4b81a44492bde8a0808c5a38173cfb85bbef9f55766bc488831867e69b3cad; acw_tc=1a0c39ca16817805023016346ecece44c362795d643419c4de93981f8b1ed1; pxcts=753d3d56-dd86-11ed-8b00-784d4d67796e; OptanonConsent=implicitConsentCountry=GDPR&implicitConsentDate=1681352713923&isGpcEnabled=0&datestamp=Tue+Apr+18+2023+09%3A16%3A30+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=6.22.0&isIABGlobal=false&hosts=&consentId=8565d40d-89e0-44b4-b259-2fa54718c79f&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false&backfilled_at=1681404688827&backfilled_seed=1&geolocation=CN%3BJS; bkng=11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbca8KLfxLPef7ZWMsiltGXrs3A2IyQP5i%2BCC25xQvvBsnQt%2FTm6jnrt%2BapiqwXtsxlMMpKRg53XLe3g4zboYSHVzVKSAlHxcYKPuFnRoEcOdhp0dtNy5fhzGDKq92Bws8vqM1dWowsInD04lZbSmM2Hi3UhhGx9HqVC15XgmUowA%3D; _uetsid=b5dc9b80dcd011ed96a57b529997ff11; _uetvid=6f70ceb0d9a211ed9919cfe4967a558c; _px3=7eb54fd4fe40bf19c85e1df1637d17255d04e225328466d353d83ccf52eec433:Dgv/bPUl+5PSvcBbGaRYOGgrfewizBWDX4DCumFexEovhPC0rMJa0OY0XlV0ey0ks01KRSU3jZDijFA6cnes3Q==:1000:5/BS3WsclrUT8M4HhosjVlwDbzZL9BDk0PmHpnrgzD5cQ1tuo9KuskgV4Umx5EJ8Ybc7va6vdLhlYTAPgVgP26DhAltcZkQtxk6j6AnJ3+nf94D0Lv91p9H0FH1PKhUlehsb+EKWInlaUzlwOuXCuOg2zRWqDHt2GRAUwSlx4ZLj9FymgDmK6yAcIEsfoHObtKRg951aXRwNF4amln/UoA==; _pxde=2651fb1d6c248eabf5c1d22f74bacee6ac2e526d7eeede2c3d7441930fc2c3aa:eyJ0aW1lc3RhbXAiOjE2ODE3ODA1ODg0MTAsImZfa2IiOjAsImlwY19pZCI6W119"

    }

    # 建立数据库链连接
    con, cur = getCursor()
    sta = f"SELECT LINK, NAME FROM HOTEL WHERE name NOT IN (SELECT hotel_name FROM room_type) LIMIT 50 OFFSET {50 * offset}"
    cur.execute(sta)
    results = cur.fetchall()

    for url, name in results:
        print("当前offset: ", offset)

        # 补全URL中的日期信息
        url = url.replace("#hotelTmpl", datestr)

        # 发送请求，接收响应
        data = get_res(url, header)

        # 解析html
        soup = BeautifulSoup(data, "html.parser")
        soup.prettify()

        cards = soup.find_all("tr")
        for card in cards:
            try:
                room_type = card.find_next("span", attrs={"class": "hprt-roomtype-icon-link"}).text.replace("\n", "")
            except Exception as e:
                print(e)
                continue
            try:
                price = card.find_next("span", attrs={"class": "prco-valign-middle-helper"}).text
                base_price = re.findall(r'\d+\.\d+|\d+', price)[0]
            except Exception as e:
                print(e)
                base_price = ""
            try:
                capacity = card.find_next("span", attrs={"class": "bui-u-sr-only"}).text.replace("\n", "")
                capacity = re.findall(r'\d+\.\d+|\d+', capacity)[0]
            except Exception as e:
                print(e)
                capacity = ""
            try:
                bed_type = card.find_next("li", attrs={"class": "rt-bed-type"}).find("span").text.replace("\n",
                                                                                                          "").replace(
                    "\"", "")
            except Exception as e:
                print(e)
                bed_type = ""
            try:
                room_description = card.find_next("p", attrs={"class": "short-room-desc"}).text.replace("\n", "")
            except Exception as e:
                print(e)
                room_description = ""
            try:
                pros = card.find_all_next("span", attrs={
                    "class": "bui-badge bui-badge--outline room_highlight_badge--without_borders"})
                properties = ""
                for pro in pros:
                    properties = properties + pro.find_next("span").text.replace("\n", "").replace("\"", "") + " "
            except Exception as e:
                print(e)
                properties = ""
            try:
                stm = "INSERT INTO ROOM_TYPE(HOTEL_NAME, ROOM_TYPE, BED_TYPE, ROOM_DESCRIPTION, PROPERTIES, CAPACITY, " \
                      "BASE_PRICE) VALUES(%s, %s, %s, %s, %s, %s, %s)"
                values = (name, room_type, bed_type, room_description, properties, capacity, base_price)
                cur.execute(stm, values)
                print(f"offset={offset}", name, room_type, bed_type, room_description, properties, capacity, price)
            except Exception as e:
                print(e, offset)
            con.commit()
    print(f"第{offset}页已爬取完成")
    # 提交请求，关闭数据库
    con.commit()
    con.close()


if __name__ == '__main__':
    dates = [";checkin=2023-05-02;checkout=2023-05-03;#hotelTmpl", ";checkin=2023-05-03;checkout=2023-05-04;#hotelTmpl",
             ";checkin=2023-05-04;checkout=2023-05-05;#hotelTmpl",
             ";checkin=2023-05-08;checkout=2023-05-09;#hotelTmpl", ";checkin=2023-05-09;checkout=2023-05-10;#hotelTmpl",
             ";checkin=2023-05-10;checkout=2023-05-11;#hotelTmpl",
             ";checkin=2023-05-11;checkout=2023-05-12;#hotelTmpl"]
    # [";checkin=2023-04-26;checkout=2023-04-27;#hotelTmpl", ";checkin=2023-04-27;checkout=2023-04-28;#hotelTmpl",

    for datestr in dates:
        offsets = range(10, 11)
        threads = []
        for offset in offsets:
            t = threading.Thread(target=crawl, args=(offset, datestr,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        print("All threads finished")
