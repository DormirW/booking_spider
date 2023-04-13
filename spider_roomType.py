import time
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from sympy.parsing.sympy_parser import null
from DAO import getCursor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

if __name__ == '__main__':
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.69",
        "Cookie": "cnfunco=1; cors_js=1; bkng_sso_session=e30; bkng_sso_ses=e30; _pxvid=a9278cf5-c273-11ed-abc2-61766a736e6a; aliyungf_tc=d3885ada5956aae4854d7f2d90a5450565aebdd37dba3f5cc48c6e44aa587d12; pxcts=1eaefc4b-c2e6-11ed-be83-6e44527a546c; _px3=b00ae55e3e40e14b8459a531f519a748224447082cb9522b158167ac90fcbc32:ycsVtiW3puL9qwm64tJrDhRLcMmNu37y58fRzDnsNhsjkJU1Yv2p6xd38D6fqbKcCRPFqc44cMKRkt/uc8SlVA==:1000:58kZIf28YEBeDrApusYwPF7y+MsyMeDTAFiahHCcQE83YEgoH5SL1V0GmyAaQtWI9f4lijGxnRT3NXG8vFOEqs/0RwLmBGuBxAqwpkKJdr3Eh6njLy4IzMPvDfKS/X7MFiJkQ4Ixc6baLIh5GeqFyXjFenr1nRAwnG5YbbZ1Gw4nXGOIWUdYklIOwYCezOYThUmkkXGCUIbHaDKxXMx/YA==; _pxde=e350cb609d3e9380ec12b46d348cda305235ff04a281c24c4c35d03b9703feab:eyJ0aW1lc3RhbXAiOjE2Nzg4NjA5MTYwNzMsImZfa2IiOjAsImlwY19pZCI6W119; acw_tc=781bad4d16788610098562653e3e457a574af3ef4a11e0de50339346fc8e86; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Mar+15+2023+14%3A16%3A52+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=6.22.0&isIABGlobal=false&hosts=&consentId=9ed25a52-ac48-498a-9154-02a2277c150b&interactionCount=1&landingPath=NotLandingPage&AwaitingReconsent=false&groups=C0001%3A1%2CC0002%3A0%2CC0004%3A0&implicitConsentCountry=GDPR&implicitConsentDate=1678803715087; bkng=11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbiKbS0JOgDBKd%2F%2B1hfoh7rsSN%2BxB95HtADwNYBs8pGf2lSb2tl%2FzFOhcqAGvO9287BuvqnpxJp%2FYvSeB0JnGKuKXONr6PaKaTqjIKtIFNTFi0c07sxHNaiLMvvFr3De4Csoj82Qf60AK8434Pk8LrQZQKiiVeiU4idAcAQtO2Reo%3D"
    }

    # 建立数据库链连接
    con, cur = getCursor()

    # edge_options = webdriver.EdgeOptions()
    # edge_options.add_experimental_option("debuggerAddress", "127.0.0.1:14556")
    # driver = webdriver.Edge(options=edge_options)
    # edge_options.add_argument("user-data-dir=C:\\Users\\94482\\AppData\\Local\\Microsoft\\Edge\\User Data")
    # driver = webdriver.Edge(options=edge_options)
    # driver = webdriver.Edge(service=Service("C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"), )
    num = 0
    for offset in range(0, 6000, 20):
        query = f"SELECT LINK, NAME FROM HOTELS LIMIT 20 OFFSET {offset}"
        count = cur.execute(query)
        rs = cur.fetchall()

        if count > 0:
            for result in rs:
                # 构造url
                url = result[0]

                # 获取酒店名称
                name = result[1]

                # driver = webdriver.Edge()
                #
                # # 打开网页
                # driver.get(url)
                #
                # # 等待网页加载完成
                # time.sleep(2)
                #
                # # 点击按钮
                # BUTTON = driver.find_element(By.XPATH, '/html/body/div/div/div[2]/button')
                # BUTTON.click()
                #
                # # 等待动态内容加载完成
                # time.sleep(3)

                # 模拟鼠标滚动到页面底部
                # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                # 等待动态内容加载完成
                # time.sleep(5)

                # 获取完整的HTML源码
                # html = driver.page_source

                # 发送请求，接收响应
                data = requests.get(url, headers=header).text

                # 解析html
                soup = BeautifulSoup(data, "html.parser")
                soup.prettify()

                # 防止异常
                try:
                    cards = soup.find_all("div", attrs={"class": "ed14448b9f ccff2b4c43 cb10ca9525"})
                    for card in cards:
                        try:
                            # 酒店房型
                            rType = card.find("span").text
                            elem = {"rType": rType}
                        except Exception as e:
                            print(e)
                            rType = null
                            elem = {"rType": null}

                        try:
                            # 床型
                            bType = card.find("span").find_next("span").text
                            elem["bType"] = bType
                        except Exception as e:
                            print(e)
                            bType = null
                            elem["bType"] = null

                        # try:
                        #     # 房间简介
                        #     description = card.find_next("p", attrs={"class": "short-room-desc"}).text.replace("\n",
                        #                                                                                        "").replace(
                        #         "\"", "")
                        #     elem["description"] = description
                        # except Exception as e:
                        #     print(e)
                        # finally:
                        #     description = null
                        #     elem["description"] = null
                        #
                        # try:
                        #     # 房间特性
                        #     properties = card.find_all("span", attrs={
                        #         "class": " bui-badge bui-badge--outline room_highlight_badge--without_borders"})
                        #     prop = ""
                        #     for p in properties:
                        #         prop.join(p.find("span").text.text.replace("\n", "").replace("\"", "") + " ")
                        #     elem["properties"] = prop
                        # except Exception as e:
                        #     print(e)
                        # finally:
                        #     prop = null
                        #     elem["properties"] = null

                        try:
                            stm = "INSERT INTO ROOMTYPE(HNAME, RTYPE, BTYPE) " \
                                  "VALUES(%s, %s, %s)"
                            values = (name, rType, bType)
                            cur.execute(stm, values)
                            con.commit()
                            num += 1
                            print(num, elem)
                        except Exception as e:
                            print(e)
                        finally:
                            continue
                except Exception as e:
                    print(e)
                finally:
                    continue

    # 提交请求，关闭数据库
    con.commit()
    con.close()
