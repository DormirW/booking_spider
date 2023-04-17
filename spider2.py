import requests
import json
from bs4 import BeautifulSoup
from datashape import null
import time
from DAO import getCursor
import openpyxl
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


def crawl(places):
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "Cookie": "cnfunco=1; cors_js=1; bkng_sso_session=e30; bkng_sso_ses=e30; _pxvid=a9278cf5-c273-11ed-abc2-61766a736e6a; aliyungf_tc=d3885ada5956aae4854d7f2d90a5450565aebdd37dba3f5cc48c6e44aa587d12; pxcts=1eaefc4b-c2e6-11ed-be83-6e44527a546c; _px3=b00ae55e3e40e14b8459a531f519a748224447082cb9522b158167ac90fcbc32:ycsVtiW3puL9qwm64tJrDhRLcMmNu37y58fRzDnsNhsjkJU1Yv2p6xd38D6fqbKcCRPFqc44cMKRkt/uc8SlVA==:1000:58kZIf28YEBeDrApusYwPF7y+MsyMeDTAFiahHCcQE83YEgoH5SL1V0GmyAaQtWI9f4lijGxnRT3NXG8vFOEqs/0RwLmBGuBxAqwpkKJdr3Eh6njLy4IzMPvDfKS/X7MFiJkQ4Ixc6baLIh5GeqFyXjFenr1nRAwnG5YbbZ1Gw4nXGOIWUdYklIOwYCezOYThUmkkXGCUIbHaDKxXMx/YA==; _pxde=e350cb609d3e9380ec12b46d348cda305235ff04a281c24c4c35d03b9703feab:eyJ0aW1lc3RhbXAiOjE2Nzg4NjA5MTYwNzMsImZfa2IiOjAsImlwY19pZCI6W119; acw_tc=781bad4d16788610098562653e3e457a574af3ef4a11e0de50339346fc8e86; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Mar+15+2023+14%3A16%3A52+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=6.22.0&isIABGlobal=false&hosts=&consentId=9ed25a52-ac48-498a-9154-02a2277c150b&interactionCount=1&landingPath=NotLandingPage&AwaitingReconsent=false&groups=C0001%3A1%2CC0002%3A0%2CC0004%3A0&implicitConsentCountry=GDPR&implicitConsentDate=1678803715087; bkng=11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbiKbS0JOgDBKd%2F%2B1hfoh7rsSN%2BxB95HtADwNYBs8pGf2lSb2tl%2FzFOhcqAGvO9287BuvqnpxJp%2FYvSeB0JnGKuKXONr6PaKaTqjIKtIFNTFi0c07sxHNaiLMvvFr3De4Csoj82Qf60AK8434Pk8LrQZQKiiVeiU4idAcAQtO2Reo%3D"
    }
    num = 0
    elem_list = []
    # places = ["中国河北", "山西", "黑龙江", "吉林", "沈阳市", "大连市", "鞍山市", "抚顺市", "本溪市", "丹东市",
    #           "锦州市", "营口市", "阜新市", "辽阳市", "盘锦市", "铁岭市", "朝阳市", "葫芦岛市", "江苏", "浙江", "安徽",
    #           "福建", "江西", "山东", "郑州市", "开封市", "洛阳市", "平顶山市", "安阳市", "鹤壁市", "新乡市", "焦作市",
    #           "濮阳市", "许昌市", "漯河市", "三门峡市", "南阳市", "商丘市", "信阳市", "周口市", "驻马店市",
    #           "湖北", "湖南", "广东", "海南", "成都市", "自贡市", "攀枝花市", "泸州市", "德阳市", "绵阳市", "广元市",
    #           "遂宁市", "内江市", "乐山市", "南充市", "眉山市", "宜宾市", "广安市", "达州市", "雅安市", "巴中市",
    #           "资阳市", "贵州", "云南", "西安市", "铜川市", "宝鸡市", "咸阳市", "渭南市", "延安市", "汉中市", "榆林市",
    #           "安康市", "商洛市", "甘肃", "青海", "台湾", "内蒙古",
    #           "广西", "西藏", "宁夏", "新疆", "北京", "天津", "上海", "重庆", "香港", "澳门"]
    # 建立数据库链连接
    con, cur = getCursor()
    for place in places:
        print("当前地区为", place)
        for offset in range(0, 1000, 25):
            counter = 0
            # 构造url
            url = f"https://www.booking.cn/searchresults.zh-cn.html?ss={place}&offset={offset}"

            # 发送请求，接收响应
            data = get_res(url, header)

            # 解析html
            soup = BeautifulSoup(data, "html.parser")
            soup.prettify()

            # 获取你想要的数据
            cards = soup.find_all("div", attrs={"data-testid": "property-card"})
            if cards == []:
                counter = 18
            for card in cards:
                if counter >= 18:
                    break
                try:
                    # 酒店名
                    name = card.find_next("div", attrs={"data-testid": "title"}).text
                    flag = 0
                    for el in elem_list:
                        if name in el.values():
                            flag = 1
                            print(f"offset={offset},第{num}项{name}出现重复")
                    if flag == 1:
                        break
                    else:
                        elem = {"name": name}

                    elem["id"] = num

                    # 酒店链接
                    link = card.find_next("a", attrs={"data-testid": "title-link"}).get('href')
                    elem["link"] = link

                    # 酒店简介
                    description = card.find_next("div", attrs={"class": "d8eab2cf7f"}).text
                    elem["description"] = description

                    # 酒店评分
                    try:
                        score = card.find_next("div", attrs={"class": "b5cd09854e d10a6220b4"}).text
                        elem["score"] = score
                    except Exception as e:
                        score = null
                        elem["score"] = score
                        print(e)

                    # 进入酒店链接获取更多信息
                    info = get_res(link, header)
                    info_soup = BeautifulSoup(info, "html.parser")
                    info_soup.prettify()

                    # 所在地区
                    positions = info_soup.find_all("a", attrs={"itemprop": "item"})
                    country = positions[-4].text.replace("\n", "")
                    province = positions[-3].text.replace("\n", "")
                    city = positions[-2].text.replace("\n", "")
                    region = ""
                    if country == "中国":
                        area = country + ' ' + province + ' ' + city
                        elem["area"] = area
                        elem["country"] = country
                        elem["province"] = province
                        elem["city"] = city
                        elem["region"] = region
                    elif "中国" in country:
                        country = "中国"
                        province = positions[-4].text.replace("\n", "").replace("中国", "")
                        city = positions[-3].text.replace("\n", "")
                        region = positions[-2].text.replace("\n", "")
                        area = country + ' ' + province + ' ' + city + ' ' + region
                        elem["area"] = area
                        elem["country"] = country
                        elem["province"] = province
                        elem["city"] = city
                        elem["region"] = region
                    elif "中国" in province:
                        province = province.replace("中国", "")
                        country = "中国"
                        area = country + ' ' + province + ' ' + city
                        elem["area"] = area
                        elem["country"] = country
                        elem["province"] = province
                        elem["city"] = city
                        elem["region"] = region
                    else:
                        country = positions[-5].text.replace("\n", "")
                        province = positions[-4].text.replace("\n", "")
                        city = positions[-3].text.replace("\n", "")
                        region = positions[-2].text.replace("\n", "")
                        area = country + ' ' + province + ' ' + city + ' ' + region
                        elem["area"] = area
                        elem["country"] = country
                        elem["province"] = province
                        elem["city"] = city
                        elem["region"] = region


                    # 具体位置
                    location = info_soup.find_all("span", attrs={"data-component": "tooltip"})[0].text.replace("\n", "")
                    elem["location"] = location

                    # 评价数量
                    try:
                        commentNum = card.find_next("div", attrs={"class": "d8eab2cf7f c90c0a70d3 db63693c62"}).text[
                                     :-5]
                        elem["commentNum"] = commentNum
                    except Exception as e:
                        commentNum = "-1"
                        elem["commentNum"] = commentNum
                        print(e)

                    # 简评
                    try:
                        briefComment = info_soup.find_next("div",
                                                           attrs={"class": "b5cd09854e f0d4d6a2f5 e46e88563a"}).get(
                            "aria-label")
                        elem["briefComment"] = briefComment
                    except Exception as e:
                        briefComment = null
                        elem["briefComment"] = briefComment
                        print(e)

                    # 图片
                    images_link = info_soup.find("div", attrs={
                        "class": "clearfix bh-photo-grid bh-photo-grid--space-down fix-score-hover-opacity"})
                    images_link = images_link.find_all("div", attrs={"aria-hidden": "true"})
                    for i in range(1, 4):
                        image_link = images_link[i].find_next("img").get("src").replace("\n", "")
                        im_res = requests.get(image_link)
                        with open("./images/%s %d.jpg" % (name, i), 'wb') as f:
                            f.write(im_res.content)
                        elem[f"image{i}"] = image_link

                    try:
                        # 读取图片并写入数据库
                        images = []
                        for i in range(1, 4):
                            with open("./images/%s %d.jpg" % (name, i), 'rb') as f:
                                images.append(f.read())
                        stm = "INSERT INTO HOTEL(ID, LINK, NAME, DESCRIPTION, AREA, COUNTRY, PROVINCE, CITY, REGION, " \
                              "LOCATION, IMAGE1, IMAGE2, IMAGE3, SCORE, COMMENTNUM, BRIEFCOMMENT) " \
                              "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        values = (
                            num, link, name, description, area, country, province, city, region, location, images[0],
                            images[1], images[2], score, commentNum, briefComment)
                        cur.execute(stm, values)
                        # con.commit()
                        print(elem)
                        num += 1
                    except Exception as e:
                        # print(e)
                        counter += 1
                    try:
                        lst = info_soup.find_all("div", attrs={"class": "ed14448b9f ccff2b4c43 cb10ca9525"})
                        for item in lst:
                            try:
                                # 酒店房型
                                rType = item.find("span").text
                                elem = {"rType": rType}
                            except Exception as e:
                                print(e)
                                rType = null
                                elem = {"rType": null}

                            try:
                                # 床型
                                bType = item.find("span").find_next("span").text
                                elem["bType"] = bType
                            except Exception as e:
                                print(e)
                                bType = null
                                elem["bType"] = null

                            try:
                                stm = "INSERT INTO ROOMTYPE(HNAME, RTYPE, BTYPE) " \
                                      "VALUES(%s, %s, %s)"
                                values = (name, rType, bType)
                                cur.execute(stm, values)
                                print(f"offset={offset},num={num}", elem)
                                elem_list.append(elem)
                            except Exception as e:
                                a = 1
                            finally:
                                continue
                    except Exception as e:
                        print(e)
                    finally:
                        continue
                except Exception as e:
                    print(e)
                finally:
                    continue
            con.commit()
            if counter >= 18:
                print(f"{place}已爬取完成，进入下一地区")
                break

    # 提交请求，关闭数据库
    con.commit()
    con.close()


if __name__ == '__main__':
    # places = [["中国河北"], ["山西"], ["黑龙江"], ["吉林"], ["沈阳市", "大连市"], ["鞍山市", "抚顺市"], ["本溪市", "丹东市", "锦州市"], ["营口市",
    # "阜新市", "辽阳市"], ["盘锦市", "铁岭市", "朝阳市", "葫芦岛市"], ["江苏"], ["浙江"], ["安徽"], ["福建"], ["江西"], ["山东"], ["郑州市", "开封市"],
    # ["洛阳市", "平顶山市", "安阳市"], ["鹤壁市", "新乡市", "焦作市"], ["濮阳市", "许昌市", "漯河市"], ["三门峡市", "南阳市", "商丘市"], ["信阳市", "周口市",
    # "驻马店市"], ["湖北"], ["湖南"], ["广东"], ["海南"], ["成都市"], ["自贡市", "攀枝花市"], ["泸州市", "德阳市", "绵阳市"], ["广元市", "遂宁市",
    # "内江市"], ["乐山市", "南充市", "眉山市"], ["宜宾市", "广安市", "达州市"], ["雅安市", "巴中市", "资阳市"], ["贵州"], ["云南"], ["西安市"], ["铜川市",
    # "宝鸡市", "咸阳市"], ["渭南市", "延安市", "汉中市"], ["榆林市", "安康市"], ["商洛市", "甘肃"], ["青海"], ["台湾"], ["内蒙古"], ["广西"], ["西藏"],
    # ["宁夏"], ["新疆"], ["北京"], ["天津"], ["上海"], ["重庆"], ["香港"], ["澳门"]]
    # places = [["香港"], ["澳门"], ["台北市"], ["新北市"],
    # ["桃园市"], ["台中市"], ["台南市"], ["高雄市"], ["新竹县"], ["苗栗县"], ["彰化县"], ["南投县"], ["云林县"], ["嘉义县"], ["屏东县"], ["宜兰县"],
    # ["花莲县"], ["台东县"], ["澎湖县"], ["金门县"], ["连江县"], ["基隆市"], ["新竹市"], ["嘉义市"]]
    places = [["台湾"]]
    threads = []
    for place in places:
        t = threading.Thread(target=crawl, args=(place,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("All threads finished")
