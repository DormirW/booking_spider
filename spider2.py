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


def crawl(places):
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3 Edge/16.16299",
        "Cookie": "cnfunco=1; cors_js=1; bkng_sso_session=e30; _pxvid=66b7ad47-d9a2-11ed-b782-6f536853454d; _gcl_au=1.1.1488254222.1681352718; OptanonAlertBoxClosed=2023-04-13T16:51:26.428Z; bkng_sso_ses=eyJib29raW5nX2dsb2JhbCI6W3siYSI6MSwiaCI6IjlLNUZPWkFqRFNLUCtmSzdwUWVZQXhlSDVRUzBNSW40ajVEd0NVdGY5S28ifV19; _gid=GA1.2.166187746.1681702448; _ga_FPD6YLJCJ7=GS1.1.1681702519.5.0.1681702519.0.0.0; _ga=GA1.2.391215452.1681352718; aliyungf_tc=1a4b81a44492bde8a0808c5a38173cfb85bbef9f55766bc488831867e69b3cad; acw_tc=1a0c39ca16817805023016346ecece44c362795d643419c4de93981f8b1ed1; pxcts=753d3d56-dd86-11ed-8b00-784d4d67796e; OptanonConsent=implicitConsentCountry=GDPR&implicitConsentDate=1681352713923&isGpcEnabled=0&datestamp=Tue+Apr+18+2023+09%3A16%3A30+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=6.22.0&isIABGlobal=false&hosts=&consentId=8565d40d-89e0-44b4-b259-2fa54718c79f&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false&backfilled_at=1681404688827&backfilled_seed=1&geolocation=CN%3BJS; bkng=11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbca8KLfxLPef7ZWMsiltGXrs3A2IyQP5i%2BCC25xQvvBsnQt%2FTm6jnrt%2BapiqwXtsxlMMpKRg53XLe3g4zboYSHVzVKSAlHxcYKPuFnRoEcOdhp0dtNy5fhzGDKq92Bws8vqM1dWowsInD04lZbSmM2Hi3UhhGx9HqVC15XgmUowA%3D; _uetsid=b5dc9b80dcd011ed96a57b529997ff11; _uetvid=6f70ceb0d9a211ed9919cfe4967a558c; _px3=7eb54fd4fe40bf19c85e1df1637d17255d04e225328466d353d83ccf52eec433:Dgv/bPUl+5PSvcBbGaRYOGgrfewizBWDX4DCumFexEovhPC0rMJa0OY0XlV0ey0ks01KRSU3jZDijFA6cnes3Q==:1000:5/BS3WsclrUT8M4HhosjVlwDbzZL9BDk0PmHpnrgzD5cQ1tuo9KuskgV4Umx5EJ8Ybc7va6vdLhlYTAPgVgP26DhAltcZkQtxk6j6AnJ3+nf94D0Lv91p9H0FH1PKhUlehsb+EKWInlaUzlwOuXCuOg2zRWqDHt2GRAUwSlx4ZLj9FymgDmK6yAcIEsfoHObtKRg951aXRwNF4amln/UoA==; _pxde=2651fb1d6c248eabf5c1d22f74bacee6ac2e526d7eeede2c3d7441930fc2c3aa:eyJ0aW1lc3RhbXAiOjE2ODE3ODA1ODg0MTAsImZfa2IiOjAsImlwY19pZCI6W119"

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
                        stm = "INSERT INTO HOTEL(LINK, NAME, DESCRIPTION, AREA, COUNTRY, PROVINCE, CITY, REGION, " \
                              "LOCATION, IMAGE1, IMAGE2, IMAGE3, SCORE, COMMENT_NUM, BRIEF_COMMENT) " \
                              "VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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
                                stm = "INSERT INTO ROOM_TYPE(HOTEL_NAME, ROOM_TYPE, BED_TYPE) " \
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
    places = [["益阳市"], ["郴州市"], ["永州市"], ["怀化市"], ["娄底市"],
              ["广州市"], ["韶关市"], ["深圳市"], ["珠海市"], ["汕头市"], ["佛山市"], ["江门市"], ["湛江市"],
              ["茂名市"], ["肇庆市"], ["惠州市"], ["梅州市"], ["汕尾市"], ["河源市"], ["阳江市"], ["清远市"],
              ["东莞市"], ["中山市"], ["潮州市"], ["揭阳市"], ["云浮市"],
              ["海口市"], ["三亚市"], ["三沙市"], ["儋州市"], ["成都市"], ["自贡市", "攀枝花市"],
              ["泸州市", "德阳市", "绵阳市"], ["广元市", "遂宁市", "内江市"], ["乐山市", "南充市", "眉山市"],
              ["宜宾市", "广安市", "达州市"], ["雅安市", "巴中市", "资阳市"], ["贵阳市"], ["六盘水市"], ["遵义市"],
              ["安顺市"], ["毕节市"], ["铜仁市"], ["昆明市"], ["曲靖市"], ["玉溪市"], ["保山市"], ["昭通市"],
              ["丽江市"], ["普洱市"], ["临沧市"], ["西安市"], ["铜川市", "宝鸡市", "咸阳市"],
              ["渭南市", "延安市", "汉中市"],
              ["榆林市", "安康市"], ["商洛市", "甘肃"], ["青海"], ["呼和浩特市"], ["包头市"], ["乌海市"],
              ["赤峰市"], ["通辽市"], ["鄂尔多斯市"], ["呼伦贝尔市"], ["巴彦淖尔市"], ["乌兰察布市"], ["昆明市"],
              ["曲靖市"], ["玉溪市"], ["保山市"], ["昭通市"], ["丽江市"], ["普洱市"], ["临沧市"], ["昆明市"],
              ["曲靖市"], ["玉溪市"], ["保山市"], ["昭通市"], ["丽江市"], ["普洱市"], ["临沧市"],
              ["宁夏"], ["新疆"], ["北京"], ["天津"], ["上海"], ["重庆"]]
    # ["香港"], ["澳门"], ["台湾"]]
# [["石家庄市", "唐山市", "秦皇岛市"], ["邯郸市", "邢台市", "保定市"], ["张家口市", "承德市",
#         #       "沧州市"], ["廊坊市", "衡水市", "太原市"], ["大同市", "阳泉市", "长治市"], ["晋城市",
#         #       "朔州市", "晋中市"], ["运城市", "忻州市", "临汾市", "吕梁市"], ["黑龙江"], ["吉林",
#         #       "沈阳市", "大连市"], ["鞍山市", "抚顺市"], ["本溪市", "丹东市", "锦州市"],
#         #       ["营口市", "阜新市", "辽阳市"],
#         #       ["盘锦市", "铁岭市", "朝阳市", "葫芦岛市"], ["南京市", "无锡市"], ["徐州市", "常州市"], ["苏州市",
#         #       "南通市", "连云港市"], ["淮安市", "盐城市", "扬州市"], ["镇江市", "泰州市", "宿迁市"],
#         #       ["杭州市", "宁波市", "温州市"], ["嘉兴市", "湖州市", "绍兴市"], ["金华市", "衢州市",
#         #       "舟山市"], ["台州市", "丽水市", "合肥市"], ["芜湖市", "蚌埠市"], ["淮南市", "马鞍山市",
#         #       "淮北市"], ["铜陵市", "安庆市", "黄山市"], ["阜阳市", "宿州市", "滁州市"], ["六安市",
#         #       "宣城市", "池州市"], ["亳州市", "福州市", "厦门市"], ["莆田市", "三明市", "泉州市"],
#         #       ["漳州市", "南平市", "龙岩市", "宁德市"],
#         #       ["南昌市", "景德镇市", "萍乡市"], ["九江市", "抚州市", "鹰潭市"], ["赣州市", "吉安市",
#         #       "宜春市"], ["新余市", "上饶市", "济南市"], ["青岛市", "淄博市", "枣庄市"], ["东营市",
#         #       "烟台市", "潍坊市", "济宁市"], ["泰安市", "威海市", "日照市"], ["临沂市", "德州市"],
#         #       ["聊城市", "滨州市", "菏泽市"], ["郑州市", "开封市"],
#         #       ["洛阳市", "平顶山市", "安阳市"], ["鹤壁市", "新乡市", "焦作市"], ["濮阳市", "许昌市", "漯河市"],
#         #       ["三门峡市", "南阳市", "商丘市"], ["信阳市", "周口市", "驻马店市"], ["武汉市"], ["黄石市"], ["十堰市"],
#         #       ["宜昌市"],
#         #       ["襄阳市", "鄂州市", "荆门市"], ["孝感市", "荆州市", "黄冈市"], ["咸宁市", "随州市"],
#         #       ["长沙市", "株洲市"], ["湘潭市", "衡阳市", "邵阳市"], ["岳阳市", "常德市", "张家界市"]]
    # places = [["香港"], ["澳门"], ["台北市"], ["新北市"],
    # ["桃园市"], ["台中市"], ["台南市"], ["高雄市"], ["新竹县"], ["苗栗县"], ["彰化县"], ["南投县"], ["云林县"], ["嘉义县"], ["屏东县"], ["宜兰县"],
    # ["花莲县"], ["台东县"], ["澎湖县"], ["金门县"], ["连江县"], ["基隆市"], ["新竹市"], ["嘉义市"]]
    threads = []
    for place in places:
        t = threading.Thread(target=crawl, args=(place,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("All threads finished")
