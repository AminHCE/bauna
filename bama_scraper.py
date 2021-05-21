import re
import os
import time
import csv
import socket
import requests
import pymongo
import hashlib

from bs4 import BeautifulSoup
from datetime import datetime


def len_fit(carry, length, char=' '):
    """
    This function will fit length of a string
    :param carry: String
    :param length: number of char in final string
    :param char: fill string with this char
    :return: String
    """
    return str(carry + char * (length - len(carry)))


def car_exist(oid, db):
    """
    This function check if a car exist in database
    :param oid: The id of a car to check for the existence
    :param db: The name of mongodb collection
    :return: True | False
    """
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongod = client["baunaDB"]
    collection = mongod[db]
    query = {"_id": oid}
    doc = collection.find(query)

    if doc:
        return True
    else:
        return False


def internet_connected(host="8.8.8.8", port=53):
    """
    Host: 8.8.8.8 (google-public-dns-a.google.com)
    OpenPort: 53/tcp
    Service: domain (DNS/TCP)
    """
    try:
        socket.setdefaulttimeout(1)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except:
        return False


def car_scraper(car_url, car_db):
    """
    This function will scrape from "bama.ir"
    :param car_url: url of car page by brand
    :param car_db: car database name in mongodb
    :return: number of booked cars
    """
    html_bama_car = None
    html_case = None
    count = 0

    for will in range(10):
        try:
            html_bama_car = requests.get(car_url)
        except requests.exceptions.RequestException as e:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(e)
            time.sleep(10)
            if internet_connected():
                break
            else:
                time.sleep(0.5)
                print("connection lost!")
                print(will)
                continue

    soup_bama_car = BeautifulSoup(html_bama_car.text, 'html.parser')
    all_bama_car = soup_bama_car.find_all('li', attrs={'class': 'car-list-item-li'})

    for cars in all_bama_car:
        jump = False
        car_case = {}
        result = (re.search(r'href=\"(.*?)\"', str(cars)))
        url = str(result.group(1))

        try:
            html_case = requests.get(url)
        except requests.exceptions.RequestException as e:
            print(e)
            for j in range(10):
                time.sleep(0.5)
                if internet_connected("8.8.8.8", 53) is True:
                    break
                else:
                    os.system('cls' if os.name == 'nt' else 'clear')
                    time.sleep(0.5)
                    print("connection lost!")
                    print(j)

        try:
            soup_case = BeautifulSoup(html_case.text, 'html.parser')

            url_hash = hashlib.md5(url.encode())
            car_case['_id'] = url_hash.hexdigest()
            car_case['date'] = str(datetime.now())

            # Find brand and model
            brand_case = soup_case.find_all('script')
            for br in brand_case:
                # Find brand
                try:
                    result = re.search(r"brand: \'(\w+)\'", str(br))
                    car_case['brand'] = result.group(1)
                except:
                    pass

                # Find model
                try:
                    result = re.search(r"model: \'(\w+)\'", str(br))
                    car_case['model'] = result.group(1)
                except:
                    pass

            all_span = soup_case.find_all('span')
            span_case = []

            for spans in all_span:
                span = (re.sub(r'\s+\n', '\n', spans.text).strip())
                span_case.append(span)

            for i in range(len(span_case)):
                if span_case[i] == "قیمت":
                    if span_case[i + 1] == "تماس بگيريد":
                        jump = True
                    elif span_case[i + 1] == "در توضیحات":
                        jump = False
                        price_case = soup_case.find_all('span', attrs={'style': 'font-weight:bold;'})

                        for pr in price_case:
                            price = (re.sub(r'\s+\n', '\n', pr.text).strip())
                            price = (re.search(r'([0-9]+(,[0-9]+)+)', price))
                            car_case['price'] = int(re.sub(r',', '', price.group(1)))
                    elif span_case[i + 1] == "حواله":
                        jump = True
                    elif span_case[i + 1] == "کارتکس":
                        jump = True
                    elif span_case[i + 1] == "توافقی":
                        jump = True
                    else:
                        jump = False
                        car_case['price'] = int(re.sub(r',', '', span_case[i + 1]))
                elif span_case[i] == "پیش پرداخت":
                    jump = True
                elif span_case[i] == "كاركرد":
                    if span_case[i + 1] == "حواله":
                        jump = True
                    elif span_case[i + 1] == "کارتکس":
                        jump = True
                    elif span_case[i + 1] == "پیش فروش":
                        jump = True
                    elif span_case[i + 1] == "-":
                        car_case['miles'] = int(0)
                    else:
                        miles = (re.sub(r',', '', span_case[i + 1]))
                        car_case['miles'] = int(re.search(r'(\d+)', miles).group(1))
                elif span_case[i] == "گیربکس":
                    if span_case[i + 1] == "اتوماتیک":
                        car_case['gearbox'] = "Auto"
                    elif span_case[i + 1] == "دنده ای":
                        car_case['gearbox'] = "Manual"
                    else:
                        jump = True
                elif span_case[i] == "بدنه":
                    if span_case[i + 1] == "بدون رنگ":
                        car_case['dot'] = int(0)
                    elif span_case[i + 1] == "یک لکه رنگ":
                        car_case['dot'] = int(1)
                    elif span_case[i + 1] == "دو لکه رنگ":
                        car_case['dot'] = int(2)
                    elif span_case[i + 1] == "چند لکه رنگ":
                        car_case['dot'] = int(3)
                    elif span_case[i + 1] == "صافکاری بدون رنگ":
                        car_case['dot'] = int(4)
                    else:
                        jump = True

            car_case['url'] = url
            all_script = soup_case.find_all('script', attrs={'type': 'text/javascript'})
            for script in all_script:
                try:
                    result = (re.search(r'var jsAdIdShortCode = \'(\w+)\';', str(script)))
                    ad_id_short_code = result.group(1)
                    get_phone_data_url = 'https://bama.ir/insertphoneclicknew/{ad_id}/false/2'.format(
                        ad_id=ad_id_short_code)
                    get_phone_data = requests.post(get_phone_data_url)
                    phone_number = get_phone_data.json()['PhoneNumber']
                    mobile_number = get_phone_data.json()['MobileNumbers']

                    phone_numbers = phone_number.split(',')

                    if mobile_number:
                        mobile_numbers = mobile_number.split(',')
                        phone_numbers += mobile_numbers

                    car_case['phone'] = phone_numbers

                except:
                    pass

            if jump is False and not car_exist(car_case["_id"], car_db):
                count += 1
                my_client = pymongo.MongoClient("mongodb://localhost:27017/")
                my_db = my_client["baunaDB"]
                my_col = my_db[car_db]
                my_col.insert_one(car_case)

        except:
            pass

    return count


def page_counter(car_url):
    global html_bama
    try:
        html_bama = requests.get(car_url)
    except requests.exceptions.RequestException as e:
        for j in range(10):
            time.sleep(0.5)
            if internet_connected() is True:
                break
            else:
                time.sleep(0.5)
                print("connection lost!")
                print(j)
                continue

    soup_bama = BeautifulSoup(html_bama.text, 'html.parser')
    all_bama = soup_bama.find_all('section', attrs={'id': 'content'})

    for case in all_bama:
        result = (re.search(r'\"TotalPages\":(\d+),\"b', str(case)))
        return result.group(1)


def run():
    breaker = 0

    with open('cars.csv') as myFile:
        reader = csv.DictReader(myFile)
        data = [r for r in reader]

    data_counter = 0
    while data:
        if len(data) <= data_counter:
            break

        cars = data[data_counter]
        count = 0
        page = 0
        start_scrap = str(datetime.now())

        print("\n\n     checking connection ...")
        if internet_connected():
            print("     connected to internet")
            total_page = int(page_counter(cars['url'])) - 2
            for i in range(total_page):
                percent = int((i / (total_page - 1)) * 100)
                per32 = int((i / total_page) * 32)
                per32_str = "█"
                for x in range(per32):
                    per32_str += "█"
                os.system('cls' if os.name == 'nt' else 'clear')
                print("\n\n     Scraping data from \"https://bama.ir\"\n        Brand: " + cars[
                    'brand'] + "\n        Models: *")
                print("\n       Progress: " + str(percent) + "%  |" + len_fit(per32_str, 32) + "| page: " + str(
                    i + 1) + " of " + str(total_page) + " | booked cars: " + str(count))
                url = cars['url'] + "?page=" + str(i)
                booked_car = car_scraper(url, cars['db'])
                count += booked_car
                page = i*12
                if booked_car == 0:
                    breaker += 1
                else:
                    breaker = 0
                if breaker == 10:
                    break

            print("\n   Data scraping is complete!")
            time.sleep(2)
            os.system('cls' if os.name == 'nt' else 'clear')
            print("\n\n     Writing on MongoDB: " + str(count))

        else:
            print("     connection fail!")

        data_counter += 1
