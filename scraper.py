from twilio.rest import Client
from unittest import result
from urllib import response
import requests
import os

from bs4 import BeautifulSoup
import csv
import pandas as pd

import time
from datetime import datetime, date, timedelta

import schedule
import sys
import traceback

from dotenv import load_dotenv

load_dotenv()


twilio_api = os.environ["TWILIO_ACCOUNT_SID"]
twilio_auth = os.environ["TWILIO_AUTH"]

account_sid = twilio_api
auth_token = twilio_auth

client = Client(account_sid, auth_token)

my_number = os.environ["my_phone_number"]


def create_csv():
    if not os.path.isfile("car.csv"):
        with open("car.csv", "w", newline="") as csvfile:
            csv_headers = [
                "id",
                "created",
                "name",
                "price",
                "location",
                "url",
                "description",
                "jpg",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
            writer.writeheader()


def get_last_scrape():
    with open("car.csv", newline="") as csvfile:
        last_scrape = ""
        df = pd.read_csv("car.csv")
        print(df)
        if not df.empty:
            last_scrape = df["created"].max()
            last_scrape = pd.to_datetime(last_scrape)
    return last_scrape


def craigslist_soup(region, term, last_scrape):
    url = "https://{region}.craigslist.org/search/{term}".format(
        region=region, term=term
    )

    response = requests.get(url=url)
    soup = BeautifulSoup(response.content, "html.parser")
    posts = soup.find_all("li", class_="result-row")

    links = []
    image_jpg_list = []
    posting_body = []
    list_results = []

    for post in posts:
        print(post)
        title_class = post.find("a", class_="result-title hdrlnk")
        links.append(title_class["href"])

    for link in links:
        response_link = requests.get(url=link)
        link_soup = BeautifulSoup(response_link.content, "html.parser")
        image_url = link_soup.find("img")
        if image_url is not None:
            image_url = image_url["src"]
        else:
            image_url = "no image provided"
        image_jpg_list.append(image_url)
        section_body_class = link_soup.find("section", id="postingbody")
        if section_body_class is not None:
            section_body_class = section_body_class.get_text()
        else:
            section_body_class = "No description provided"
        stripped = section_body_class.replace("\n\nQR Code Link to Post\n", "")
        final_strip = stripped.replace("\n\n", "")
        posting_body.append(final_strip)

    for index, post in enumerate(posts):
        car_description_full = posting_body[index]
        image_url_jpg = image_jpg_list[index]
        result_price = post.find("span", class_="result-price")
        result_price_text = result_price.get_text()
        time_class = post.find("time", class_="result-date")
        created_at = time_class["datetime"]
        title_class = post.find("a", class_="result-title hdrlnk")
        url = title_class["href"]
        cl_id = title_class["data-id"]
        title_text = title_class.text
        neighborhood = post.find("span", class_="result-hood")
        if neighborhood is not None:
            neighborhood_text = neighborhood.get_text()
        else:
            neighborhood_text == "No neighborhood provided"
        result_listings = {
            "cl_id": cl_id,
            "created_at": created_at,
            "title_text": title_text,
            "price": result_price_text,
            "neighborhood_text": neighborhood_text,
            "url": url,
            "description": car_description_full,
            "jpg": image_url_jpg,
        }

        if pd.isnull(pd.to_datetime(last_scrape)):
            list_results.append(result_listings)
            print(
                f"The datetime is null. Listing posted {created_at} and last scrape at {last_scrape}."
            )
        elif pd.to_datetime(result_listings["created_at"]) > (pd.to_datetime(last_scrape)):
            list_results.append(result_listings)
            print(
                f"Listing posted {created_at} and last scrapetime {last_scrape}."
            )
        else:
            print(
                f"Listing posted {created_at} and last scrapetime {last_scrape}.")
    return list_results


def insert_into_csv_db(result_listings):
    with open("listing.csv", "a", encoding="utf-8") as csvfile:
        fieldnames = [
            "id",
            "created",
            "name",
            "price",
            "location",
            "url",
            "description",
            "jpg",
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        for item in result_listings:
            writer.writerow(
                {
                    "id": item["cl_id"],
                    "created": item["created_at"],
                    "name": item["title_text"],
                    "price": item["price"],
                    "location": item["neighborhood_text"],
                    "url": item["url"],
                    "description": item["description"],
                    "jpg": item["jpg"],
                }
            )
        csvfile.close()


def send_text_message(result_listings):
    for item in result_listings:
        if item["neighborhood_text"].strip().lower() == "(east brunswick)":
            message = client.messages.create(
                body="Car in neighborhood" + item["url"],
                from_="+12513253827",
                to="+1" + my_number,

            )


if __name__ == "__main__":
    while True:
        print("Starting Scraping :{}".format(time.ctime())
              )

        try:
            create_csv()
            c_l = craigslist_soup(
                region="cnj", term="cta", last_scrape=get_last_scrape()

            )
            insert_into_csv_db(result_listings=c_l)
            send_text_message(result_listings=c_l)
        except KeyboardInterrupt:
            print("Exiting...")
            sys.exit(1)
        except Exception as exc:
            print("Error with the scraping", sys.exc_info()[0])
            traceback.print_exc()
        in_ten_minutes = datetime.now() + timedelta(minutes=60)

        print(
            "{}: Successfully finished scraping. Next scrape will be at {}".format(time.ctime(), in_ten_minutes))

        schedule.run_pending()
        time.sleep(600)
