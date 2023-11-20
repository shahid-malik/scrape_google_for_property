import re
import csv
import favicon
import urllib
import os.path
import requests
import pandas as pd
from os import listdir
from os.path import isfile, join

from config import *
from utils import get_domain_from_url, get_email
from requests_html import HTMLSession


def get_source(url):

    try:
        session = HTMLSession()
        response = session.get(url)
        return response

    except requests.exceptions.RequestException as e:
        print(e)


def scrape_google(query):

    query = urllib.parse.quote_plus(query)
    response = get_source("https://www.google.co.uk/search?q=" + query)

    links = list(response.html.absolute_links)
    google_domains = ('https://www.google.',
                      'https://google.',
                      'https://webcache.googleusercontent.',
                      'http://webcache.googleusercontent.',
                      'https://policies.google.',
                      'https://support.google.',
                      'https://maps.google.')

    for url in links[:]:
        if url.startswith(google_domains):
            links.remove(url)

    return links


def get_results(query):
    query = urllib.parse.quote_plus(query)
    response = get_source(input_url + query)
    return response


def parse_results(response):
    css_identifier_result = ".tF2Cxc"
    css_identifier_title = "h3"
    css_identifier_link = ".yuRUbf a"
    results = []
    try:
        results = response.html.find(css_identifier_result)
    except:
        pass
    output = []

    for result in results:
        try:
            item = {
                'title': result.find(css_identifier_title, first=True).text,
                'link': result.find(css_identifier_link, first=True).attrs['href'],
                # 'text': result.find(css_identifier_text, first=True).text
            }
            output.append(item)
        except Exception as e:
            print(e)
    print("Total {} items scrapped".format(len(results)))
    return [output, len(results)]


def combine_unique_data():
    cities = [f for f in listdir(output_data_dir) if isfile(join(output_data_dir, f))]
    li = []

    for city in cities:
        try:
            city = ".".join(city.split(".")[:-1])
            city_output_domains_file = output_data_dir+city+"_search_domains"+output_file_ext
            df = pd.read_csv(city_output_domains_file, index_col=None, header=0)
            li.append(df)
        except:
            pass

    frame = pd.concat(li, axis=0, ignore_index=True)

    frame.to_csv(final_output_file_v3)


def combine_all_data():
    cities = [f for f in listdir(output_data_dir) if isfile(join(output_data_dir, f))]
    li = []

    for city in cities:
        try:
            city = ".".join(city.split(".")[:-1])
            city_output_domains_file = output_data_dir+city+output_file_ext
            df = pd.read_csv(city_output_domains_file, index_col=None, header=0)
            li.append(df)
        except:
            pass
    frame = pd.concat(li, axis=0, ignore_index=True)
    frame.to_csv(all_output_file_v3)


def google_search(query):

    # step 1 - create output csv files per city
    # if first_run:
    #     create_output_city_file()

        # step 2 - read city and search for each city
        # query_city()

    # step 3 - Remove duplicated, have only root urls (domains)
    # remove_duplicates_n_data_cleaning(output_file=final_output_dir_v3)

    # step 4 - Combine data from individual to one file
    # combine_unique_data()
    # combine_all_data()

    # step 5 - Remove duplicated, have only root urls (domains)
    # remove_duplicates_n_data_cleaning_v3(output_file=final_output_file_clean_v3)

    # step 6
    get_contact_details_v3()


def query_city():
    f = open(input_file, "r", encoding="utf-8")
    cities = f.readlines()
    for city in cities:
        city_record_len = 0
        city = city.split(",")[1]
        print("*********** start scraping data for {}".format(city))
        initial_city_search_query = search_query + city
        for i in range(pages_to_scrape):
            if i > 0:
                page_no = i*10
                city_search_query_with_page = initial_city_search_query
                city_search_query_with_page = city_search_query_with_page + "&start={}".format(page_no)
                city_search_query = city_search_query_with_page
            else:
                city_search_query = initial_city_search_query
            # results = google_search(city_search_query)
            response = get_results(city_search_query)
            results, records_len = parse_results(response)
            write_results(results, city)
            city_record_len = city_record_len+records_len
            if city_record_len>50:
                break
        print("*********** End scraping data successfully for {} with total records {}/n".format(city, city_record_len))


def create_output_city_file():
    """
    Create city output empty file
    :return:
    """
    f = open(input_file, "r", encoding="utf-8")
    cities = f.readlines()
    for city in cities:
        city_name = city.split(",")[1]
        city_output_file = output_data_dir + "/" + city_name + output_file_ext
        if not os.path.isfile(city_output_file):
            empty_output_file = open(city_output_file, "w")
            empty_output_file.close()


def write_results(results, city):
    """
    Write results to respective city output file.Every city has its own results
    :param results:
    :param city:
    :return:
    """
    columns = ['title', 'url']
    output_file = output_data_dir + city + output_file_ext
    with open(output_file, 'a', encoding='UTF8', newline='') as f:
        search_results = [list(a.values()) for a in results]
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(search_results)


def remove_duplicates_n_data_cleaning(output_file):
    """
    Remove any duplicate domains/websites to save time.
    :param output_file:
    :return:
    """
    cities = [f for f in listdir(output_data_dir) if isfile(join(output_data_dir, f))]

    for city in cities:
        try:
            # city = city.split(".")[0]
            city = ".".join(city.split(".")[:-1])
            output_file = output_data_dir+city+output_file_ext
            city_output_domains_file = output_data_dir+city+"_search_domains"+output_file_ext
            df = pd.read_csv(output_file, index_col=0)
            df['domain'] = df.apply(lambda row: get_domain_from_url(row), axis=1)
            df['iframe_exist'] = False
            df['email'] = ""
            df['phone'] = ""
            df['logo'] = ""
            print("Total records before removing duplicates: {}".format(df.shape[0]))
            df_updated = df.drop_duplicates(subset=['domain'])
            df_updated.drop('url', inplace=True, axis=1)
            df_updated.to_csv(city_output_domains_file)
            print("Total records after removing duplicates: {}".format(df_updated.shape[0]))

        except:
            pass


def remove_duplicates_n_data_cleaning_v3(output_file):
    """
    Remove any duplicate domains/websites to save time.
    :param output_file:
    :return:
    """
    df = pd.read_csv(final_output_file_v3, encoding='cp1252', index_col=0)
    df['domain'] = df.apply(lambda row: get_domain_from_url(row), axis=1)
    df['iframe_exist'] = False
    df['email'] = ""
    df['phone'] = ""
    df['logo'] = ""
    print("Total records before removing duplicates: {}".format(df.shape[0]))
    df_updated = df.drop_duplicates(subset=['domain'])
    df_updated.drop('url', inplace=True, axis=1)
    df_updated.to_csv(final_output_file_clean_v3)
    print("Total records after removing duplicates: {}".format(df_updated.shape[0]))


def get_contact_details():
    """
    Get contact details like email, telephone for each city
    :return:
    """
    f = open(input_file, "r", encoding="utf-8")
    cities = f.readlines()
    for city in cities:
        city = city.split(",")[1]
        city_output_domains_file = output_data_dir+city+"_search_domains"+output_file_ext

        print("---------------------------processing now: {}".format(city_output_domains_file))
        df = pd.read_csv(city_output_domains_file)
        telephone = ""
        email = ""
        web_favicon = ""
        iframe_included = False
        for i, row in df.iterrows():
            try:
                domain = row['domain']
                print("Processing now: {}".format(domain))
                try:
                    url = "http://" + domain
                except:
                    continue
                try:
                    r = requests.get(url, timeout=3)
                    site_text = r.text
                    web_favicon = favicon.get(url, timeout=2)
                    web_favicon = web_favicon[0].url
                    print("------ Fetched logo")
                    email = get_email(r.content, url)
                    print("------ Fetched email")
                    # telephone = get_telephone(site_text)
                    # print("------ Fetched telephone")
                    iframe_included = is_iframe_included(site_text)
                    print("------ Fetched iframe status")

                except Exception as e:
                    print(e)
                df.at[i, 'email'] = email
                df.at[i, 'iframe_exist'] = iframe_included
                df.at[i, 'logo'] = web_favicon
            except:
                pass
        df.to_csv(city_output_domains_file)


def get_contact_details_v3():
    """
    Get contact details like email, telephone for each city
    :return:
    """

    df = pd.read_csv(final_output_file_clean_v3_missing)
    telephone = ""
    email = ""
    web_favicon = ""
    iframe_included = False
    i = 0
    for i, row in df.iterrows():
        domain = row['domain']
        print("{} Processing now: {}".format(i, domain))
        try:
            url = "http://" + domain
        except:
            continue
        try:
            req_session = requests.Session()
            r = req_session.get(url, timeout=3)
            site_text = r.text
            web_favicon = favicon.get(url, timeout=2)
            web_favicon = web_favicon[0].url
            print("------ Fetched logo")
            email = get_email(r.content, url)
            print("------ Fetched email")
            # telephone = get_telephone(site_text)
            # print("------ Fetched telephone")
            iframe_included = is_iframe_included(site_text)
            print("------ Fetched iframe status")
        except Exception as e:
            iframe_included = -1
            email = -1
            web_favicon = -1
        df.at[i, 'email'] = email
        df.at[i, 'iframe_exist'] = iframe_included
        df.at[i, 'logo'] = web_favicon

        with open(final_output_file_clean_v5, 'a', newline="", encoding="utf-8") as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(list(df.iloc[i]))
        i = i+1
    df.to_csv(final_output_file_clean_v4)


def get_telephone(site_text):
    """
    Get telephone from the website
    :param site_text:
    :return:
    """
    regex = r".*(?:\+49|0049|\+\(49\)|\+43|0043|\+\(43\)).*"
    matches = re.finditer(regex, site_text, re.MULTILINE)
    all_contacts = []
    for matchNum, match in enumerate(matches, start=1):
        all_contacts = []
        phone_no = match.group()
        phone_no = re.sub("[^0-9]", "", phone_no)
        all_contacts.append(phone_no)
    all_contacts = list(set(all_contacts))
    telephone = ",".join(all_contacts)
    return telephone


def is_iframe_included(text):
    """
    Check if iframe exist on the website
    :param text:
    :return:
    """
    if iframe_code in text:
        return True
    if ifrome_link in text:
        return True
    return False
