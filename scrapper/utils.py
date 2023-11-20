import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re


def get_domain_from_url(url):
    """
    Get domain from Url
    :param url:
    :return:
    """
    domain = urlparse(url[0]).netloc
    return domain


def get_email(content, page):
    soup = BeautifulSoup(content, "lxml")
    text = soup.get_text()
    emails = re.findall(r'[a-z0-9]+@[gmail|yahoo|outlook].com', text)
    domain = urlparse(page).netloc.replace("www.", "")
    re_exp = r'[a-z0-9]+@{}'.format(domain)
    emails_to = re.findall(re_exp, text)
    email_list = emails+emails_to
    emails = ",".join(email_list)
    return emails


def get_phone(soup, page):
    response = requests.get(page, timeout=2)
    try:
        phone = soup.select("a[href*=callto]")[0].text
        return phone
    except:
        pass

    try:
        # phone = re.findall(r'\(?\b[0-9][0-9]{2}\)?[-][0-9][0-9]{2}[-][0-9]{4}\b', response.text)[0]
        phone = re.findall(r'^(?:(\+49|0049|\+\(49\)|\(\+49\))?[ ()\/-]*(?(1)|0)1(?:5[12579]|6[023489]|7[0-9])|(\+43|0043|\+\(43\)|\(\+43\))?[ ()\/-]*(?(2)|0)6(?:64|(?:50|6[0457]|7[0678]|8[0168]|9[09])))[ ()\/-]*\d(?:[ \/-]*\d){6,7}\s*$', response.text)[0]
        return phone
    except:
        pass

    try:
       phone = re.findall(r'\(?\b[0-9][0-9]{2}\)?[-. ]?[0-9][0-9]{2}[-. ]?[0-9]{4}\b', response.text)[-1]
       return phone
    except:
        print('Phone number not found')
        phone = ''
        return phone

