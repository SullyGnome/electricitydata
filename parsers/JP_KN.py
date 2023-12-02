#!/usr/bin/env python3
import re
from datetime import datetime
from io import BytesIO
from logging import Logger, getLogger
from urllib.request import Request, urlopen

# The arrow library is used to handle datetimes
import arrow
from bs4 import BeautifulSoup
from PIL import Image
from pytesseract import image_to_string

# The request library is used to fetch content through HTTP
from requests import Session

from .JP import fetch_production as JP_fetch_production

# please try to write PEP8 compliant code (use a linter). One of PEP8's
# requirement is to limit your line length to 79 characters.


def fetch_production(
    zone_key: str = "JP-KN",
    session: Session | None = None,
    target_datetime: datetime | None = None,
    logger: Logger = getLogger(__name__),
):
    """
    This method adds nuclear production on top of the solar data returned by the JP parser.
    It tries to match the solar data with the nuclear data.
    If there is a difference of more than 30 minutes between solar and nuclear data, the method will fail.
    """
    session = session or Session()
    if target_datetime is not None:
        raise NotImplementedError("This parser can only fetch live data")

    JP_data = JP_fetch_production(zone_key, session, target_datetime, logger)
    nuclear_mw, nuclear_datetime = get_nuclear_production()
    latest = JP_data[
        -1
    ]  # latest solar data is the most likely to fit with nuclear production
    diff = None
    if nuclear_datetime > latest["datetime"]:
        diff = nuclear_datetime - latest["datetime"]
    else:
        diff = latest["datetime"] - nuclear_datetime
    if abs(diff.seconds) > 60 * 60:
        raise Exception("Difference between nuclear datetime and JP data is too large")

    latest["production"]["nuclear"] = nuclear_mw
    latest["production"]["unknown"] = latest["production"]["unknown"] - nuclear_mw
    return latest


URL = (
    "https://www.kepco.co.jp/energy_supply/energy/nuclear_power/info/monitor/live_unten"
)
IMAGE_CORE_URL = "https://www.kepco.co.jp/"


def getImageText(imgUrl, lang):
    """
    Fetches image based on URL, crops it and extract text from the image.
    """
    req = Request(imgUrl, headers={"User-Agent": "Mozilla/5.0"})
    img_bytes = urlopen(req).read()
    img = Image.open(BytesIO(img_bytes))
    width, height = img.size
    img = img.crop((0, 0, 160, height))
    # cropping the image, makes it easier to read for tesseract

    text = image_to_string(img, lang=lang)

    return text

def getNukeTimeFromImage(imgUrl, lang):
    req = Request(imgUrl, headers={"User-Agent": "Mozilla/5.0"})
    img_bytes = urlopen(req).read()
    img = Image.open(BytesIO(img_bytes))
    img.convert('RGB').save("Orig.jpg")
    width, height = img.size
    img = img.crop((0, 5, 160, height))
    

    img.paste(0, (37,0,49,height))
    img.paste(0, (69,0,83,height))
    img.paste(0, (113,0,128,height))
    img.paste(0, (146,0,164,height))
    
    img = img.resize((int(width*0.85), height))
    img.convert('RGB').save("Orig_filled.jpg") 
    
    text = image_to_string(img, lang=lang, config='--psm 4')

    return text

def extractCapacity(tr):
    """
    The capacity for each unit has the class "list03".
    and it uses the chinese symbol for 10k(万).
    If this changes, the method will become inaccurate.
    """
    td = tr.findAll("td", {"class": "list03"})
    if len(td) == 0:
        return None
    raw_text = td[0].getText()
    kw_energy = raw_text.split("万")[0]
    return float(kw_energy) * 10000


def extractOperationPercentage(tr):
    """Operation percentage is located on images of type .gif"""
    td = tr.findAll("img")
    if len(td) == 0:
        return None
    img = td[0]
    URL = IMAGE_CORE_URL + img["src"]
    if ".gif" in URL:
        text = getImageText(URL, "eng")
        # will return a number and percentage eg ("104%"). Sometimes a little more eg: ("104% 4...")
        split = text.split("%")
        if len(split) == 0:
            return None
        return float(split[0]) / 100
    else:
        return None


def extractTime(soup):
    """
    Time is located in an image.
    Decipher the text containing the data and assumes there will only be 4 digits making up the datetime.
    """
    imgRelative = soup.findAll("img", {"class": "time-data"})[0]["src"]
    imgUrlFull = IMAGE_CORE_URL + imgRelative

    #text = getNukeTimeFromImage(imgUrlFull, "eng")
    #text = re.sub('[^0-9]',' ', text)
    #print(text)

    #https://www.kepco.co.jp/energy_supply/energy/nuclear_power/info/monitor/live_unten/
    return arrow.now(tz="Asia/Tokyo")

    digits = re.findall(r"\d+", text)
    digits = list(map(lambda x: int(x), digits))
    if len(digits) != 4:
        # something went wrong while extracting time from Japan
        raise Exception("Something went wrong while extracting local time")
    nuclear_datetime = (
        arrow.now(tz="Asia/Tokyo")
        .replace(month=digits[0], day=digits[1], hour=digits[2], minute=digits[3])
        .floor("minute")
        .datetime
    )
    return nuclear_datetime


def get_nuclear_production():
    """
    Fetches all the rows that contains data of nuclear units and calculates the total kw generated by all plants.
    Illogically, all the rows has the class "mihama_realtime" which they might fix in the future.
    """
    
    r = Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    html = urlopen(r).read()
    soup = BeautifulSoup(html, "html.parser")
    nuclear_datetime = extractTime(soup)
    _rows = soup.findAll(
        "tr", {"class": "mihama_realtime"}
    )  # TODO: Should we just remove this?
    tr_list = soup.findAll("tr")
    total_kw = 0
    for tr in tr_list:
        capacity = extractCapacity(tr)
        operation_percentage = extractOperationPercentage(tr)
        if capacity is None or operation_percentage is None:
            continue
        kw = capacity * operation_percentage
        total_kw = total_kw + kw
    nuclear_mw = total_kw / 1000.0  # convert to mw
    return (nuclear_mw, nuclear_datetime)


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print("fetch_production() ->")
    print(fetch_production())
