# -*- coding: utf-8 -*-
import datetime
import xml.etree.cElementTree as ET
from os import path
from xml.etree.ElementTree import Element

import pytz
from robocorp import log, workitems
from robocorp.tasks import task
from RPA.HTTP import HTTP

from .constants import OUTPUT_FOLDER

VCB_URL = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10"
VCB_RATE_FILE = "vcb_rate.xml"


@task
def minimal_task():
    """
    A minimal task that does nothing.
    """
    http = HTTP()
    file_path = path.join(OUTPUT_FOLDER, VCB_RATE_FILE)
    file = http.download(VCB_URL, target_file=file_path, overwrite=True)
    log.info(file)
    rate_date, rate_data = read_xml_data(file_path)
    if rate_date and rate_data:
        create_work_items(rate_date, rate_data)
        log.info("Work items created successfully")


def read_xml_data(file_path):
    """Read data from XML file

    Args:
        file_path (str): XML file path

    Returns:
        tuple[str, list]: tuple of rate date and rate data
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    rate_date = None
    rate_data = []
    for child in root:
        tag = child.tag
        if tag == 'DateTime':
            date_time = _get_datetime_element(child)
            if not date_time:
                continue
            rate_date = date_time.strftime("%Y-%m-%d %H:%M:%S")
            # workitems.outputs.create({
            #     'date_time': date_time.strftime("%Y-%m-%d %H:%M:%S"),
            # })
        elif tag == 'Exrate':
            code, buy, transfer, sell = _get_ex_rate(child)
            if not code:
                continue
            rate_data.append({
                'currency_code': code,
                'buy': buy,
                'transfer': transfer,
                'sell': sell,
            })

    if not rate_date:
        log.info("No rate date found")
        return None, None
    return rate_date, rate_data


def _get_datetime_element(xml_child: Element) -> datetime.datetime | None:
    """Get datetime from XML element

    Args:
        xml_child (Element): Element contains datetime

    Returns:
        datetime.datetime | None: datetime object
    """
    format_str = "%m/%d/%Y %I:%M:%S %p"
    dt_str = xml_child.text
    if not dt_str:
        return None

    dt = datetime.datetime.strptime(dt_str, format_str)
    dt = pytz.timezone('Asia/Ho_Chi_Minh').localize(dt)
    return dt.astimezone(pytz.UTC)


def _get_ex_rate(xml_child: Element) -> tuple[str, float | None, float | None, float | None]:
    """
    Get exchange rate from XML element

    Args:
        xml_child (Element): Element contains exchange rate

    Returns:
        tuple[str, float | None, float | None, float | None]: Tuple of Code, buy, transfer, sell exchange rate
    """
    ex_rate_str = xml_child.attrib

    if not ex_rate_str:
        return ('', None, None, None)

    currency_code = ex_rate_str.get('CurrencyCode', None)
    if currency_code is None:
        return ('', None, None, None)

    ex_buy = ex_rate_str.get('Buy', '')
    ex_transfer = ex_rate_str.get('Transfer', '')
    ex_sell = ex_rate_str.get('Sell', '')
    return (
        currency_code,
        _check_currency_number_format(ex_buy),
        _check_currency_number_format(ex_transfer),
        _check_currency_number_format(ex_sell),
    )


def _check_currency_number_format(str_amount: str) -> float | None:
    """
    Check if the currency number is in correct format
    Value is in Vietnam Currency Number Format: 25,118.00

    Args:
        str_amount (str): Currency number in string format

    Returns:
        float | None: Currency number in float format or None if the input is invalid
    """
    if str_amount:
        try:
            return float(str_amount.replace(',', ''))
        except ValueError:
            return None
    return None


def create_work_items(rate_date: str, rate_data: list[dict[str, float]]):
    """Create work items

    Args:
        rate_date (str): Rate date
        rate_data (list): List of rate data
    """
    for data in rate_data:
        workitems.outputs.create({
            'rate_date': rate_date,
            **data,
        })


if __name__ == "__main__":
    file_path = path.join(OUTPUT_FOLDER, VCB_RATE_FILE)
    read_xml_data(file_path)
