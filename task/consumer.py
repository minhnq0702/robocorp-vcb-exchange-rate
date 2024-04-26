# -*- coding: utf-8 -*-
import os
from collections.abc import Callable

from robocorp import log, workitems
from robocorp.tasks import task
from RPA.Excel.Files import Files
from RPA.Tables import Tables

from .constants import OUTPUT_FOLDER

OUT_VCB_RATE_FILE = "rate_data.xlsx"
table = Tables()
wb = Files()


@task
def minimal_task():
    """
    A minimal task that does nothing.
    """
    process_rate_data()


def process_rate_data():
    """Get rate data from work items

    Returns:
        tuple[str, list]: tuple of rate date and rate data
    """
    rate_date = None
    rate_data = []
    for item in workitems.inputs:
        payload = item.payload

        if isinstance(payload, dict):
            try:
                get_processor()(payload)
            except Exception as e:
                item.fail(str(e))
            else:
                item.done()

    submit_data()

    return rate_date, rate_data


def get_processor() -> Callable[[dict[str, float | str]], None]:
    """Return a processor function based on the environment

    Returns:
        Callable[[dict[str, float | str]], None]: Function to process rate data
    """
    return push_data_to_excel
    # return push_data_to_api


def push_data_to_excel(data: dict):
    try:
        wb.get_active_worksheet()
    except:
        wb.create_workbook(
            os.path.join(OUTPUT_FOLDER, OUT_VCB_RATE_FILE),
            sheet_name='rate_data',
        )
        wb.create_worksheet(
            'rate_data',
            exist_ok=True,
            content=dict.fromkeys([
                'rate_date', 'currency_code', 'buy', 'transfer', 'sell'
            ]),
            header=True,
        )
        wb.set_active_worksheet('rate_data')
        wb.delete_rows(2)

    wb.append_rows_to_worksheet(
        data,
        header=True,
        formatting_as_empty=True,
    )


def push_data_to_api(data: dict):
    pass


def submit_data():
    try:
        wb.save_workbook()
    except Exception as e:
        log.exception(str(e))

