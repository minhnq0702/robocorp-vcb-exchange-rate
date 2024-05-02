# -*- coding: utf-8 -*-
import os
from collections.abc import Callable
from typing import ParamSpec

from robocorp import log, workitems
from robocorp.tasks import task
from RPA.Excel.Files import Files
from RPA.Tables import Tables

from .constants import OUTPUT_FOLDER
from .kafka import KafkaManager

OUT_VCB_RATE_FILE = "rate_data.xlsx"
table = Tables()
wb = Files()


@task
def submit_exchange_rate():
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
    submit_processor, final_func = get_processor()
    if not submit_processor:
        log.info('No processor found')
        return rate_date, rate_data
    
    for item in workitems.inputs:
        payload = item.payload

        if isinstance(payload, dict):
            try:
                submit_processor(
                    payload,
                )
            except Exception as e:
                item.fail(str(e))
            else:
                item.done()

    if final_func is not None:
        final_func()

    return rate_date, rate_data


def get_processor() -> tuple[Callable[[dict[str, float | str]], None] | None, Callable | None]:
    """Return a processor function based on the environment

    Returns:
        Callable[[dict[str, float | str], **kwargs], Callable | None]: Function to process rate data
    """
    push_method = os.environ.get('PUSH_METHOD', None)
    if push_method == 'kafka':
        return push_data_to_kafka, close_kafka_producer
    elif push_method == 'excel':
        return push_data_to_excel, submit_data
    # elif push_method == 'api':
    #     return push_data_to_api
    return None, None
    

# * -- Excel --
def push_data_to_excel(data: dict[str, float | str]):
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


def submit_data():
    try:
        wb.save_workbook()
    except Exception as e:
        log.exception(str(e))


# * -- KAFKA --
def push_data_to_kafka(data: dict[str, float | str]):
    kafka_manager = KafkaManager()
    # Use the kafka_manager instance to push data to Kafka
    topic = str(os.environ.get('KAFKA_TOPIC', 'rate_data'))
    kafka_manager.push_data(
        'ExchangeRate',
        data, 
        topic,
    )


def close_kafka_producer():
    kafka_manager = KafkaManager()
    kafka_manager.close_producer()


def push_data_to_api(data: dict[str, float | str]):
    pass
