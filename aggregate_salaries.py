import asyncio
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta

from motor.motor_asyncio import AsyncIOMotorClient


async def aggregate_salaries(dt_from, dt_upto, group_type):
    # Подключение к MongoDB
    client = AsyncIOMotorClient("mongodb://localhost:27017/")
    db = client["sampleDB"]
    collection = db["sample_collection"]

    # Запрос данных из коллекции
    query = {"dt": {"$gte": dt_from, "$lte": dt_upto}}

    # Агрегация данных
    pipeline = [
        {
            "$match": query
        },
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$dt",
                        "unit": group_type
                    }
                },
                "total": {"$sum": "$value"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "total": 1
            }
        },
        {
            "$sort": {"date": 1}
        }
    ]

    aggregated_data = await collection.aggregate(pipeline).to_list(None)

    dict = {}
    for data in aggregated_data:
        dict[data["date"]] = data["total"]

    current_date = dt_from
    while current_date <= dt_upto:

        if current_date not in dict.keys():
            dict[current_date] = 0

        if group_type == 'day':
            current_date += timedelta(days=1)
        elif group_type == 'hour':
            current_date += timedelta(hours=1)
        elif group_type == 'month':
            current_date += relativedelta(months=1)

    sorted_values = sorted(dict.items())
    # # Формирование ответа
    # dataset = [data["total"] for data in aggregated_data]
    # labels = [data["date"] for data in aggregated_data]

    labels = []
    dataset = []
    for tup in sorted_values:
        labels.append(tup[0].isoformat())
        dataset.append(tup[1])

    return {"dataset": dataset, "labels": labels}


async def main():
    # dt_from = datetime(2022, 9, 1)
    # dt_upto = datetime(2022, 12, 31, 23, 59)
    # group_type = 'month'

    dt_from = datetime(2022, 10, 1)
    dt_upto = datetime(2022, 11, 30, 23, 59)
    # group_type = 'hour'
    group_type = 'day'

    result = await aggregate_salaries(dt_from, dt_upto, group_type)
    print(result)

# Запуск асинхронного кода
loop = asyncio.get_event_loop()
loop.run_until_complete(main())