from datetime import timedelta
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

    agg_dict = {}
    for data in aggregated_data:
        agg_dict[data["date"]] = data["total"]

    current_date = dt_from
    while current_date <= dt_upto:

        if current_date not in agg_dict.keys():
            agg_dict[current_date] = 0

        if group_type == 'day':
            current_date += timedelta(days=1)
        elif group_type == 'hour':
            current_date += timedelta(hours=1)
        elif group_type == 'month':
            current_date += relativedelta(months=1)

    sorted_values = sorted(agg_dict.items())

    labels = []
    dataset = []
    for tup in sorted_values:
        labels.append(tup[0].isoformat())
        dataset.append(tup[1])

    return {"dataset": dataset, "labels": labels}
