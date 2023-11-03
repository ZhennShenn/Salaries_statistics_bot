import asyncio
import datetime
import json

from aiogram import Bot, Dispatcher, types

from aggregate_salaries import aggregate_salaries
from config import TG_TOKEN

# Создание экземпляра бота
bot = Bot(token=TG_TOKEN)
dp = Dispatcher()


# Обработчик текстовых сообщений
@dp.message()
async def message_processing(message: types.Message):
    try:
        # Получение JSON из текстового сообщения
        data = json.loads(message.text)

        # Извлечение входных данных из JSON
        dt_from = datetime.datetime.fromisoformat(data["dt_from"])
        dt_upto = datetime.datetime.fromisoformat(data["dt_upto"])
        group_type = data["group_type"]

        # Вызов функции агрегации
        result = await aggregate_salaries(dt_from, dt_upto, group_type)

        # Отправка агрегированных данных в ответ
        await message.reply(json.dumps(result))

    except Exception as e:
        await message.reply("Произошла ошибка при обработке запроса.")


# Запуск бота
async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
