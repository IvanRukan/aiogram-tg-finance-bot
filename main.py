import asyncio
import concurrent.futures
import datetime
import os
import aiofiles
import gspread
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, Filter
from aiogram.types import ReplyKeyboardRemove
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from gspread import Cell


async def get_spreadsheet(name):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, client.open, name)

async def get_worksheet(name, spreadsheet):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, spreadsheet.worksheet, name)

async def async_append_row(worksheet, data):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        executor,
        lambda: worksheet.append_row(data, value_input_option='USER_ENTERED')
    )

async def async_copy(origin, target):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, origin.copy_to, target.id)
    return result

async def async_worksheet_by_id(target, copied_schema):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, target.get_worksheet_by_id, copied_schema['sheetId'])
    return result

async def async_update_cells(schema, cells):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor,
                               lambda: schema.update_cells(cells, value_input_option='USER_ENTERED')
                               )


async def async_update_title(schema, data, date):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, schema.update_title, data[0] + ' ' + date)


async def async_get_all_rows(worksheet):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, worksheet.get_all_records)
    return result


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

CREDENTIALS_FILE = "tgfinancebot-credentials.json"

creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)

client = gspread.authorize(creds)

cmd_dict = {'command_type': None, 'artist': None, 'city': None, 'date': None}

spreadsheet = None

worksheet = None

current_cmd = None

executor = concurrent.futures.ThreadPoolExecutor(max_workers=15)

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


async def read_artists():
    try:
        async with aiofiles.open('artists.txt', mode='r', encoding='utf-8') as f:
            content = await f.read()
            content = content.split(',')
            return content
    except FileNotFoundError:
        return []


async def write_artists(to_write):
    artists = await read_artists()
    artists.append(to_write)
    artists = ','.join(artists)
    async with aiofiles.open('artists.txt','w', encoding='utf-8') as f:
        await f.write(artists)


class ArtistFilter(Filter):

    async def __call__(self, message: types.Message) -> bool:
        artists = await read_artists()
        return message.text in artists


start_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/добавить_трату"), KeyboardButton(text="/добавить_событие")],
         [KeyboardButton(text="/просмотреть_траты")], [KeyboardButton(text='/помощь')]
    ],
    resize_keyboard=True
)


def get_artists_keyboard(artists):
    buttons = [[KeyboardButton(text=artist)] for artist in artists]
    buttons.append([KeyboardButton(text='/start')])
    kb = ReplyKeyboardMarkup(
        keyboard= buttons
        ,
        resize_keyboard=True
    )
    return kb


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    welcome_text = (
        '''Приветствую!
/добавить_трату - внести трату;
/добавить_событие - создать новое мероприятие;
/просмотреть_траты - посмотреть смету за период;
/помощь - руководство пользователя.
        '''
    )
    await message.answer(welcome_text, reply_markup=start_keyboard)


@dp.message(Command("добавить_трату"))
async def cmd_add_payment(message: types.Message):
    global current_cmd
    artists = await read_artists()
    artists_keyboard = get_artists_keyboard(artists)
    current_cmd = message.text
    response_text = 'Выберите артиста:'
    await message.reply(response_text, reply_markup=artists_keyboard)


@dp.message(Command('добавить_событие'))
async def cmd_add_event(message: types.Message):
    global current_cmd
    current_cmd = message.text
    await message.reply('Введите артиста и список дат в формате: АРТИСТ,гггг-мм-дд,гггг-мм-дд,гггг-мм-дд \nПример: HORUS,2025-05-22,2025-05-23,2025-05-25', reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='/start')]], resize_keyboard=True))


@dp.message(Command('просмотреть_траты'))
async def cmd_show_expenses(message: types.Message):
    global current_cmd
    current_cmd = message.text
    artists = await read_artists()
    artists_keyboard = get_artists_keyboard(artists)
    await message.reply('Выберите артиста:', reply_markup=artists_keyboard)


@dp.message(Command('помощь'))
async def cmd_help(message: types.Message):
    await message.reply('Ссылка на документацию: https://github.com/IvanRukan/aiogram-tg-finance-bot')


@dp.message(ArtistFilter())
async def handle_artist_selection(message: types.Message):
    global current_cmd
    artist = message.text
    response_text = await connect_to_spreadsheet(artist)
    if 'Установил' in response_text:
        if current_cmd == '/добавить_трату':
            response_text += 'Введите трату в формате (запятая разделитель, комментарий необязателен): \nдата,сумма,категория,кто потратил,комментарий. \nПример: 22.05.2025,500,Бытовой райдер,Кирилл,купил пиво\nДоступные категории: Технический довоз, Аренда площадки, Персонал, Гостиница, Бытовой райдер, Еда, Суточные, Билеты, Транспорт, Такси, Багаж, Доп. место.'
        elif current_cmd == '/просмотреть_траты':
            response_text += 'Введите период для трат в следующем формате: 22.05.2025,25.07.2026'
        await message.reply(response_text,reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='/start')]], resize_keyboard=True))
    else:
        await message.reply(response_text, reply_markup=start_keyboard)


async def connect_to_spreadsheet(artist):
    global client, spreadsheet, worksheet
    try:
        spreadsheet = await get_spreadsheet(artist)
        worksheet = await get_worksheet('Общие траты', spreadsheet)
        return 'Установил соединение!'
    except (SpreadsheetNotFound, WorksheetNotFound) as e:
        return f'Не нашел такого мероприятия!'


async def copy_spreadsheet(data):
    source_spreadsheet = await get_spreadsheet('Бот шаблон')
    pattern_schema = await get_worksheet('Шаблон', source_spreadsheet)
    pattern_payment = await get_worksheet('Общие траты', source_spreadsheet)
    target_spreadsheet = await get_spreadsheet(data[0])
    try:
        existing_payment = await get_worksheet('Общие траты', target_spreadsheet)
    except WorksheetNotFound:
        copied_payment = await async_copy(pattern_payment, target_spreadsheet)
        new_payment = await async_worksheet_by_id(target_spreadsheet, copied_payment)
        await async_update_title(new_payment, ['Общие'], 'траты')
    for date in data[1:]:
        copied_schema = await async_copy(pattern_schema, target_spreadsheet)
        new_schema = await async_worksheet_by_id(target_spreadsheet, copied_schema)
        cells_to_update = [
            Cell(row=1, col=1, value=data[0] + ' ' + date),
            Cell(row=5, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Технический довоз'\")); 0)"),
            Cell(row=6, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Аренда площадки'\")); 0)"),
            Cell(row=7, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Персонал'\")); 0)"),
            Cell(row=13, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Гостиница'\")); 0)"),
            Cell(row=14, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Бытовой райдер'\")); 0)"),
            Cell(row=15, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Еда'\")); 0)"),
            Cell(row=16, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Суточные'\")); 0)"),
            Cell(row=22, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Билеты'\")); 0)"),
            Cell(row=23, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Транспорт'\")); 0)"),
            Cell(row=24, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Такси'\")); 0)"),
            Cell(row=25, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Багаж'\")); 0)"),
            Cell(row=26, col=2,
                 value=f"=IFERROR(SUM(QUERY('Общие траты'!A2:C600; \"select B where A = date '{date}' and C = 'Доп. место'\")); 0)"),
        ]
        await async_update_cells(new_schema, cells_to_update)
        await async_update_title(new_schema, data, date)


async def get_expenses_by_dates(start, end):
    global worksheet
    start = datetime.datetime.strptime(start, "%d.%m.%Y")
    end = datetime.datetime.strptime(end, "%d.%m.%Y")
    data = await async_get_all_rows(worksheet)
    result = {}
    for row in data:
        try:
            date = datetime.datetime.strptime(row.get('Дата'), "%d.%m.%Y")
        except ValueError:
            date = datetime.datetime.strptime(row.get('Дата'), "%d-%m-%Y")
        if start <= date <= end:
            category = row.get('Категория')
            amount = row.get('Сумма')
            if category not in result.keys():
                result[category] = amount
                continue
            result[category] += amount
    return result


@dp.message()
async def message_handling(message: types.Message):
    global worksheet, current_cmd, spreadsheet
    try:
        data = message.text.split(',')
    except AttributeError:
        await message.reply('Спасибо за стикер! Но выбери команду из списка!', reply_markup=start_keyboard)
        return
    if current_cmd == '/добавить_трату':
        try:
            date_obj = datetime.datetime.strptime(data[0], "%d.%m.%Y")
            data[0] = date_obj.strftime("%d-%m-%Y")
            int(data[1])
            await async_append_row(worksheet, data)
            current_cmd = None
            await message.reply('Запись успешно добавлена!', reply_markup=start_keyboard)
        except (AttributeError, ValueError):
            await message.reply('Соединение еще не установлено или формат ввода неверен!', reply_markup=start_keyboard)
    elif current_cmd == '/добавить_событие':

        current_cmd = None
        try:
            await copy_spreadsheet(data)
            artists = await read_artists()
            if data[0] not in artists:
                await write_artists(data[0])
            await message.reply('Успешно создал листы под мероприятие!', reply_markup=start_keyboard)
        except SpreadsheetNotFound:
            await message.reply('Сначала создайте таблицу с соответствующим артистом!', reply_markup=start_keyboard)
    elif current_cmd == '/просмотреть_траты':
        try:
            result = await get_expenses_by_dates(data[0], data[1])
            reply_text = ''
            for key, val in result.items():
                reply_text += f'{key}: {val}\n'
            reply_text += f'Всего: {sum(result.values())}'
            await message.answer(reply_text, reply_markup=start_keyboard)
        except IndexError:
            await message.reply('Формат ввода неверен! Повторите операцию.', reply_markup=start_keyboard)
    else:
        current_cmd = None
        await message.reply('Выберите команду!')


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
