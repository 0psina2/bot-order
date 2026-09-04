"""
gift_sniper.py
==============

Бот-снайпер для ОФИЦИАЛЬНОГО Gift Marketplace внутри Telegram
(тот самый маркет коллекционных подарков, встроенный в сам Telegram —
НЕ Portals и не другие сторонние мини-аппы).

Работает под твоим личным аккаунтом (не бот-токеном), т.к. владеть и
покупать коллекционные NFT-подарки на баланс аккаунта может только
пользовательский аккаунт.

Использует официальные, задокументированные MTProto-методы Telegram:
  - payments.getStarGifts          -> список типов подарков (узнать gift_id)
  - payments.getResaleStarGifts    -> лоты конкретного типа подарка на продаже
  - payments.getPaymentForm        -> получить форму оплаты для конкретного лота
  - payments.sendStarsForm         -> оплатить звёздами и завершить покупку

Документация: https://core.telegram.org/api/gifts

-----------------------------------------------------------------------
УСТАНОВКА
-----------------------------------------------------------------------
1. pip install --upgrade telethon
   (нужна свежая версия, эти методы добавлены в 2025 году)

2. Получи api_id и api_hash на https://my.telegram.org -> API development tools

3. Впиши их ниже в CONFIG (или через переменные окружения TG_API_ID / TG_API_HASH)

4. По умолчанию TARGET_GIFT_IDS = [] — бот сканирует ВСЕ типы подарков на
   маркете и хватает любой лот с ценой <= MAX_PRICE_STARS (сейчас 250★).
   Если хочешь сузить до конкретных коллекций — сначала посмотри список:
       python gift_sniper.py --list-gifts
   и впиши нужные gift_id в TARGET_GIFT_IDS = [123, 456, ...].

5. Дальше запускай:
       python gift_sniper.py --watch
   При первом запуске Telethon попросит номер телефона, код из Telegram
   и пароль 2FA (если включён) — это нормально, так создаётся сессия
   (файл .session), после чего логиниться заново не нужно.

-----------------------------------------------------------------------
ЗАПУСК НА RAILWAY / СЕРВЕРЕ (без интерактивного ввода)
-----------------------------------------------------------------------
На сервере нет интерактивной консоли, поэтому обычный client.start()
там не сработает (упадёт с EOFError при попытке спросить телефон).

Шаги:
1. У СЕБЯ НА КОМПЬЮТЕРЕ (не на Railway) запусти один раз:
       python generate_session.py
   Он попросит телефон/код/2FA как обычно и в конце распечатает длинную
   строку — это твоя "строка сессии" (StringSession).

2. На Railway (Variables / Environment) добавь переменную:
       TG_SESSION_STRING = <вставь скопированную строку целиком>
   а также (если ещё не заданы):
       TG_API_ID = 39527734
       TG_API_HASH = 83385c56c5bba6da3b28e19831f3b55b

3. Деплой gift_sniper.py как обычно, команда запуска:
       python gift_sniper.py --watch
   Скрипт увидит TG_SESSION_STRING и залогинится без единого вопроса.

ВАЖНО: строка сессии — это как пароль от аккаунта целиком. Никогда не
публикуй её в открытом репозитории, не пиши в чат, храни только в
переменных окружения Railway (они приватные).

-----------------------------------------------------------------------
ВАЖНЫЕ ОГОВОРКИ
-----------------------------------------------------------------------
- Это НЕ гарантия "успеть первым". Если другие люди/боты тоже мониторят
  тот же тип подарка, выигрывает тот, чей запрос физически быстрее дошёл
  до серверов Telegram (сеть, пинг, частота опроса). Скрипт даёт
  техническую возможность, а не гарантию.
- Слишком частый опрос (POLL_INTERVAL слишком маленький) может привести
  к FLOOD_WAIT — Telegram временно заблокирует запросы. Начни с
  1-2 секунд и смотри на реакцию сервера.
- ВСЕГДА сначала протестируй с DRY_RUN = True, чтобы увидеть, что бот
  находит и по какой логике, прежде чем разрешать реальные покупки.
- Скрипт покупает через баланс Stars, привязанный к твоему аккаунту.
  Проверяй баланс заранее (Settings -> My Stars в приложении Telegram).
"""

import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional

from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession
from telethon.errors import RPCError, FloodWaitError

# ----------------------------------------------------------------------
# CONFIG — отредактируй под себя
# ----------------------------------------------------------------------

API_ID = int(os.environ.get("TG_API_ID", "39527734"))
API_HASH = os.environ.get("TG_API_HASH", "83385c56c5bba6da3b28e19831f3b55b")
SESSION_NAME = "gift_sniper_session"                      # файл сессии (для локального запуска)
SESSION_STRING = os.environ.get("TG_SESSION_STRING", "")   # строка сессии (для запуска на Railway/сервере,
                                                            # без интерактивного ввода — см. generate_session.py)

TARGET_GIFT_IDS = []         # Впиши конкретные gift_id (числа), чтобы сузить вручную.
TARGET_GIFT_NAMES = [
    "Chill Flame",
    "Vice Cream",
    "Mood Pack",
    "Xmas Stocking",
    "Lunar Snake",
    "Candy Cane",
    "Happy Brownie",
    "Swag Bag",
    "Whip Cupcake",
    "Clover Pin",
    "Snake Box",
    "Faith Amulet",
]
MAX_PRICE_STARS = 250        # покупать только если цена <= этого значения (в Stars).
ATTRIBUTES_FILTER = None    # опционально: список StarGiftAttributeId для фильтра по модели/фону и т.д.
                             # (актуально только если TARGET_GIFT_IDS содержит один конкретный id)

POLL_INTERVAL_SECONDS = 2.0   # пауза между полными циклами сканирования ВСЕХ типов
PER_TYPE_DELAY_SECONDS = 1.2 # маленькая пауза между запросами разных типов внутри одного цикла,
                               # чтобы не улететь во FLOOD_WAIT при большом числе коллекций
PAGE_LIMIT = 5                 # сколько самых дешёвых лотов забирать за раз на каждый тип (уже отсортированы по цене)

MAX_BUYS_PER_RUN = None       # None = без ограничения. Можно поставить число, чтобы бот остановился
                               # после N успешных покупок (защита от опустошения баланса по ошибке).

DRY_RUN = False   # True = только логировать находки, НЕ покупать. Поставь False, когда убедишься, что всё работает верно.

SHOW_FLOORS_EVERY_CYCLE = True  # True = в конце каждого цикла показывать текущую минимальную цену
                                  # по каждой отслеживаемой коллекции — чтобы наглядно видеть, что бот
                                  # реально получает живые данные с маркета (сверяй с ценами в самом Telegram).

# ----------------------------------------------------------------------


@dataclass
class Listing:
    slug: str
    num: int
    price_stars: int
    gift_id: int


def extract_price_stars(star_gift) -> Optional[int]:
    """StarGiftUnique.resell_amount — вектор StarsAmount (обычно один элемент в звёздах)."""
    amounts = getattr(star_gift, "resell_amount", None)
    if not amounts:
        return None
    for amt in amounts:
        # starsAmount: amount:long nanos:int  (nanos используется для дробных сумм; для целых звёзд nanos=0)
        if isinstance(amt, types.StarsAmount):
            return amt.amount
    return None


async def fetch_cheapest_listings(client: TelegramClient, gift_id: int, limit: int = PAGE_LIMIT):
    result = await client(
        functions.payments.GetResaleStarGiftsRequest(
            gift_id=gift_id,
            offset="",
            limit=limit,
            sort_by_price=True,
            attributes=ATTRIBUTES_FILTER,
        )
    )
    listings = []
    for g in result.gifts:
        price = extract_price_stars(g)
        if price is None:
            continue
        listings.append(Listing(slug=g.slug, num=g.num, price_stars=price, gift_id=g.gift_id))
    return listings


async def buy_listing(client: TelegramClient, listing: Listing) -> bool:
    """Пытается купить конкретный лот. Возвращает True при успехе."""
    invoice = types.InputInvoiceStarGiftResale(
        slug=listing.slug,
        to_id=types.InputPeerSelf(),
        ton=False,
    )
    try:
        form = await client(functions.payments.GetPaymentFormRequest(invoice=invoice))
        result = await client(
            functions.payments.SendStarsFormRequest(form_id=form.form_id, invoice=invoice)
        )
        print(f"[BUY OK] slug={listing.slug} num={listing.num} price={listing.price_stars}★ -> {result}")
        return True
    except FloodWaitError as e:
        print(f"[FLOOD_WAIT] нужно подождать {e.seconds}s")
        await asyncio.sleep(e.seconds)
        return False
    except RPCError as e:
        # Частые причины: лот уже купили, лот заблокирован (locked_until_date),
        # превышен лимит покупок на пользователя (limited_per_user) и т.п.
        print(f"[BUY FAILED] slug={listing.slug}: {e}")
        return False


async def list_gift_types(client: TelegramClient):
    result = await client(functions.payments.GetStarGiftsRequest(hash=0))
    print(f"Найдено типов подарков: {len(result.gifts)}\n")
    for g in result.gifts:
        gid = getattr(g, "id", None)
        title = getattr(g, "title", None)
        label = title if title else "(без отдельного названия)"
        print(f"gift_id={gid}\t{label}")


async def get_all_gift_ids(client: TelegramClient):
    """Возвращает список gift_id всех существующих типов подарков (для полного сканирования рынка).
    Берём только НЕ-уникальные базовые типы подарков (обычные, ещё не апгрейженные в NFT) —
    именно у них есть смысл смотреть resale-рынок через gift_id."""
    result = await client(functions.payments.GetStarGiftsRequest(hash=0))
    ids = []
    for g in result.gifts:
        gid = getattr(g, "id", None)
        if gid is not None:
            ids.append(gid)
    return ids


async def resolve_gift_ids_by_names(client: TelegramClient, names: list):
    """Ищет gift_id по названиям (частичное совпадение, без учёта регистра).
    Возвращает (список найденных gift_id, словарь gift_id->title) и печатает
    предупреждение по неопознанным именам."""
    result = await client(functions.payments.GetStarGiftsRequest(hash=0))
    catalog = []
    for g in result.gifts:
        gid = getattr(g, "id", None)
        title = getattr(g, "title", None)
        if gid is not None and title:
            catalog.append((gid, title))

    resolved = []
    titles = {}
    for wanted in names:
        wanted_lower = wanted.strip().lower()
        matches = [(gid, title) for gid, title in catalog if wanted_lower in title.lower()]
        if not matches:
            print(f"[ПРЕДУПРЕЖДЕНИЕ] Название '{wanted}' не найдено в каталоге подарков — пропускаю.")
            continue
        for gid, title in matches:
            print(f"  '{wanted}' -> gift_id={gid} ({title})")
            resolved.append(gid)
            titles[gid] = title

    return list(dict.fromkeys(resolved)), titles  # убираем дубликаты, сохраняя порядок


async def watch_loop(client: TelegramClient):
    buys_done = 0
    last_logged_price = {}  # slug -> цена, при которой мы последний раз реагировали на этот лот
    bought_slugs = set()    # slug, которые мы уже успешно купили в этой сессии (не трогаем повторно)

    gift_titles = {}

    if TARGET_GIFT_NAMES:
        print(f"Ищу gift_id по {len(TARGET_GIFT_NAMES)} указанным названиям...")
        gift_ids, gift_titles = await resolve_gift_ids_by_names(client, TARGET_GIFT_NAMES)
        if not gift_ids:
            raise SystemExit("Ни одно название не удалось сопоставить с каталогом подарков. Проверь написание.")
        print(f"Слежу за {len(gift_ids)} подтверждёнными типами подарков (по названиям).")
    elif TARGET_GIFT_IDS:
        gift_ids = list(TARGET_GIFT_IDS)
        print(f"Слежу за {len(gift_ids)} указанными типами подарков (по id).")
    else:
        gift_ids = await get_all_gift_ids(client)
        print(f"TARGET_GIFT_IDS/TARGET_GIFT_NAMES пусты -> сканирую ВСЕ {len(gift_ids)} типов подарков на маркете.")

    print(f"Порог цены={MAX_PRICE_STARS}★, интервал цикла={POLL_INTERVAL_SECONDS}s, "
          f"пауза между типами={PER_TYPE_DELAY_SECONDS}s, DRY_RUN={DRY_RUN}")

    while True:
        cycle_start = time.time()
        found_this_cycle = 0
        floors_this_cycle = {}

        for gift_id in gift_ids:
            try:
                listings = await fetch_cheapest_listings(client, gift_id)
            except FloodWaitError as e:
                print(f"[FLOOD_WAIT] {e.seconds}s — жду")
                await asyncio.sleep(e.seconds)
                continue
            except RPCError as e:
                print(f"[ERROR fetch gift_id={gift_id}] {e}")
                await asyncio.sleep(PER_TYPE_DELAY_SECONDS)
                continue

            if listings:
                floors_this_cycle[gift_id] = listings[0].price_stars  # уже отсортировано по цене

            for listing in listings:
                if listing.slug in bought_slugs:
                    continue  # уже купили этот конкретный NFT в этой сессии — не трогаем снова

                if listing.price_stars > MAX_PRICE_STARS:
                    continue  # цена сейчас не подходит — но НЕ запоминаем навсегда,
                              # вдруг он ещё подешевеет на следующем цикле

                # Цена подходит. Реагируем, только если это новый лот ИЛИ его цена
                # изменилась с прошлого раза, когда мы на неё реагировали — так мы не
                # спамим одним и тем же логом каждый цикл, но и не пропускаем реальное
                # снижение цены на уже виденном ранее slug (например, мисклик на уже
                # выставленном лоте меняет цену, а не создаёт новое объявление).
                if last_logged_price.get(listing.slug) == listing.price_stars:
                    continue

                last_logged_price[listing.slug] = listing.price_stars
                found_this_cycle += 1
                ts = time.strftime("%H:%M:%S")
                print(f"[{ts}] НАЙДЕН ЛОТ: gift_id={gift_id} slug={listing.slug} "
                      f"num={listing.num} цена={listing.price_stars}★ (порог {MAX_PRICE_STARS}★)")
                if DRY_RUN:
                    print("   -> DRY_RUN включён, покупка НЕ выполняется.")
                else:
                    ok = await buy_listing(client, listing)
                    if ok:
                        bought_slugs.add(listing.slug)
                        buys_done += 1
                        if MAX_BUYS_PER_RUN is not None and buys_done >= MAX_BUYS_PER_RUN:
                            print(f"Достигнут лимит MAX_BUYS_PER_RUN={MAX_BUYS_PER_RUN}, останавливаюсь.")
                            return

            await asyncio.sleep(PER_TYPE_DELAY_SECONDS)

        # Не даём словарю расти бесконечно (лоты, которые давно исчезли, можно "забыть")
        if len(last_logged_price) > 50000:
            last_logged_price.clear()

        if SHOW_FLOORS_EVERY_CYCLE and floors_this_cycle:
            parts = []
            for gid in gift_ids:
                if gid in floors_this_cycle:
                    name = gift_titles.get(gid, str(gid))
                    parts.append(f"{name}={floors_this_cycle[gid]}★")
            print("   Текущие полы: " + ", ".join(parts))

        elapsed = time.time() - cycle_start
        ts = time.strftime("%H:%M:%S")
        if found_this_cycle:
            print(f"[{ts}] цикл завершён ({elapsed:.1f}s, проверено {len(gift_ids)} типов), "
                  f"найдено новых лотов дешевле {MAX_PRICE_STARS}★: {found_this_cycle}.")
        else:
            print(f"[{ts}] цикл завершён ({elapsed:.1f}s, проверено {len(gift_ids)} типов), "
                  f"новых лотов дешевле {MAX_PRICE_STARS}★ не найдено.")
        sleep_left = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
        await asyncio.sleep(sleep_left)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list-gifts", action="store_true", help="Показать все типы подарков и их gift_id")
    parser.add_argument("--watch", action="store_true", help="Запустить слежение/снайпинг за TARGET_GIFT_ID")
    args = parser.parse_args()

    if not API_ID or not API_HASH:
        raise SystemExit("Заполни API_ID и API_HASH (получить на https://my.telegram.org)")

    if SESSION_STRING:
        # Запуск на сервере/Railway — без интерактивного ввода.
        session = StringSession(SESSION_STRING)
        client = TelegramClient(session, API_ID, API_HASH, flood_sleep_threshold=0)
        await client.connect()
        if not await client.is_user_authorized():
            raise SystemExit(
                "TG_SESSION_STRING задан, но авторизация не прошла (сессия истекла/невалидна). "
                "Сгенерируй новую строку сессии локально через generate_session.py."
            )
    else:
        # Локальный запуск — обычный интерактивный логин (спросит телефон/код/2FA).
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH, flood_sleep_threshold=0)
        await client.start()  # при первом запуске спросит телефон, код, пароль 2FA

    try:
        if args.list_gifts:
            await list_gift_types(client)
        elif args.watch:
            await watch_loop(client)
        else:
            parser.print_help()
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
