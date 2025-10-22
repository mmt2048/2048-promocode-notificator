import asyncio
import os
from aiogram import Bot
from aiogram.utils.markdown import hlink
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from convex import ConvexClient
from loguru import logger


BOT_TOKEN = os.getenv("BOT_TOKEN")
CONVEX_URL = os.getenv("CONVEX_URL")
WEB_APP_URL = os.getenv("WEB_APP_URL")
NOTIFICATION_DELAY_IN_SECONDS = int(os.getenv("NOTIFICATION_DELAY_IN_SECONDS"))


async def schedule_promocode_notification(
    convex_client: ConvexClient,
    bot: Bot,
    promocode_id: str,
) -> None:
    await asyncio.sleep(NOTIFICATION_DELAY_IN_SECONDS)

    promocode_with_user = convex_client.query(
        "promocodes:getPromocodeWithUserAndType",
        {"promocodeId": promocode_id},
    )
    promocode = promocode_with_user.get("promocode", {})
    promocode_type = promocode_with_user.get("promocodeType", {})
    user = promocode_with_user.get("user", {})
    min_order = int(promocode_type.get("minOrder"))
    discount = int(promocode_type.get("discount"))
    url = promocode_type.get("url")
    label = promocode_type.get("label")
    code = promocode.get("code")
    opened = promocode.get("opened")
    label_text = (
        f"({hlink(label, url)})" if label and url else (f"({label})" if label else "")
    )

    telegram_id = user.get("telegramId")

    if not code or telegram_id is None:
        logger.error(f"Invalid code or telegram_id: {code} {telegram_id}")
        return

    try:
        chat_id = int(telegram_id)
    except (TypeError, ValueError):
        logger.error(f"Invalid telegram_id: {telegram_id}")
        return

    if opened:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Открыть Магнит Маркет 🧲",
                        url="https://trk.mail.ru/c/kjtii2",
                    )
                ]
            ]
        )

        await bot.send_message(
            chat_id=chat_id,
            text=f"Не забудьте применить ваш промокод <code>{code}</code> на <b>{discount} ₽</b> от {min_order} ₽ {label_text}",
            reply_markup=keyboard,
        )
    else:
        web_app = WebAppInfo(url=WEB_APP_URL)

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Получить промокод ✨",
                        web_app=web_app,
                    )
                ]
            ]
        )

        await bot.send_message(
            chat_id=chat_id,
            text=f"Вам доступен новый промокод <b>-{discount} ₽</b> от {min_order} ₽ {label_text}🥳",
            reply_markup=keyboard,
        )

    logger.info(f"Sent notification for promocode {promocode_id} to user {chat_id}")


async def main():
    client = ConvexClient(CONVEX_URL)

    seen_promocode_ids: set[str] = set()

    async with Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML", link_preview_is_disabled=True),
    ) as bot:
        subscription = client.subscribe("promocodes:getLastPromocodeId")

        async for promocode_id in subscription:
            if promocode_id in seen_promocode_ids:
                continue

            seen_promocode_ids.add(promocode_id)

            asyncio.create_task(
                schedule_promocode_notification(
                    convex_client=client,
                    bot=bot,
                    promocode_id=promocode_id,
                )
            )
            logger.info(f"Scheduled notification for promocode {promocode_id}")


if __name__ == "__main__":
    logger.info("Starting promocode notificator")
    asyncio.run(main())
