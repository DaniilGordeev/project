#   Copyright (c) 2021. Tocenomiczs

import asyncio
import json
import random
import re
import string
from datetime import datetime, timezone, timedelta
from threading import Thread
from time import sleep

from telebot import TeleBot, types

import config
import keyboards as kbs
from banker import Banker
from database import Database
from mailing_core import start_mailings
from qiwi import Qiwi
from qiwi_withdraw import QiwiWithdraw

app = TeleBot(config.token, parse_mode="HTML")
qiwi = Qiwi(config.qiwi_secret_token)
qiwi_withdraw = QiwiWithdraw(config.qiwi_withdraw_token)
checking_banker = False

Thread(target=start_mailings, args=()).start()


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "back")
def back(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    db.status()
    if query.message.content_type == "photo":
        app.edit_message_caption("Действие отменено", query.message.chat.id, query.message.id,
                                 reply_markup=kbs.main_kb)
    elif query.message.content_type == "text":
        app.send_message(from_id, "Действие отменено", reply_markup=kbs.main_kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "delete")
def delete(query):
    app.answer_callback_query(query.id, show_alert=False)
    app.delete_message(query.message.chat.id, query.message.id)


@app.message_handler(regexp=r"^🖥 Профиль$")
def cabinet(message):
    from_id = message.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    deals_count = db.get_deals_count()
    deals_sum = db.get_deals_sum()
    text = f"🆔 Ваш id: {from_id}\n\n" \
           f"💰 Баланс: {user['balance']} RUB\n" \
           f"♻️ Количество сделок: {deals_count}\n" \
           f"💳 Сумма сделок: {deals_sum} RUB\n" \
           f"📊 Рейтинг: {user['rating']}"
    app.send_message(from_id, text, reply_markup=kbs.cabinet_inline)


@app.message_handler(regexp=r"^🤝 Сделки$")
def my_deals(message):
    from_id = message.from_user.id
    db = Database(from_id, app)
    deals = db.get_deals()
    kb = types.InlineKeyboardMarkup()
    for deal in deals:
        kb.add(types.InlineKeyboardButton(f"Сделка #{deal['id']}", callback_data=json.dumps({
            "action": "about_deal",
            "deal_id": deal['id']
        })))
    kb.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "back"})))
    app.send_message(from_id, "Выберите сделку", reply_markup=kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "admin_active_deals")
def admin_active_deals(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    if from_id not in config.admins:
        return
    db = Database(from_id, app)
    deals = db.get_active_deals()
    kb = types.InlineKeyboardMarkup()
    for deal in deals:
        kb.add(types.InlineKeyboardButton(f"Сделка #{deal['id']}", callback_data=json.dumps({
            "action": "admin_about_deal",
            "deal_id": deal['id']
        })))
    kb.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "admin_back"})))
    app.edit_message_text("Выберите сделку", query.message.chat.id, query.message.id, reply_markup=kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "admin_about_deal")
def admin_about_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    if from_id not in config.admins:
        return
    db = Database(from_id, app)
    deal_id = json.loads(query.data).get("deal_id")
    deal = db.get_deal(deal_id)
    buyer = Database(deal['buyer'], app).get_me()
    seller = Database(deal['seller'], app).get_me()
    if deal['status'] == "waiting_seller":
        deal_status = "Ожидание подтверждения продавца"
    elif deal['status'] == "waiting_for_pay":
        deal_status = "Ожидание оплаты покупателем"
    elif deal['status'] == "waiting_goods_transfer":
        deal_status = "Ожидание передачи товара продавцом"
    elif deal['status'] == "arbitrage":
        deal_status = "Арбитраж"
    elif deal['status'] == "closed_arbitrage":
        deal_status = "Закрыто арбитражем"
    elif deal['status'] == "closed":
        deal_status = "Завершена"
    elif deal['status'] == "canceled":
        deal_status = "Отменена"
    else:
        deal_status = "Неизвестно"
    deal_message = f"🔰 Сделка: #{deal_id}\n\n" \
                   f"➖ Покупатель: @{buyer['username']}\n\n" \
                   f"➖ Продавец: @{seller['username']}\n\n" \
                   f"💰 Сумма: {deal['sum']} RUB\n\n" \
                   f"📝 Условия: {deal['info']}\n\n" \
                   f"♻️ Статус: {deal_status}"
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Сообщения сделки",
                                      callback_data=json.dumps({"action": "deal_messages", "deal_id": deal_id})))
    if deal['status'] == "waiting_goods_transfer":
        kb.add(types.InlineKeyboardButton("Вернуть деньги покупателю", callback_data=json.dumps(
            {"action": "admin_cancel_deal", "deal_id": deal_id})))
    kb.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "admin_back"})))
    app.edit_message_text(deal_message, query.message.chat.id, query.message.id, reply_markup=kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "admin_cancel_deal")
def admin_cancel_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    if from_id not in config.admins:
        return
    db = Database(from_id, app)
    deal_id = json.loads(query.data).get("deal_id")
    deal = db.get_deal(deal_id)
    if deal['status'] != "waiting_goods_transfer":
        app.answer_callback_query(query.id, text="Невозможно отменить сделку")
        return
    buyer_db = Database(deal['buyer'], app)
    buyer_db.change_balance(deal['sum'])
    db.set_deal_status(deal_id, "canceled")
    message = f"Сделка #{deal_id} отменена администратором, деньги возвращены покупателю"
    app.edit_message_text(f"Сделка #{deal_id} отменена", query.message.chat.id, query.message.id)
    app.send_message(deal['buyer'], message)
    app.send_message(deal['seller'], message)
    buyer = db.find_user(deal['buyer'])
    seller = db.find_user(deal['seller'])
    app.send_message(config.notifications_channel, f"🔰 Сделка: #{deal_id}\n\n"
                                                   f"➖ Покупатель: @{buyer['username']}\n\n"
                                                   f"➖ Продавец: @{seller['username']}\n\n"
                                                   f"💰 Сумма: {deal['sum']} RUB\n\n"
                                                   "♻️ Статус: Отменено")


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "deal_messages")
def deal_messages(query):
    from_id = query.from_user.id
    if from_id not in config.admins:
        return
    db = Database(from_id, app)
    deal_id = json.loads(query.data).get("deal_id")
    messages = db.get_deal_messages(deal_id)
    if len(messages) == 0:
        app.answer_callback_query(query.id, "Нету сообщений в сделке")
        return
    users = {}
    message = ""
    for msg in messages:
        if users.get(msg['user_id']) is None:
            user = db.find_user(user_id=msg['user_id'])
            users[msg['user_id']] = user['username']
        message += f"{users[msg['user_id']]}: {msg['message']}\n"
    app.answer_callback_query(query.id)
    app.edit_message_text(message, query.message.chat.id, query.message.id, reply_markup=kbs.admin_back, parse_mode="")


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "mailing")
def create_mailing(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    db.temp()
    db.mailing_photo()
    db.status("mailing_text")
    app.edit_message_text("Введите текст рассылки или отправьте фото", query.message.chat.id, query.message.id,
                          reply_markup=kbs.admin_back)


@app.callback_query_handler(func=lambda query: json.loads(query.data)['action'] == "test_mailing")
def test_mailing(query):
    app.answer_callback_query(query.id, show_alert=False)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    mailing_id = json.loads(query.data)['id']
    mailing = db.get_mailing(mailing_id)
    if mailing is None:
        return
    if mailing['photo_id'] is None:
        app.send_message(from_id, mailing['mailing_text'], reply_markup=kbs.inline_delete)
    else:
        attachment = mailing['photo_id'].split("|")
        if attachment[0] == "PHOTO":
            app.send_photo(from_id, attachment[1], mailing['mailing_text'],
                           reply_markup=kbs.inline_delete)
        elif attachment[0] == "VIDEO":
            app.send_video(from_id, attachment[1], caption=mailing['mailing_text'],
                           reply_markup=kbs.inline_delete)
        elif attachment[0] == "DOCUMENT":
            app.send_document(from_id, attachment[1], caption=mailing['mailing_text'],
                              reply_markup=kbs.inline_delete)
        elif attachment[0] == "ANIMATION":
            app.send_animation(from_id, attachment[1], caption=mailing['mailing_text'],
                               reply_markup=kbs.inline_delete)


@app.callback_query_handler(func=lambda query: json.loads(query.data)['action'] == "confirm_mailing")
def confirm_mailing(query):
    app.answer_callback_query(query.id, show_alert=False)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    mailing_id = json.loads(query.data)['id']
    mailing = db.get_mailing(mailing_id)
    if mailing is None:
        return
    db.confirm_mailing(mailing_id)
    if query.message.content_type == "text":
        app.edit_message_text(f"Рассылка успешно добавлена, её id: {mailing_id}", query.message.chat.id,
                              query.message.id)
    else:
        app.edit_message_caption(f"Рассылка успешно добавлена, её id: {mailing_id}", query.message.chat.id,
                                 query.message.id)


@app.callback_query_handler(func=lambda query: json.loads(query.data)['action'] == "cancel_mailing")
def cancel_mailing(query):
    app.answer_callback_query(query.id, show_alert=False)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    mailing_id = json.loads(query.data)['id']
    mailing = db.get_mailing(mailing_id)
    if mailing is None:
        return
    if mailing['confirmed']:
        if query.message.content_type == "text":
            app.edit_message_text("Рассылка уже подтверждена", query.message.chat.id, query.message.id)
        else:
            app.edit_message_caption("Рассылка уже подтверждена", query.message.chat.id, query.message.id)
    db.delete_mailing(mailing_id)
    if query.message.content_type == "text":
        app.edit_message_text("Рассылка успешно удалена", query.message.chat.id, query.message.id)
    else:
        app.edit_message_caption("Рассылка успешно удалена", query.message.chat.id, query.message.id)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "about_deal")
def about_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    deal_id = json.loads(query.data).get("deal_id")
    deal = db.get_deal(deal_id)
    if deal['buyer'] != from_id and deal['seller'] != from_id:
        return
    buyer = Database(deal['buyer'], app).get_me()
    seller = Database(deal['seller'], app).get_me()
    if deal['status'] == "waiting_seller":
        deal_status = "Ожидание подтверждения продавца"
    elif deal['status'] == "waiting_for_pay":
        deal_status = "Ожидание оплаты покупателем"
    elif deal['status'] == "waiting_goods_transfer":
        deal_status = "Ожидание передачи товара продавцом"
    elif deal['status'] == "arbitrage":
        deal_status = "Арбитраж"
    elif deal['status'] == "closed_arbitrage":
        deal_status = "Закрыто арбитражем"
    elif deal['status'] == "closed":
        deal_status = "Завершена"
    elif deal['status'] == "canceled":
        deal_status = "Отменена"
    else:
        deal_status = "Неизвестно"
    deal_message = f"🔰 Сделка: #{deal_id}\n\n" \
                   f"➖ Покупатель: @{buyer['username']}\n\n" \
                   f"➖ Продавец: @{seller['username']}\n\n" \
                   f"💰 Сумма: {deal['sum']} RUB\n\n" \
                   f"📝 Условия: {deal['info']}\n\n" \
                   f"♻️ Статус: {deal_status}"
    kb = types.InlineKeyboardMarkup()
    if deal['status'] == "waiting_for_pay" and from_id == deal['buyer']:
        kb.add(types.InlineKeyboardButton("Начать сделку", callback_data=json.dumps({
            "action": "pay_deal",
            "deal_id": deal_id
        })))
    if deal['status'] == "waiting_goods_transfer" and from_id == deal['buyer']:
        kb.add(types.InlineKeyboardButton("Подтвердить передачу товара", callback_data=json.dumps({
            "action": "close_deal",
            "deal_id": deal_id
        })))
    if deal['status'] == "waiting_goods_transfer":
        kb.add(types.InlineKeyboardButton("Перевести сделку в арбитраж", callback_data=json.dumps({
            "action": "deal_arbitrage",
            "deal_id": deal_id
        })))
    kb.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "back"})))
    app.edit_message_text(deal_message, query.message.chat.id, query.message.id, reply_markup=kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "deposit")
def deposit(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    db.status("deposit_sum")
    app.edit_message_text("Введите сумму пополнения", query.message.chat.id, query.message.id,
                          reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "withdraw")
def withdraw(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    db.status("withdraw_sum")
    fee = json.loads(open("settings.json", "r").read())['withdraw_fee']
    app.edit_message_text("Введите сумму вывода\n"
                          f"На вывод действует комиссия {fee}%", query.message.chat.id, query.message.id,
                          reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "admin_contact")
def admin_contact(query):
    app.answer_callback_query(query.id)
    app.edit_message_text(f"Администратор для связи: {config.admin_contact}", query.message.chat.id,
                          query.message.id, reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "admin_back")
def admin_back(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    db.status()
    app.edit_message_text("Админ меню", query.message.chat.id, query.message.id, reply_markup=kbs.admin_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "add_ad_button")
def add_ad_button(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    db.status("ad_button_text")
    app.edit_message_text("Введите название рекламной кнопки", query.message.chat.id, query.message.id,
                          reply_markup=kbs.admin_back)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "remove_ad_button")
def remove_ad_button(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    data = json.loads(query.data)
    if from_id not in config.admins:
        return
    if data.get("id") is None:
        ads = db.get_ads()
        kb = types.InlineKeyboardMarkup()
        for ad in ads:
            kb.add(types.InlineKeyboardButton(ad['button_name'], callback_data=json.dumps({
                "action": "remove_ad_button",
                "id": ad['id']
            })))
        kb.add(types.InlineKeyboardButton("Отменить", callback_data=json.dumps({"action": "admin_back"})))
        app.edit_message_text("Выберите кнопку", query.message.chat.id, query.message.id,
                              reply_markup=kb)
    else:
        db.remove_ad_button(data['id'])
        app.edit_message_text("Вы успешно удалили рекламную кнопку", query.message.chat.id, query.message.id,
                              reply_markup=kbs.admin_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "edit_ad_button")
def edit_ad_button(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    data = json.loads(query.data)
    if from_id not in config.admins:
        return
    if data.get("id") is None:
        ads = db.get_ads()
        kb = types.InlineKeyboardMarkup()
        for ad in ads:
            kb.add(types.InlineKeyboardButton(ad['button_text'], callback_data=json.dumps({
                "action": "edit_ad_button",
                "id": ad['id']
            })))
        kb.add(types.InlineKeyboardButton("Отменить", callback_data=json.dumps({"action": "admin_back"})))
        app.edit_message_text("Выберите кнопку", query.message.chat.id, query.message.id,
                              reply_markup=kb.to_json())
    else:
        db.temp(data['id'])
        db.status("edit_button")
        app.edit_message_text("Введите новый текст рекламной кнопки", query.message.chat.id, query.message.id,
                              reply_markup=kbs.admin_back)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "withdraw_fee")
def withdraw_fee(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    db.status("withdraw_fee")
    app.edit_message_text("Введите новую комиссию", query.message.chat.id, query.message.id,
                          reply_markup=kbs.admin_back)


@app.message_handler(regexp=r"^🌐 О боте$")
def information(message):
    app.send_message(message.from_user.id, config.information, reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "open_deal")
def open_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    seller_id = json.loads(query.data)['user_id']
    seller = db.find_user(user_id=seller_id)
    if seller is None or seller_id == from_id:
        return
    db.status(f"deal_sum__{seller_id}")
    app.edit_message_text("Введите сумму сделки в RUB", query.message.chat.id, query.message.id,
                          reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "deposit_type")
def deposit_type(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    payment_type = json.loads(query.data)['type']
    if payment_type not in ['qiwi', 'btc']:
        return
    payment_id = "-".join(
        ["".join([random.choice(string.ascii_letters + string.digits) for _ in range(4)]) for _ in range(3)])
    try:
        payment_sum = int(user['temp_field'])
    except ValueError:
        return
    if payment_type == "qiwi":
        db.add_payment(payment_id, payment_sum, "qiwi")
        bill_url = qiwi.generate_bill(payment_sum, payment_id)
        if not bill_url:
            app.edit_message_text("Произошло ошибка, повторите снова", query.message.chat.id, query.message.id)
            return
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Оплатить", url=bill_url))
        kb.add(types.InlineKeyboardButton("Проверить оплату", callback_data=json.dumps({
            "action": "check_qiwi",
            "payment_id": payment_id
        })))
        kb.add(types.InlineKeyboardButton("Отменить", callback_data=json.dumps({
            "action": "cancel_payment",
            "payment_id": payment_id
        })))
        app.edit_message_text("Счёт сгенерирован", query.message.chat.id, query.message.id, reply_markup=kb)
    elif payment_type == "btc":
        db.add_payment(payment_id, payment_sum, "banker")
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Отменить", callback_data=json.dumps({
            "action": "cancel_payment",
            "payment_id": payment_id
        })))
        db.status(f"waiting_btc__{payment_id}")
        app.edit_message_text("Отправьте чек BTC Banker в этот чат\n"
                              "Курс должен быть Binance\n\n"
                              "Наш сервис не гарантирует сохранность денег при пополнении баланса этим способом в "
                              "связи с частой блокировкой аккаунтов банкира", query.message.chat.id, query.message.id,
                              reply_markup=kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "withdraw_type")
def withdraw_type(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    payment_type = json.loads(query.data)['type']
    if payment_type not in ['qiwi', 'btc']:
        return
    payment_sum = int(user['temp_field'])
    if payment_sum > user['balance']:
        return
    if payment_type == "btc":
        db.change_balance(-payment_sum)
        app.edit_message_text(
            f"Заявка на вывод {payment_sum} RUB успешно сформирована, ожидайте чек в лс от администратора",
            query.message.chat.id, query.message.id)
        fee = json.loads(open("settings.json", "r").read())['withdraw_fee']
        sum_to_send = int(payment_sum - (payment_sum / 100 * fee))
        admin_kb = types.InlineKeyboardMarkup()
        admin_kb.add(types.InlineKeyboardButton("Выплатил", callback_data=json.dumps({"action": "delete"})))
        for admin in config.admins:
            app.send_message(admin, "❗️❗️❗️Новый вывод❗️❗️❗️\n"
                                    f"Пользователь: @{user['username']}\n"
                                    f"Сумма заявки: {payment_sum} RUB\n"
                                    f"Сумма для выплаты: {sum_to_send} RUB\n"
                                    "Тип вывода: БТК чек", reply_markup=admin_kb)
        app.send_message(config.notifications_channel, "Новый вывод\n"
                                                       f"Пользователь: @{user['username']}\n"
                                                       f"Сумма вывода: {payment_sum} RUB\n"
                                                       "Тип вывода: БТК чек")
    elif payment_type == "qiwi":
        db.status("qiwi_number")
        app.edit_message_text("Введите номер для вывода", query.message.chat.id, query.message.id,
                              reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "check_qiwi")
def check_qiwi(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    payment_id = json.loads(query.data).get("payment_id")
    if payment_id is None:
        return
    payment = db.get_payment(payment_id)
    if payment['status'] != 0:
        return
    if qiwi.is_bill_payed(payment_id):
        db.change_balance(payment['sum'])
        db.set_payment_status(payment_id, 1)
        for admin in config.admins:
            app.send_message(admin, "Новое пополнение\n"
                                    f"Пользователь: @{user['username']}\n"
                                    f"Сумма: {payment['sum']} RUB\n"
                                    f"Через Киви")
        app.send_message(from_id, "Счёт успешно оплачен\n"
                                  f"На баланс зачислено {payment['sum']} RUB")
        app.edit_message_text("Счёт успешно оплачен", query.message.chat.id, query.message.id)
        return
    else:
        app.send_message(from_id, "Счёт всё ещё не оплачен")
        return


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "admin_stats")
def admin_stats(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    all_users = len(db.get_all_users())
    day_users = db.get_users_count("day")
    week_users = db.get_users_count("week")
    month_users = db.get_users_count("month")
    deals_active = db.get_deals_stats(status="active")
    deals_day = db.get_deals_stats(period="day")
    deals_week = db.get_deals_stats(period="week")
    deals_month = db.get_deals_stats(period="month")
    deals_all = db.get_deals_stats()
    user_balances = db.get_users_balances()
    active_deals_sum = db.active_deals_sum()
    stats = f"Количество пользователей: {all_users}\n" \
            f"Балансы пользователей: {user_balances} RUB\n" \
            f"Сумма активных сделок: {active_deals_sum} RUB\n\n" \
            f"Пользователей за сутки: {day_users}\n" \
            f"Пользователей за неделю: {week_users}\n" \
            f"Пользователей за месяц: {month_users}\n\n" \
            f"Всего сделок: {deals_all}\n" \
            f"Активных сделок: {deals_active}\n" \
            f"Сделок за сутки: {deals_day}\n" \
            f"Сделок за неделю: {deals_week}\n" \
            f"Сделок за месяц: {deals_month}"
    app.edit_message_text(stats, query.message.chat.id, query.message.id, reply_markup=kbs.admin_back)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "database_backup")
def database_backup(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    if from_id not in config.admins:
        return
    db = open("garant.sqlite", "rb")
    app.send_document(from_id, db)


@app.message_handler(regexp=r"^🔍 Поиск пользователя$")
def find_user(message):
    from_id = message.from_user.id
    db = Database(from_id, app)
    db.status("find_user")
    app.send_message(from_id, "Введите никнейм продавца", reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "admin_find_user")
def admin_find_user(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    db.status("admin_find_user")
    app.edit_message_text("Введите никнейм пользователя", query.message.chat.id, query.message.id,
                          reply_markup=kbs.admin_back)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "confirm_deal")
def confirm_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    deal_id = json.loads(query.data)['deal_id']
    deal = db.get_deal(deal_id)
    if deal is None:
        return
    if deal['status'] != "waiting_seller":
        return
    if from_id != deal['seller']:
        return
    buyer = db.find_user(user_id=deal['buyer'])
    db.set_deal_status(deal_id, "waiting_for_pay")
    deal_message = f"🔰 Сделка: #{deal_id}\n\n" \
                   f"➖ Покупатель: @{buyer['username']}\n\n" \
                   f"➖ Продавец: @{user['username']}\n\n" \
                   f"💰 Сумма: {deal['sum']} RUB\n\n" \
                   f"📝 Условия: {deal['info']}\n\n" \
                   "♻️ Статус: Ожидайте уведомления об оплате"
    communicate_button = types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
        {"action": "communicate", "deal_id": deal_id}))
    kb = types.InlineKeyboardMarkup()
    kb.add(communicate_button)
    app.edit_message_text(deal_message +
                          "\nНе передавайте товар до резервирования покупателем денег в системе, иначе арбитраж не "
                          "сможет вам помочь в случае обмана\n\n"
                          "<b>⚠️ Для обеспечения безопасности средств необходимо общаться только внутри этого бота, "
                          "товар передавать только через бота, в противном случае СДЕЛКА БУДЕТ ОТМЕНЕНА И СРЕДСТВА "
                          "КОНФИСКОВАНЫ ДО РЕШЕНИЯ СПОРА</b>\n\n", query.message.chat.id, query.message.id,
                          reply_markup=kb)
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Начать сделку", callback_data=json.dumps({
        "action": "pay_deal",
        "deal_id": deal_id
    })))
    kb.add(types.InlineKeyboardButton("Отмена", callback_data=json.dumps({
        "action": "decline_deal",
        "deal_id": deal_id
    })))
    kb.add(communicate_button)
    app.send_message(buyer['tg'], deal_message +
                     "\nПосле начала сделки с вашего баланса спишется и зарезервируется в системе сумма сделки до "
                     "передачи товара и подтверждения вами этого\n\n"
                     "<b>⚠️ Для обеспечения безопасности средств необходимо общаться только внутри этого бота, "
                     "товар передавать только через бота, в противном случае СДЕЛКА БУДЕТ ОТМЕНЕНА И СРЕДСТВА "
                     "КОНФИСКОВАНЫ ДО РЕШЕНИЯ СПОРА</b>\n\n",
                     reply_markup=kb)

@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "communicate")
def communicate(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    deal_id = json.loads(query.data)['deal_id']
    deal = db.get_deal(deal_id)
    if deal is None:
        return
    if deal['status'] != "waiting_for_pay" and deal['status'] != "waiting_goods_transfer" or \
            deal['status'] == "arbitrage":
        return
    if from_id != deal['seller'] and from_id != deal['buyer']:
        return
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Выйти из режима переписки",
                                      callback_data=json.dumps({"action": "exit_communicate"})))
    db.status(f"communicate__{deal_id}")
    app.send_message(from_id, "Вы вошли в режим переписки, теперь все сообщения, которые вы будете отправлять "
                              "будут пересылаться второму участнику сделки")


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "exit_communicate")
def exit_communicate(query):
    from_id = query.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    if user['status'] is not None and user['status'].startswith("communicate"):
        db.status()
        app.send_message(from_id, "Вы успешно вышли из режима переписки и вернулись в обычный режим бота")
        app.answer_callback_query(query.id)
    else:
        app.answer_callback_query(query.id, text="Вы не в режиме переписки")


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "pay_deal")
def pay_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    deal_id = json.loads(query.data)['deal_id']
    deal = db.get_deal(deal_id)
    if deal is None:
        return
    if deal['status'] != "waiting_for_pay":
        return
    if from_id != deal['buyer']:
        return
    if user['balance'] < deal['sum']:
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Начать сделку", callback_data=json.dumps({
            "action": "pay_deal",
            "deal_id": deal_id
        })))
        kb.add(types.InlineKeyboardButton("Отмена", callback_data=json.dumps({
            "action": "decline_deal",
            "deal_id": deal_id
        })))
        app.edit_message_text("На вашем балансе недостаточно средств", query.message.chat.id, query.message.id,
                              reply_markup=kb)
        return
    seller = db.find_user(user_id=deal['seller'])
    db.change_balance(-deal['sum'])
    db.set_deal_status(deal_id, "waiting_goods_transfer")
    app.send_message(config.notifications_channel, f"🔰 Сделка: #{deal_id}\n\n"
                                                   f"➖ Покупатель: @{user['username']}\n\n"
                                                   f"➖ Продавец: @{seller['username']}\n\n"
                                                   f"💰 Сумма: {deal['sum']} RUB\n\n"
                                                   "♻️ Статус: Сделка создана")
    deal_message = f"🔰 Сделка: #{deal_id}\n\n" \
                   f"➖ Покупатель: @{user['username']}\n\n" \
                   f"➖ Продавец: @{seller['username']}\n\n" \
                   f"💰 Сумма: {deal['sum']} RUB\n\n" \
                   f"📝 Условия: {deal['info']}\n\n" \
                   "♻️ Статус: Передача товара"
    kb_seller = types.InlineKeyboardMarkup()
    kb_seller.add(types.InlineKeyboardButton("Перевести сделку в арбитраж", callback_data=json.dumps({
        "action": "deal_arbitrage",
        "deal_id": deal_id
    })))
    kb_seller.add(types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
        {"action": "communicate", "deal_id": deal_id})))
    kb_buyer = types.InlineKeyboardMarkup()
    kb_buyer.add(types.InlineKeyboardButton("Подтвердить передачу товара", callback_data=json.dumps({
        "action": "close_deal",
        "deal_id": deal_id
    })))
    kb_buyer.add(types.InlineKeyboardButton("Перевести сделку в арбитраж", callback_data=json.dumps({
        "action": "deal_arbitrage",
        "deal_id": deal_id
    })))
    kb_buyer.add(types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
        {"action": "communicate", "deal_id": deal_id})))
    app.send_message(from_id, deal_message, reply_markup=kb_buyer, disable_web_page_preview=True)
    app.send_message(seller['tg'], deal_message, reply_markup=kb_seller, disable_web_page_preview=True)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "deal_arbitrage")
def deal_arbitrage(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    deal_id = json.loads(query.data)['deal_id']
    deal = db.get_deal(deal_id)
    seller = db.find_user(user_id=deal['seller'])
    buyer = db.find_user(user_id=deal['buyer'])
    if deal is None:
        return
    if deal['status'] != "waiting_goods_transfer":
        return
    if from_id != deal['buyer'] and from_id != deal['seller']:
        return
    db.set_deal_status(deal_id, "arbitrage")
    arbitrator = db.find_user(user_id=config.arbitrage)
    deal_message = f"Сделка #{deal_id} переведена в статус арбитража по решению одной из сторон, дальнейшие " \
                   f"манипуляции со сделкой может производить только арбитр\n\n" \
                   f"Контакт арбитра: @{arbitrator['username']}\n\n" \
                   f"Ожидайте пока арбитр напишет вам, или напишите первым, объясняя суть конфлика\n\n" \
                   f"По решению арбитра сделка будет закрыта в пользу продавца или покупателя\n" \
                   f"Данная сделка не будет учитываться в статистике сделок при поиске пользователя\n" \
                   f"У проигравшей стороны рейтинг отнимется на одну единицу, выигравшей стороне рейтинг прибавится " \
                   f"на один "
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
        {"action": "communicate", "deal_id": deal_id})))
    app.send_message(seller['tg'], deal_message, reply_markup=kb)
    app.send_message(buyer['tg'], deal_message, reply_markup=kb)
    app.send_message(config.notifications_channel, f"🔰 Сделка: #{deal_id}\n\n"
                                                   f"➖ Покупатель: @{buyer['username']}\n\n"
                                                   f"➖ Продавец: @{seller['username']}\n\n"
                                                   f"💰 Сумма: {deal['sum']} RUB\n\n"
                                                   "♻️ Статус: Арбитраж")
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Закрыть в пользу покупателя", callback_data=json.dumps({
        "action": "close_arbitrator",
        "deal_id": deal_id,
        "to": "buyer"
    })))
    kb.add(types.InlineKeyboardButton("Закрыть в пользу продавца", callback_data=json.dumps({
        "action": "close_arbitrator",
        "deal_id": deal_id,
        "to": "seller"
    })))
    app.send_message(arbitrator['tg'], f"🔰 Сделка: #{deal_id}\n\n"
                                       f"➖ Покупатель: @{buyer['username']}\n\n"
                                       f"➖ Продавец: @{seller['username']}\n\n"
                                       f"💰 Сумма: {deal['sum']} RUB\n\n"
                                       f"📝 Условия: {deal['info']}\n\n"
                                       "♻️ Статус: Открыт арбитраж", reply_markup=kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "close_arbitrator")
def close_arbitrator(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    seller_db = Database(from_id, app)
    deal_id = json.loads(query.data)['deal_id']
    deal = seller_db.get_deal(deal_id)
    seller = seller_db.find_user(user_id=deal['seller'])
    buyer = seller_db.find_user(user_id=deal['buyer'])
    buyer_db = Database(buyer['tg'], app)
    seller_db = Database(seller['tg'], app)
    if deal is None:
        return
    if deal['status'] != "arbitrage":
        return
    if from_id != config.arbitrage:
        return
    favor = json.loads(query.data)['to']
    if favor == "seller":
        favor = "продавца"
        seller_db.change_balance(deal['sum'])
        db.add_rating(seller['tg'], 1)
        db.add_rating(buyer['tg'], -1)
    else:
        favor = "покупателя"
        db.add_rating(seller['tg'], -1)
        db.add_rating(buyer['tg'], 1)
        buyer_db.change_balance(deal['sum'])
    db.set_deal_status(deal_id, "closed_arbitrage")
    deal_message = f"Сделка #{deal_id} закрыта арбитром в пользу {favor}"
    # app.send_message(config.notifications_channel, f"🔰 Сделка: #{deal_id}\n\n"
    #                                                f"➖ Покупатель: @{buyer['username']}\n\n"
    #                                                f"➖ Продавец: @{seller['username']}\n\n"
    #                                                f"💰 Сумма: {deal['sum']} RUB\n\n"
    #                                                f"♻️ Статус: Закрыто в пользу {favor}")
    app.send_message(seller['tg'], deal_message)
    app.send_message(buyer['tg'], deal_message)
    app.send_message(from_id, f"Сделка #{deal_id} успешно закрыта в пользу {favor}")


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "add_promocode")
def add_promocode(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    db.status("promocode_sum")
    app.edit_message_text("Введите сумму промокода", query.message.chat.id, query.message.id,
                          reply_markup=kbs.admin_back)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "close_deal")
def close_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    deal_id = json.loads(query.data)['deal_id']
    deal = db.get_deal(deal_id)
    if deal is None:
        return
    if deal['status'] != "waiting_goods_transfer":
        return
    if from_id != deal['buyer']:
        return
    seller_db = Database(deal['seller'], app)
    seller = seller_db.get_me()
    db.set_deal_status(deal_id, 'closed')
    db.add_rating(from_id, 1)
    db.add_rating(seller['tg'], 1)
    seller_db.change_balance(deal['sum'])
    deal_message = f"🔰 Сделка: #{deal_id}\n\n" \
                   f"➖ Покупатель: @{user['username']}\n\n" \
                   f"➖ Продавец: @{seller['username']}\n\n" \
                   f"💰 Сумма: {deal['sum']} RUB\n\n" \
                   f"📝 Условия: {deal['info']}\n\n" \
                   "♻️ Статус: Сделка закрыта успешно"
    # app.send_message(config.notifications_channel, f"🔰 Сделка: #{deal_id}\n\n"
    #                                                f"➖ Покупатель: @{user['username']}\n\n"
    #                                                f"➖ Продавец: @{seller['username']}\n\n"
    #                                                f"💰 Сумма: {deal['sum']} RUB\n\n"
    #                                                "♻️ Статус: Завершено успешно")
    app.send_message(from_id, deal_message, reply_markup=kbs.back_inline)
    app.send_message(seller['tg'], deal_message, reply_markup=kbs.back_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "decline_deal")
def decline_deal(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    deal_id = json.loads(query.data)['deal_id']
    deal = db.get_deal(deal_id)
    if deal is None:
        return
    if deal['status'] != "waiting_seller" and deal['status'] != "waiting_for_pay":
        return
    if from_id != deal['buyer'] and from_id != deal['seller']:
        return
    db.set_deal_status(deal_id, "canceled")
    app.send_message(deal['seller'], f"🔰 Сделка: #{deal_id} Отменена")
    app.send_message(deal['buyer'], f"🔰 Сделка: #{deal_id} Отменена")


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "cancel_payment")
def cancel_payment(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    payment_id = json.loads(query.data).get("payment_id")
    payment = db.get_payment(payment_id)
    if payment_id is None:
        return
    if payment['status'] != 0:
        return
    if payment['sum'] == "qiwi":
        qiwi.reject_bill(payment_id)
    db.set_payment_status(payment_id, 2)
    app.edit_message_text("Оплата отменена", query.message.chat.id, query.message.id)


@app.message_handler(commands=['admin'])
def admin_inline(message):
    from_id = message.from_user.id
    if from_id not in config.admins:
        return
    app.send_message(from_id, "Админ-меню", reply_markup=kbs.admin_inline)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "arbitrages")
def arbitrages(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    deals = db.get_arbitrage_deals()
    kb = types.InlineKeyboardMarkup()
    for deal in deals:
        kb.add(types.InlineKeyboardButton(f"Сделка #{deal['id']}", callback_data=json.dumps(
            {"action": "arbitrage_info", "deal_id": deal['id']})))
    kb.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "admin_back"})))
    app.edit_message_text("Список арбитражей", query.message.chat.id, query.message.id, reply_markup=kb)


@app.callback_query_handler(lambda query: json.loads(query.data)['action'] == "arbitrage_info")
def arbitrage_info(query):
    app.answer_callback_query(query.id)
    from_id = query.from_user.id
    db = Database(from_id, app)
    if from_id not in config.admins:
        return
    deal_id = json.loads(query.data)['deal_id']
    deal = db.get_deal(deal_id)
    buyer = db.find_user(user_id=deal['buyer'])
    seller = db.find_user(user_id=deal['seller'])
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Закрыть в пользу покупателя", callback_data=json.dumps({
        "action": "close_arbitrator",
        "deal_id": deal_id,
        "to": "buyer"
    })))
    kb.add(types.InlineKeyboardButton("Закрыть в пользу продавца", callback_data=json.dumps({
        "action": "close_arbitrator",
        "deal_id": deal_id,
        "to": "seller"
    })))
    kb.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({
        "action": "admin_back"
    })))
    app.edit_message_text(f"🔰 Сделка: #{deal['id']}\n\n"
                          f"➖ Покупатель: @{buyer['username']}\n\n"
                          f"➖ Продавец: @{seller['username']}\n\n"
                          f"💰 Сумма: {deal['sum']} RUB\n\n"
                          f"📝 Условия: {deal['info']}\n\n"
                          "♻️ Статус: Открыт арбитраж", query.message.chat.id, query.message.id, reply_markup=kb)


@app.channel_post_handler(commands=['peer'])
def peer_channel(message):
    print(message.chat.id)


@app.message_handler(commands=['peer'])
def peer_channel(message):
    print(message.chat.id)


@app.message_handler(content_types=['text'])
def text_handler(message):
    global checking_banker
    if message.chat.id < 0:
        return
    from_id = message.from_user.id
    text = message.text
    db = Database(from_id, app)
    user = db.get_me()
    if text == '/start':
        app.send_message(from_id, f"👑 Добро пожаловать, {user['username']}!", reply_markup=kbs.main_kb)
        return
    if db.can_activate_promo(text):
        promo_sum = db.activate_promo(text)
        if not promo_sum:
            return
        db.change_balance(promo_sum)
        app.send_message(from_id, f"На ваш баланс начислено {promo_sum} RUB")
    if user['status'] is not None:
        status = user['status']
        if status == "deposit_sum":
            try:
                deposit_sum = int(text)
            except ValueError:
                app.send_message(from_id, "Введите число", reply_markup=kbs.back_inline)
                return
            if deposit_sum <= 0:
                app.send_message(from_id, "Введите положительное число", reply_markup=kbs.back_inline)
                return
            db.temp(deposit_sum)
            db.status()
            app.send_message(from_id, "Выберите способ оплаты", reply_markup=kbs.deposit_type)
        elif status == "withdraw_sum":
            try:
                withdraw_sum = int(text)
            except ValueError:
                app.send_message(from_id, "Введите число", reply_markup=kbs.back_inline)
                return
            if withdraw_sum < 5:
                app.send_message(from_id, "Минимальная сумма для вывода - 50 RUB", reply_markup=kbs.back_inline)
                return
            if withdraw_sum > user['balance']:
                app.send_message(from_id, "На вашем балансе недостаточно средств\n"
                                          f"Ваш баланс: {user['balance']} RUB", reply_markup=kbs.back_inline)
                return
            fee = json.loads(open("settings.json", "r").read())['withdraw_fee']
            sum_to_send = int(withdraw_sum - (withdraw_sum / 100 * fee))
            db.temp(withdraw_sum)
            db.status()
            app.send_message(from_id, f"К выплате: {sum_to_send} RUB\n"
                                      "Выберите способ вывода (вывод на киви моментальный)",
                             reply_markup=kbs.withdraw_type)
        elif status == "qiwi_number":
            payment_sum = int(user['temp_field'])
            fee = json.loads(open("settings.json", "r").read())['withdraw_fee']
            sum_to_send = int(payment_sum - (payment_sum / 100 * fee))
            if payment_sum > user['balance']:
                return
            if qiwi_withdraw.transfer(text, sum_to_send):
                db.change_balance(-payment_sum)
                db.status()
                app.send_message(from_id, f"Выплата на номер {text} суммой {sum_to_send} RUB успешно выполнена")
                app.send_message(config.notifications_channel, "Новый вывод\n"
                                                               f"Пользователь: @{user['username']}\n"
                                                               f"Сумма вывода: {payment_sum} RUB\n"
                                                               "Тип вывода: Киви")
                for admin in config.admins:
                    app.send_message(admin, "Выплата на киви️\n"
                                            f"Пользователь: @{user['username']}\n"
                                            f"Сумма заявки: {payment_sum} RUB\n"
                                            f"Сумма для выплаты: {sum_to_send} RUB\n"
                                            "Тип вывода: Киви", reply_markup=kbs.inline_delete)
            else:
                app.send_message(from_id, "Ошибка выплаты, проверьте введённый номер или напишите администратору")
                db.status()
        elif user['status'].startswith("waiting_btc"):
            payment_id = user['status'].split("__")[1]
            if payment_id is None:
                return
            payment = db.get_payment(payment_id)
            if payment['status'] != 0:
                return
            check_id = re.findall(r"https://telegram\.me/BTC_CHANGE_BOT\?start=(.*)", text)
            if len(check_id) < 1:
                app.send_message(from_id, "Ошибка парсинга, попробуйте ещё раз", reply_markup=kbs.back_inline)
                return
            while True:
                if checking_banker:
                    sleep(0.1)
                    continue
                checking_banker = True
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                banker = Banker(config.api_id, config.api_hash)
                try:
                    result = banker.check_cheque(check_id[0])
                except Exception as e:
                    checking_banker = False
                    print(e)
                    sleep(0.1)
                    continue
                else:
                    del banker
                    checking_banker = False
                    break
            # noinspection PyUnboundLocalVariable
            if not result:
                app.send_message(from_id, "Невалидный чек, попробуйте ещё раз")
                return
            # noinspection PyUnboundLocalVariable
            db.change_balance(result)
            db.set_payment_status(payment_id, 1)
            # noinspection PyUnboundLocalVariable
            for admin in config.admins:
                app.send_message(admin, "Новое пополнение\n"
                                        f"Пользователь: @{user['username']}\n"
                                        f"Сумма: {result} RUB\n"
                                        f"Через БТК")
            app.send_message(from_id, "Счёт успешно оплачен\n"
                                      f"На баланс зачислено {result} RUB")
            db.status()
            return
        elif status == "find_user":
            nickname = re.sub(r"[^a-zA-Z0-9_]", "", text)
            finding_user = db.find_user(nickname)
            if finding_user is None:
                app.send_message(from_id, "Такого пользователя не существует\n"
                                          "Попробуйте ещё раз", reply_markup=kbs.back_inline)
                return
            if finding_user['tg'] == from_id:
                app.send_message(from_id, "Вы не можете открыть сделку с самим собой\n"
                                          "Попробуйте ещё раз", reply_markup=kbs.back_inline)
                return
            user_deals_count = db.get_deals_count(finding_user['tg'])
            user_deals_sum = db.get_deals_sum(finding_user['tg'])
            text = "Подробнее:\n\n" \
                   f"👤 Пользователь: @{finding_user['username']}\n\n" \
                   f"♻️ Количество сделок: {user_deals_count}\n\n" \
                   f"💳 Сумма сделок: {user_deals_sum} RUB\n\n" \
                   f"📊 Рейтинг: {finding_user['rating']}"
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("🔰 Открыть сделку", callback_data=json.dumps({
                "action": "open_deal",
                "user_id": finding_user['tg']
            })))
            db.status()
            app.send_message(from_id, text, reply_markup=kb)
        elif status == "admin_find_user":
            if from_id not in config.admins:
                return
            nickname = re.sub(r"[^a-zA-Z0-9_]", "", text)
            finding_user = db.find_user(nickname)
            if finding_user is None:
                app.send_message(from_id, "Такого пользователя не существует\n"
                                          "Попробуйте ещё раз", reply_markup=kbs.back_inline)
                return
            text = f"Пользователь @{finding_user['username']}\n" \
                   f"Баланс: {finding_user['balance']}\n\n" \
                   f"Введите новый баланс"
            db.status(f"new_balance__{finding_user['tg']}")
            app.send_message(from_id, text, reply_markup=kbs.admin_back)
        elif status.startswith("new_balance"):
            try:
                new_balance = int(text)
            except ValueError:
                app.send_message(from_id, "Введите число", reply_markup=kbs.admin_back)
                return
            if new_balance < 0:
                app.send_message(from_id, "Введите число больше или равное нулю", reply_markup=kbs.admin_back)
                return
            finding_user = int(status.split("__")[1])
            finding_user = Database(finding_user, app)
            finding_user.set_balance(new_balance)
            app.send_message(from_id, "Успешно установлено")
            db.status()
        elif status.startswith("deal_sum"):
            seller_id = status.split("__")[1]
            seller = db.find_user(user_id=seller_id)
            if seller is None:
                return
            try:
                deal_sum = float(text)
            except ValueError:
                app.send_message(from_id, "Введите число", reply_markup=kbs.back_inline)
                return
            if deal_sum < 10:
                app.send_message(from_id, "Минимальная сумма сделки - 10 руб")
                return
            if user['balance'] < deal_sum:
                app.send_message(from_id, "На вашем балансе недостаточно средств\n"
                                          f"Ваш баланс: {user['balance']} RUB", reply_markup=kbs.back_inline)
                return
            db.status(f"deal_info__{seller_id}__{deal_sum}")
            app.send_message(from_id, "🛑 ВАЖНО 🛑\n\n"
                                      "Максимально подробно опишите все условия которым должен соответствовать товар, "
                                      "арбитр не будет учитывать личную переписку с продавцом, если спорный момент не "
                                      "описывался на данном этапе.",
                             reply_markup=kbs.back_inline)
        elif status.startswith("deal_info"):
            db.status()
            seller_id = status.split("__")[1]
            seller = db.find_user(user_id=seller_id)
            deal_sum = status.split("__")[2]
            deal_info = text
            deal_id = db.add_deal(from_id, seller_id, deal_sum, deal_info)
            deal_message = f"🔰 Сделка: #{deal_id} успешно создана.\n\n" \
                           f"➖ Покупатель: @{user['username']}\n\n" \
                           f"➖ Продавец: @{seller['username']}\n\n" \
                           f"💰 Стоимость: {deal_sum} RUB\n\n" \
                           f"📝 Условия: {deal_info}\n\n" \
                           "♻️ Статус: Ожидает подтверждения"
            app.send_message(from_id, deal_message)
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Подтвердить", callback_data=json.dumps({
                "action": "confirm_deal",
                "deal_id": deal_id
            })))
            kb.add(types.InlineKeyboardButton("Отказаться", callback_data=json.dumps({
                "action": "decline_deal",
                "deal_id": deal_id
            })))
            app.send_message(seller_id, deal_message, reply_markup=kb)
        elif status == "ad_button_text":
            if from_id not in config.admins:
                return
            db.temp(text)
            db.status("ad_text")
            app.send_message(from_id, "А теперь введите текст рекламной кнопки")
        elif status == "ad_text":
            if from_id not in config.admins:
                return
            button_text = user['temp_field']
            button_id = db.add_ad_button(button_text, text, user['mailing_photo'])
            db.status()
            app.send_message(from_id, f"Вы успешно добавили рекламную кнопку, её Id: {button_id}",
                             reply_markup=kbs.admin_inline)
        elif status == "edit_button":
            if from_id not in config.admins:
                return
            db.change_button_text(user['temp_field'], text)
            db.status()
            app.send_message(from_id, f"Вы успешно изменили текст рекламной кнопки",
                             reply_markup=kbs.admin_inline)
        elif status == "withdraw_fee":
            if from_id not in config.admins:
                return
            try:
                fee = int(text)
            except ValueError:
                app.send_message(from_id, "Введите число", reply_markup=kbs.admin_back)
                return
            if fee > 99 or fee < 1:
                app.send_message(from_id, "Введите число от 1 до 99", reply_markup=kbs.admin_back)
                return
            open("settings.json", "w").write(json.dumps({"withdraw_fee": fee}))
            db.status()
            app.send_message(from_id, "Новая комиссия успешно установлена")
        elif status == "promocode_sum":
            if from_id not in config.admins:
                return
            try:
                promocode_sum = int(text)
            except ValueError:
                app.send_message(from_id, "Введите число", reply_markup=kbs.admin_back)
                return
            if promocode_sum <= 0:
                app.send_message(from_id, "Введите число больше 0")
                return
            db.status(f"promocode_activations__{promocode_sum}")
            app.send_message(from_id, "Введите количество активаций", reply_markup=kbs.admin_back)
        elif status.startswith("promocode_activations"):
            if from_id not in config.admins:
                return
            promocode_sum = int(status.split("__")[1])
            try:
                activations = int(text)
            except ValueError:
                app.send_message(from_id, "Введите число", reply_markup=kbs.admin_back)
                return
            if activations <= 0:
                app.send_message(from_id, "Введите число больше 0")
                return
            db.status()
            promocode_text = "".join([random.choice(string.ascii_letters + string.digits) for _ in range(10)])
            db.add_promocode(promocode_text, promocode_sum, activations)
            app.send_message(from_id, "Промокод успешно добавлен, вот его код:")
            app.send_message(from_id, promocode_text)
        elif status == "mailing_text":
            if from_id not in config.admins:
                return
            db.temp(text)
            db.status("mailing_date")
            count = len(db.get_all_users())
            if user['mailing_photo'] is None:
                app.send_message(from_id, f"Рассылку получат {count} пользователей\n"
                                          "Введите време отправки в МСК (20:00 или 2021-10-10 20:00)""",
                                 reply_markup=kbs.admin_back)
            else:
                attachment = user['mailing_photo'].split("|")
                if attachment[0] == "PHOTO":
                    app.send_photo(user['tg'], attachment[1], f"Рассылку получат {count} пользователей\n"
                                                              "Введите време отправки в МСК (20:00 или 2021-10-10 "
                                                              "20:00)""", reply_markup=kbs.admin_back)
                elif attachment[0] == "VIDEO":
                    app.send_video(user['tg'], attachment[1], caption=f"Рассылку получат {count} пользователей\n"
                                                                      "Введите време отправки в МСК (20:00 или "
                                                                      "2021-10-10 20:00)""",
                                   reply_markup=kbs.admin_back)
                elif attachment[0] == "DOCUMENT":
                    app.send_document(user['tg'], attachment[1], caption=f"Рассылку получат {count} пользователей\n"
                                                                         "Введите време отправки в МСК (20:00 или "
                                                                         "2021-10-10 20:00)""",
                                      reply_markup=kbs.admin_back)
                elif attachment[0] == "ANIMATION":
                    app.send_animation(user['tg'], attachment[1], caption=f"Рассылку получат {count} пользователей\n"
                                                                          "Введите време отправки в МСК (20:00 или "
                                                                          "2021-10-10 20:00)""",
                                       reply_markup=kbs.admin_back)
        elif status == "mailing_date":
            if from_id not in config.admins:
                return
            if re.fullmatch(r"^\d{1,2}:\d{2}$", text):
                time = re.findall(r"^(\d{1,2}):(\d{2})$", text)[0]
                now = datetime.now(timezone(timedelta(hours=3)))
                time = datetime(year=now.year, month=now.month, day=now.day, hour=int(time[0]),
                                minute=int(time[1]), tzinfo=timezone(timedelta(hours=3)))
            elif re.fullmatch(r"^(\d{4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})$", text):
                time = re.findall(r"^(\d{4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})$", text)[0]
                time = datetime(year=int(time[0]), month=int(time[1]), day=int(time[2]), hour=int(time[3]),
                                minute=int(time[4]), tzinfo=timezone(timedelta(hours=3)))
            else:
                app.send_message(from_id, "Неправильный формат даты", reply_markup=kbs.admin_back)
                return
            mailing_id = db.add_mailing(user['temp_field'], user['tg'], int(time.timestamp()), user['mailing_photo'])
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton("Подтвердить рассылку", callback_data=json.dumps(
                {'action': 'confirm_mailing', 'id': mailing_id})))
            keyboard.add(
                types.InlineKeyboardButton("Тестовое сообщение",
                                           callback_data=json.dumps({'action': 'test_mailing', 'id': mailing_id})),
                types.InlineKeyboardButton("Отменить рассылку",
                                           callback_data=json.dumps({'action': 'cancel_mailing', 'id': mailing_id})))
            if user['mailing_photo'] is None:
                app.send_message(from_id, f"""Время рассылки: {time}""", reply_markup=keyboard)
            else:
                attachment = user['mailing_photo'].split("|")
                if attachment[0] == "PHOTO":
                    app.send_photo(user['tg'], attachment[1], f"""Время рассылки: {time}""", reply_markup=keyboard)
                elif attachment[0] == "VIDEO":
                    app.send_video(user['tg'], attachment[1], caption=f"""Время рассылки: {time}""",
                                   reply_markup=keyboard)
                elif attachment[0] == "DOCUMENT":
                    app.send_document(user['tg'], attachment[1], caption=f"""Время рассылки: {time}""",
                                      reply_markup=keyboard)
                elif attachment[0] == "ANIMATION":
                    app.send_animation(user['tg'], attachment[1], caption=f"""Время рассылки: {time}""",
                                       reply_markup=keyboard)

            db.status()
        elif status.startswith("deal_feedback"):
            deal_id = int(status.split("__")[1])
            deal = db.get_deal(deal_id)
            if deal['status'] != "closed":
                db.status()
                return
            if deal['seller'] != from_id and deal['buyer'] != from_id:
                db.status()
                return
            if len(text) < 30:
                app.send_message(from_id, "Минимальная длина отзыва - 30 симовлов", reply_markup=kbs.back_inline)
                return
            buyer = db.find_user(user_id=deal['buyer'])
            seller = db.find_user(user_id=deal['seller'])
            if deal['seller'] == from_id:
                whom = "продавца"
            else:
                whom = "покупателя"
            db.status()
            feedback_award = int(deal['sum'] * 0.01)
            db.change_balance(feedback_award)
            channel_message = f"🔰 Сделка: #{deal_id}\n\n" \
                              f"➖ Покупатель: @{buyer['username']}\n\n" \
                              f"➖ Продавец: @{seller['username']}\n\n" \
                              f"💰 Сумма: {deal['sum']} RUB\n\n" \
                              f"♻️ Отзыв {whom}: {text}"
            app.send_message(config.feedback_channel, channel_message)
            app.send_message(config.notifications_channel, channel_message)
            app.send_message(from_id, f"Вы успешно оставили отзыв, на ваш баланс начислено {feedback_award}")
        elif status.startswith("communicate"):
            deal_id = int(status.split("__")[1])
            deal = db.get_deal(deal_id)
            if from_id != deal['seller'] and from_id != deal['buyer']:
                return
            if deal['status'] != "waiting_for_pay" and deal['status'] != "waiting_goods_transfer" or \
                    deal['status'] == "arbitrage":
                return
            db.add_communicate_message(deal_id, text)
            if from_id == deal['seller']:
                receiver = deal['buyer']
            else:
                receiver = deal['seller']
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
                {"action": "communicate", "deal_id": deal_id})))
            app.send_message(receiver, f"Новое сообщение в сделке #{deal_id}:\n\n"
                                       f"{user['username']}: {text}", reply_markup=kb)
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Выйти из режима переписки",
                                              callback_data=json.dumps({"action": "exit_communicate"})))
            app.send_message(from_id, "Ваше сообщение успешно отправлено второму участнику сделки", reply_markup=kb)


@app.message_handler(content_types=['animation'])
def gif(message):
    if message.chat.id < 0:
        return
    from_id = message.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    photo_id = message.animation.file_id
    status = user['status']
    if status == "mailing_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("ANIMATION|" + photo_id)
        app.send_message(from_id, "А теперь введите текст рассылки", reply_markup=kbs.admin_back)
    elif status == "ad_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("ANIMATION|" + photo_id)
        db.status("ad_text")
        app.send_message(from_id, "А теперь введите текст рекламной кнопки",
                         reply_markup=kbs.admin_back)


@app.message_handler(content_types=['document'])
def document(message):
    if message.chat.id < 0:
        return
    from_id = message.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    photo_id = message.document.file_id
    status = user['status']
    if status == "mailing_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("DOCUMENT|" + photo_id)
        app.send_message(from_id, "А теперь введите текст рассылки", reply_markup=kbs.admin_back)
    elif status == "ad_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("DOCUMENT|" + photo_id)
        db.status("ad_text")
        app.send_message(from_id, "А теперь введите текст рекламной кнопки",
                         reply_markup=kbs.admin_back)
    elif status.startswith("communicate"):
        deal_id = int(status.split("__")[1])
        deal = db.get_deal(deal_id)
        if from_id != deal['seller'] and from_id != deal['buyer']:
            return
        if deal['status'] != "waiting_for_pay" and deal['status'] != "waiting_goods_transfer" or \
                deal['status'] == "arbitrage":
            return
        db.add_communicate_message(deal_id, "[[файл]]")
        if from_id == deal['seller']:
            receiver = deal['buyer']
        else:
            receiver = deal['seller']
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
            {"action": "communicate", "deal_id": deal_id})))
        app.send_document(receiver, photo_id, caption=f"Новое сообщение в сделке #{deal_id}", reply_markup=kb)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Выйти из режима переписки",
                                          callback_data=json.dumps({"action": "exit_communicate"})))
        app.send_message(from_id, "Ваше сообщение успешно отправлено второму участнику сделки", reply_markup=kb)


@app.message_handler(content_types=['video'])
def video(message):
    if message.chat.id < 0:
        return
    from_id = message.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    photo_id = message.video.file_id
    status = user['status']
    if status == "mailing_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("VIDEO|" + photo_id)
        app.send_message(from_id, "А теперь введите текст рассылки", reply_markup=kbs.admin_back)
    elif status == "ad_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("VIDEO|" + photo_id)
        db.status("ad_text")
        app.send_message(from_id, "А теперь введите текст рекламной кнопки",
                         reply_markup=kbs.admin_back)
    elif status.startswith("communicate"):
        deal_id = int(status.split("__")[1])
        deal = db.get_deal(deal_id)
        if from_id != deal['seller'] and from_id != deal['buyer']:
            return
        if deal['status'] != "waiting_for_pay" and deal['status'] != "waiting_goods_transfer" or \
                deal['status'] == "arbitrage":
            return
        db.add_communicate_message(deal_id, "[[видео]]")
        if from_id == deal['seller']:
            receiver = deal['buyer']
        else:
            receiver = deal['seller']
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
            {"action": "communicate", "deal_id": deal_id})))
        app.send_video(receiver, photo_id, caption=f"Новое сообщение в сделке #{deal_id}", reply_markup=kb)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Выйти из режима переписки",
                                          callback_data=json.dumps({"action": "exit_communicate"})))
        app.send_message(from_id, "Ваше сообщение успешно отправлено второму участнику сделки", reply_markup=kb)


@app.message_handler(content_types=['photo'])
def photo(message):
    if message.chat.id < 0:
        return
    from_id = message.from_user.id
    db = Database(from_id, app)
    user = db.get_me()
    photo_id = message.photo[-1].file_id
    status = user['status']
    if status == "mailing_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("PHOTO|" + photo_id)
        app.send_message(from_id, "А теперь введите текст рассылки", reply_markup=kbs.admin_back)
    elif status == "ad_text":
        if from_id not in config.admins:
            return
        db.mailing_photo("PHOTO|" + photo_id)
        db.status("ad_text")
        app.send_message(from_id, "А теперь введите текст рекламной кнопки",
                         reply_markup=kbs.admin_back)
    elif status.startswith("communicate"):
        deal_id = int(status.split("__")[1])
        deal = db.get_deal(deal_id)
        if from_id != deal['seller'] and from_id != deal['buyer']:
            return
        if deal['status'] != "waiting_for_pay" and deal['status'] != "waiting_goods_transfer" or \
                deal['status'] == "arbitrage":
            return
        db.add_communicate_message(deal_id, "[[фото]]")
        if from_id == deal['seller']:
            receiver = deal['buyer']
        else:
            receiver = deal['seller']
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Перейти в режим переписки", callback_data=json.dumps(
            {"action": "communicate", "deal_id": deal_id})))
        app.send_photo(receiver, photo_id, caption=f"Новое сообщение в сделке #{deal_id}", reply_markup=kb)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Выйти из режима переписки",
                                          callback_data=json.dumps({"action": "exit_communicate"})))
        app.send_message(from_id, "Ваше сообщение успешно отправлено второму участнику сделки", reply_markup=kb)

print('Started')
print(f"https://t.me/{app.get_me().username}")
app.polling(True)
