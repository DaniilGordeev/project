#   Copyright (c) 2021. Tocenomiczs

import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Union

from telebot import TeleBot


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class Database:
    _user_id: Optional[int]
    _db: sqlite3.Connection  # Sqlite connection to db
    _cursor: sqlite3.Cursor

    def __init__(self, user_id: Optional[int] = None, app: Optional[TeleBot] = None):
        self._user_id = user_id
        self._db = sqlite3.connect("garant.sqlite")
        self._db.row_factory = dict_factory
        self._cursor = self._db.cursor()
        if self._user_id is None:
            return
        username = app.get_chat(self._user_id).username
        query = "SELECT * FROM users WHERE tg = ?"
        args = (self._user_id,)
        self._cursor.execute(query, args)
        user = self._cursor.fetchone()
        if user is None:
            now = int(datetime.now(timezone(timedelta(hours=3))).timestamp())
            query = "INSERT INTO users (tg, username, balance, rating, status, temp_field, reg_time) VALUES " \
                    "(?, ?, 0, 0, NULL, NULL, ?)"
            args = (self._user_id, username, now)
            self._cursor.execute(query, args)
            self._db.commit()
        else:
            if user['username'] != username:
                query = "UPDATE users SET username = ? WHERE tg = ?"
                args = (username, self._user_id)
                self._cursor.execute(query, args)
                self._db.commit()

    def get_me(self):
        query = "SELECT * FROM users WHERE tg = ?"
        args = (self._user_id,)
        self._cursor.execute(query, args)
        return self._cursor.fetchone()

    def status(self, status_text: Optional[str] = None):
        query = "UPDATE users SET status = ? WHERE tg = ?"
        args = (status_text, self._user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def temp(self, temp_text: Union[str, int, float] = None):
        query = "UPDATE users SET temp_field = ? WHERE tg = ?"
        args = (temp_text, self._user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def change_balance(self, adding_sum: int):
        query = "UPDATE users SET balance = balance + ? WHERE tg = ?"
        args = (adding_sum, self._user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def set_balance(self, new_balance: int):
        query = "UPDATE users SET balance = ? WHERE tg = ?"
        args = (new_balance, self._user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def add_payment(self, payment_id: str, price: Union[int, float], payment_type: str):
        query = "INSERT INTO payments (id, sum, type, status, user_id) VALUES (?, ?, ?, 0, ?)"
        args = (payment_id, price, payment_type, self._user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def get_payment(self, payment_id: str):
        query = "SELECT * FROM payments WHERE id = ?"
        args = (payment_id,)
        self._cursor.execute(query, args)
        return self._cursor.fetchone()

    def set_payment_status(self, payment_id: str, status: int):
        query = "UPDATE payments SET status = ? WHERE id = ?"
        args = (status, payment_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def find_user(self, nickname: Optional[str] = None, user_id: Optional[int] = None):
        if nickname is not None:
            query = "SELECT * FROM users WHERE username = ?"
            args = (nickname,)
        else:
            query = "SELECT * FROM users WHERE tg = ?"
            args = (user_id,)
        self._cursor.execute(query, args)
        return self._cursor.fetchone()

    def get_deals_sum(self, from_id: Optional[int] = None):
        query = "SELECT SUM(sum) FROM deals WHERE (buyer = ? OR seller = ?) AND status = 'closed'"
        if from_id is None:
            args = (self._user_id, self._user_id)
        else:
            args = (from_id, from_id)
        self._cursor.execute(query, args)
        deals_sum = self._cursor.fetchone()['SUM(sum)']
        return 0 if deals_sum is None else deals_sum

    def get_deals_count(self, from_id: Optional[int] = None):
        query = "SELECT COUNT(*) FROM deals WHERE (buyer = ? OR seller = ?) AND status = 'closed'"
        if from_id is None:
            args = (self._user_id, self._user_id)
        else:
            args = (from_id, from_id)
        self._cursor.execute(query, args)
        return self._cursor.fetchone()['COUNT(*)']

    def add_deal(self, buyer_id: int, seller_id: int, deal_sum: Union[float, int], deal_info: str):
        now = int(datetime.now(timezone(timedelta(hours=3))).timestamp())
        query = "INSERT INTO deals (seller, buyer, sum, status, create_time, info) VALUES " \
                "(?, ?, ?, 'waiting_seller', ?, ?)"
        args = (seller_id, buyer_id, deal_sum, now, deal_info)
        self._cursor.execute(query, args)
        self._db.commit()
        return self._cursor.lastrowid

    def set_deal_status(self, deal_id: int, deal_status: str):
        query = "UPDATE deals SET status = ? WHERE id = ?"
        args = (deal_status, deal_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def get_deal(self, deal_id: int):
        query = "SELECT * FROM deals WHERE id = ?"
        args = (deal_id,)
        self._cursor.execute(query, args)
        return self._cursor.fetchone()

    def set_active_deal(self, user_id: int, deal_id: int):
        query = "UPDATE users SET active_deal = ? WHERE tg = ?"
        if user_id is None:
            args = (deal_id, self._user_id)
        else:
            args = (deal_id, user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def add_rating(self, user_id: int, rating: int):
        query = "UPDATE users SET rating = rating + ? WHERE tg = ?"
        args = (rating, user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def add_ad_button(self, name: str, text: str, photo_id: Optional[str]):
        query = "INSERT INTO ads (button_name, button_text, photo_id) VALUES (?, ?, ?)"
        args = (name, text, photo_id)
        self._cursor.execute(query, args)
        self._db.commit()
        return self._cursor.lastrowid

    def mailing_photo(self, photo_id: Optional[str] = None):
        query = "UPDATE users SET mailing_photo = ? WHERE tg = ?"
        args = (photo_id, self._user_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def get_ads(self):
        query = "SELECT * FROM ads"
        self._cursor.execute(query)
        return self._cursor.fetchall()

    def remove_ad_button(self, button_id: int):
        query = "DELETE FROM ads WHERE id = ?"
        args = (button_id,)
        self._cursor.execute(query, args)
        self._db.commit()

    def change_button_text(self, button_id: int, button_text: str):
        query = "UPDATE ads SET button_text = ? WHERE id = ?"
        args = (button_text, button_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def add_promocode(self, promocode_text: str, promocode_sum: int, promocode_activations: int):
        query = "INSERT INTO coupons (sum, code, activated, max_activations) VALUES (?, ?, 0, ?)"
        args = (promocode_sum, promocode_text, promocode_activations)
        self._cursor.execute(query, args)
        self._db.commit()

    def can_activate_promo(self, text: str):
        query = "SELECT * FROM coupons WHERE code = ? AND max_activations > activated"
        args = (text,)
        self._cursor.execute(query, args)
        if self._cursor.fetchone() is None:
            return False
        return True

    def activate_promo(self, text: str):
        query = "SELECT * FROM coupons WHERE code = ? AND max_activations > activated"
        args = (text,)
        self._cursor.execute(query, args)
        promo = self._cursor.fetchone()
        if promo is None:
            return False
        query = "UPDATE coupons SET activated = activated + 1 WHERE id = ?"
        args = (promo['id'],)
        self._cursor.execute(query, args)
        return promo['sum']

    def get_deals(self):
        query = "SELECT * FROM deals WHERE seller = ? OR buyer = ?"
        args = (self._user_id, self._user_id)
        self._cursor.execute(query, args)
        return self._cursor.fetchall()

    def get_arbitrage_deals(self):
        query = "SELECT * FROM deals WHERE status = 'arbitrage'"
        self._cursor.execute(query)
        return self._cursor.fetchall()

    def get_all_users(self):
        query = "SELECT * FROM users"
        self._cursor.execute(query)
        return self._cursor.fetchall()

    def add_mailing(self, mailing_text: str, user_id: int, mailing_date: int, mailing_photo: Optional[str] = None):
        query = "INSERT INTO mailings (mailing_text, photo_id, send_time, created_by) VALUES (?, ?, ?, ?)"
        args = (mailing_text, mailing_photo, mailing_date, user_id)
        self._cursor.execute(query, args)
        self._db.commit()
        return self._cursor.lastrowid

    def get_mailing(self, mailing_id: int):
        query = "SELECT * FROM mailings WHERE id = ?"
        args = (mailing_id,)
        self._cursor.execute(query, args)
        return self._cursor.fetchone()

    def confirm_mailing(self, mailing_id: int):
        query = "UPDATE mailings SET confirmed = 1 WHERE id = ?"
        args = (mailing_id,)
        self._cursor.execute(query, args)
        self._db.commit()

    def delete_mailing(self, mailing_id: int):
        query = "DELETE FROM mailings WHERE id = ?"
        args = (mailing_id,)
        self._cursor.execute(query, args)
        self._db.commit()

    def get_mailings_to_send(self, timestamp: int):
        query = "SELECT * FROM mailings WHERE send_time < ? AND confirmed = 1 AND status = 0"
        args = (timestamp,)
        self._cursor.execute(query, args)
        return self._cursor.fetchall()

    def update_mailing_status(self, mailing_id: int, status: int):
        query = "UPDATE mailings SET status = ? WHERE id = ?"
        args = (status, mailing_id)
        self._cursor.execute(query, args)
        self._db.commit()

    def get_users_count(self, period: str):
        if period not in ['day', 'week', 'month']:
            return
        msk_tz = timezone(timedelta(hours=3))
        now = datetime.now(msk_tz)
        if period == "day":
            start_from = int((now - timedelta(days=1)).timestamp())
        elif period == "week":
            start_from = int((now - timedelta(weeks=1)).timestamp())
        elif period == "month":
            start_from = int((now - timedelta(weeks=4)).timestamp())
        else:
            start_from = int((now - timedelta(days=1)).timestamp())
        query = "SELECT COUNT(*) FROM users WHERE reg_time > ?"
        args = (start_from,)
        self._cursor.execute(query, args)
        return self._cursor.fetchone()["COUNT(*)"]

    def get_active_deals(self):
        query = "SELECT * FROM deals WHERE status != 'closed' AND status != 'canceled' " \
                "AND status != 'closed_arbitrage' AND status != 'waiting_seller'"
        self._cursor.execute(query)
        return self._cursor.fetchall()

    def get_deals_stats(self, status: Optional[str] = None, period: Optional[str] = None):
        if status is None and period is None:
            query = "SELECT COUNT(*) FROM deals"
            args = ()
        elif status is not None and period is None:
            if status == "active":
                query = "SELECT COUNT(*) FROM deals WHERE status != 'closed' AND status != 'canceled' " \
                        "AND status != 'closed_arbitrage' AND status != 'waiting_seller'"
                args = ()
        elif period is not None and status is None:
            msk_tz = timezone(timedelta(hours=3))
            now = datetime.now(msk_tz)
            if period == "day":
                start_from = int((now - timedelta(days=1)).timestamp())
            elif period == "week":
                start_from = int((now - timedelta(weeks=1)).timestamp())
            elif period == "month":
                start_from = int((now - timedelta(weeks=4)).timestamp())
            else:
                start_from = int((now - timedelta(days=1)).timestamp())
            query = "SELECT COUNT(*) FROM deals WHERE create_time > ?"
            args = (start_from,)
        # noinspection PyUnboundLocalVariable
        self._cursor.execute(query, args)
        return self._cursor.fetchone()['COUNT(*)']

    def get_users_balances(self):
        query = "SELECT SUM(balance) FROM users"
        self._cursor.execute(query)
        return self._cursor.fetchone()['SUM(balance)']

    def active_deals_sum(self):
        query = "SELECT SUM(sum) FROM deals WHERE status != 'closed' AND status != 'canceled' " \
                "AND status != 'closed_arbitrage' AND status != 'waiting_seller'"
        self._cursor.execute(query)
        return self._cursor.fetchone()['SUM(sum)']

    def add_communicate_message(self, deal_id, message):
        query = "INSERT INTO communicate (deal_id, user_id, message) VALUES (?, ?, ?)"
        args = (deal_id, self._user_id, message)
        self._cursor.execute(query, args)
        self._db.commit()

    def get_deal_messages(self, deal_id):
        query = "SELECT * FROM communicate WHERE deal_id = ?"
        args = (deal_id,)
        self._cursor.execute(query, args)
        return self._cursor.fetchall()
