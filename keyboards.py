#   Copyright (c) 2021. Tocenomiczs

import json

from telebot import types

from database import Database

main_kb = types.ReplyKeyboardMarkup(True)
main_kb.add(
    types.KeyboardButton("🤝 Сделки"),
    types.KeyboardButton("🖥 Профиль")
)
main_kb.add(
    types.KeyboardButton("🔍 Поиск пользователя"),
    types.KeyboardButton("🌐 О боте")
)

inline_delete = types.InlineKeyboardMarkup()
inline_delete.add(types.InlineKeyboardButton("❌ Понятно", callback_data=json.dumps({"action": "delete"})))

cabinet_inline = types.InlineKeyboardMarkup()
cabinet_inline.add(
    types.InlineKeyboardButton("Пополнить баланс", callback_data=json.dumps({"action": "deposit"})),
    types.InlineKeyboardButton("Вывести", callback_data=json.dumps({"action": "withdraw"}))
)
cabinet_inline.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "back"})))

deposit_type = types.InlineKeyboardMarkup()
deposit_type.add(types.InlineKeyboardButton("QIWI", callback_data=json.dumps({
    "action": "deposit_type",
    "type": "qiwi"
})))
deposit_type.add(types.InlineKeyboardButton("BTC чек", callback_data=json.dumps({
    "action": "deposit_type",
    "type": "btc"
})))

withdraw_type = types.InlineKeyboardMarkup()
withdraw_type.add(types.InlineKeyboardButton("QIWI", callback_data=json.dumps({
     "action": "withdraw_type",
     "type": "qiwi"
 })))
withdraw_type.add(types.InlineKeyboardButton("BTC чек", callback_data=json.dumps({
    "action": "withdraw_type",
    "type": "btc"
})))

admin_inline = types.InlineKeyboardMarkup()
admin_inline.add(
    types.InlineKeyboardButton("Комиссия", callback_data=json.dumps({"action": "withdraw_fee"})),
    types.InlineKeyboardButton("Активные сделки", callback_data=json.dumps({"action": "admin_active_deals"}))
)
admin_inline.add(
    types.InlineKeyboardButton("Рассылка", callback_data=json.dumps({"action": "mailing"})),
    types.InlineKeyboardButton("Статистика", callback_data=json.dumps({"action": "admin_stats"})),
)
admin_inline.add(
    types.InlineKeyboardButton("Промокод", callback_data=json.dumps({"action": "add_promocode"})),
    types.InlineKeyboardButton("Бэкап базы", callback_data=json.dumps({"action": "database_backup"}))
)
admin_inline.add(
    types.InlineKeyboardButton("Арбитражи", callback_data=json.dumps({"action": "arbitrages"})),
    types.InlineKeyboardButton("Изменить баланс", callback_data=json.dumps({"action": "admin_find_user"}))
)

admin_back = types.InlineKeyboardMarkup()
admin_back.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "admin_back"})))

back_inline = types.InlineKeyboardMarkup()
back_inline.add(types.InlineKeyboardButton("Назад", callback_data=json.dumps({"action": "back"})))
