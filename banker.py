#  Copyright (c) 2021. Tocenomiczs
import re
import time
from typing import Optional, Union

from telethon import TelegramClient


class Banker:
    _client: TelegramClient
    _me_id: int

    def __init__(self, api_id: int, api_hash: str, number: Optional[str] = None,
                 password: Optional[str] = None):
        if number is not None:
            if password is not None:
                self._client = TelegramClient("banker", api_id=api_id, api_hash=api_hash).start(
                    phone=lambda: number, password=lambda: password)
            else:
                self._client = TelegramClient("banker", api_id=api_id, api_hash=api_hash).start(
                    phone=lambda: number)
        else:
            self._client = TelegramClient("banker", api_id=api_id, api_hash=api_hash).start()
        self._me_id = self._client.loop.run_until_complete(self._client.get_me()).id

    def __del__(self):
        self._client.disconnect()
        del self._client

    def check_cheque(self, cheque_id: str) -> Union[bool, float]:
        self._client.loop.run_until_complete(self._client.send_message('BTC_CHANGE_BOT', f'/start {cheque_id}'))
        response = self._client.loop.run_until_complete(self._get_last_message())
        if "Упс, кажется, данный чек успел обналичить кто-то другой 😟" in response:
            return False
        try:
            response = float(re.findall(r'Вы получили \d+\.\d+ BTC \(([\d.]+) RUB\)', response)[0])
        except IndexError or ValueError:
            return False
        return response

    async def _get_last_message(self) -> str:
        while True:
            message = (await self._client.get_messages("BTC_CHANGE_BOT", limit=1))[0]
            if message.message.startswith("Приветствую,"):
                time.sleep(0.5)
                continue
            if message.from_id is not None:
                if message.from_id.user_id == self._me_id:
                    time.sleep(0.5)
                    continue
            else:
                return message.message
