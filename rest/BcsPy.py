import logging  # Будем вести лог
import time
from datetime import datetime, timedelta
from time import time_ns  # Текущее время в наносекундах, прошедших с 01.01.1970 UTC
import uuid  # Номера подписок должны быть уникальными во времени и пространстве
from json import loads, JSONDecodeError, dumps  # Сервер WebSockets работает с JSON сообщениями
from pytz import timezone, utc  # Работаем с временнОй зоной и UTC
import requests.adapters  # Настройки запросов/ответов
from requests import post, get, put, delete, Response  # Запросы/ответы от сервера запросов
from jwt import decode
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings


class BcsPy:
    """Работа с Bcs OpenAPI V2 https://trade-api.bcs.ru/ из Python"""
    requests.adapters.DEFAULT_RETRIES = 10  # Настройка кол-ва попыток
    requests.adapters.DEFAULT_POOL_TIMEOUT = 10  # Настройка таймауту запроса в секундах
    tz_msk = timezone('Europe/Moscow')  # Время UTC будем приводить к московскому времени
    jwt_token_ttl = 43200  # Время жизни токена JWT в секундах - 12 часов
    exchanges = ('MOEX', 'SPBX',)  # Биржи
    logger = logging.getLogger('BcsPy')  # Будем вести лог

    def __init__(self, refresh_token):
        """Инициализация

        :param str refresh_token: Токен
        :param bool demo: Режим демо торговли. По умолчанию установлен режим реальной торговли
        """
        self.oauth_server = f'https://be.broker.ru/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token'  # Сервер аутентификации
        self.api_server = f'https://be.broker.ru'  # Сервер запросов
        self.cws_server = f'wss://ws.broker.ru'  # Сервис работы с заявками WebSocket
        self.cws_socket = None  # Подключение к -серверу WebSocket
        self.ws_server = f'wss://ws.broker.ru'  # Сервис подписок и событий WebSocket
        self.ws_socket = None  # Подключение к серверу WebSocket
        self.ws_task = None  # Задача управления подписками WebSocket
        self.ws_ready = False  # WebSocket готов принимать запросы
        #
        self.on_error = self.default_handler  # Ошибка (Task)
        #
        self.refresh_token = refresh_token  # Токен
        self.jwt_token = None  # Токен JWT
        self.jwt_token_decoded = dict()  # Информация по портфелям
        self.jwt_token_issued = 0  # UNIX время в секундах выдачи токена JWT
        self.accounts = list()  # Счета (портфели по договорам)
        self.get_jwt_token()  # Получаем токен JWT
        if self.jwt_token_decoded:
            str1 = ""
            #all_agreements = self.jwt_token_decoded['agreements'].split(' ')  # Договоры
            #all_portfolios = self.jwt_token_decoded['portfolios'].split(' ')  # Портфели
        self.subscriptions = {}  # Справочник подписок. Для возобновления всех подписок после перезагрузки сервера Алор
        self.symbols = {}  # Справочник тикеров

    def __enter__(self):
        """Вход в класс, например, с with"""
        return self

    def get_positions_money(self, portfolio, exchange, without_currency=False, format='Simple'):
        """Получение информации о позициях
        """
        disable_warnings(InsecureRequestWarning)
        result = self.check_result(
            get(url=f'{self.api_server}/trade-api-bff-portfolio/api/v1/portfolio',
                headers=self.get_headers(), verify=False))
        _positions = []
        _money = []
        if result is None:
            return _positions, _money
        for row in result:
            str = ""
            if row['term'] == "T1" and row['board'] == "":
                _money.append(row)
            if row['term'] == "T1" and row['board'] != "":
                if float(row["quantity"]) > 0:
                    _positions.append(row)
        return _positions, _money

    def close_market_order(self, portfolio, exchange, symbol, side, quantity, security_board, comment='',
                           time_in_force='GoodTillCancelled'):
        """Создание рыночной заявки

        """
        if side <= 0:
            return None
        headers = self.get_headers()
        str_uuid = self.get_request_uuid()
        payload = {'clientOrderId': str_uuid,
                   'side': str(side),
                   'orderType': str(1),
                   'orderQuantity': int(quantity),
                   'ticker': symbol,
                   'classCode': security_board
                   }

        disable_warnings(InsecureRequestWarning)
        result = self.check_result(
            post(url=f'{self.api_server}/trade-api-bff-operations/api/v1/orders', headers=headers,
                 json=payload, verify=False))
        time.sleep(5)
        disable_warnings(InsecureRequestWarning)
        status = self.check_result(
            get(url=f'{self.api_server}/trade-api-bff-operations/api/v1/orders/{str_uuid}', headers=headers, verify=False))
        if status is None:
            return None
        if (status["data"]["orderStatus"] == "4"
                or status["data"]["orderStatus"] == "8"
                or status["data"]["orderStatus"] == "9"):
            return None
        return result

    def create_market_order(self, portfolio, exchange, symbol, side, quantity, security_board, comment='',
                            time_in_force='GoodTillCancelled'):
        """Создание рыночной заявки

        """
        if side <= 0:
            return None
        headers = self.get_headers()
        str_uuid = self.get_request_uuid()
        payload = {'clientOrderId': str_uuid,
                   'side': str(side),
                   'orderType': str(1),
                   'orderQuantity': int(quantity),
                   'ticker': symbol,
                   'classCode': security_board
                   }
        disable_warnings(InsecureRequestWarning)
        result = self.check_result(
            post(url=f'{self.api_server}/trade-api-bff-operations/api/v1/orders', headers=headers,
                 json=payload, verify=False))
        time.sleep(5)
        disable_warnings(InsecureRequestWarning)
        status = self.check_result(
            get(url=f'{self.api_server}/trade-api-bff-operations/api/v1/orders/{str_uuid}', headers=headers, verify=False))
        if status is None:
            return None
        if (status["data"]["orderStatus"] == "4"
                or status["data"]["orderStatus"] == "8"
                or status["data"]["orderStatus"] == "9"):
            return None
        return result

    def get_quotes(self, symbols, security_board, _date_start, _date_end):
        """Получение информации о котировках для выбранных инструментов
        """
        current_datetime = datetime.now()
        current_datetime_iso_8601_string = current_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        one_hour_ago = current_datetime - timedelta(hours=24)
        one_hour_ago_iso_8601_string = one_hour_ago.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        params = {'classCode': security_board,
                  'ticker': symbols,
                  'startDate': one_hour_ago_iso_8601_string,
                  'endDate': current_datetime_iso_8601_string,
                  'timeFrame': "M30",
                  }
        disable_warnings(InsecureRequestWarning)
        result = self.check_result(
            get(url=f'{self.api_server}'
                    f'/trade-api-market-data-connector/api/v1/candles-chart?'
                    f'classCode={security_board}'
                    f'&ticker={symbols}'
                    f'&startDate={one_hour_ago_iso_8601_string}'
                    f'&endDate={current_datetime_iso_8601_string}'
                    f'&timeFrame=M30',
                headers=self.get_headers(), timeout=60, verify=False))
        return result

    def get_jwt_token(self):
        """Получение, выдача, обновление JWT токена"""
        now = int(datetime.timestamp(datetime.now()))  # Текущая дата и время в виде UNIX времени в секундах
        payload = {
            'client_id': "trade-api-write",
            'refresh_token': self.refresh_token,
            'grant_type': "refresh_token"
        }
        if self.jwt_token is None or now - self.jwt_token_issued > self.jwt_token_ttl:  # Если токен JWT не был выдан или был просрочен
            response = post(url=f'{self.oauth_server}',
                            data=payload)  # Запрашиваем новый JWT токен с сервера аутентификации
            if response.status_code != 200:  # Если при получении токена возникла ошибка
                self.on_error(f'Ошибка получения JWT токена: {response.status_code}')  # Событие ошибки
                self.jwt_token = None  # Сбрасываем токен JWT
                self.jwt_token_decoded = None  # Сбрасываем данные о портфелях
                self.jwt_token_issued = 0  # Сбрасываем время выдачи токена JWT
            else:  # Токен получен
                token = response.json()  # Читаем данные JSON
                self.jwt_token = token['access_token']  # Получаем токен JWT
                self.jwt_token_decoded = decode(self.jwt_token, options={
                    'verify_signature': False})  # Получаем из него данные о портфелях
                self.jwt_token_issued = now  # Дата выдачи токена JWT
        return self.jwt_token

    def get_headers(self):
        """Получение хедеров для запросов"""
        return {'Content-Type': 'application/json', 'Authorization': f'Bearer {self.get_jwt_token()}'}

    def get_request_id(self):
        """Получение уникального кода запроса"""
        return f'{time_ns()}'  # Текущее время в наносекундах, прошедших с 01.01.1970 в UTC

    def get_request_uuid(self):
        """Получение уникального кода запроса"""
        return f'{uuid.uuid4()}'  # Текущее время в наносекундах, прошедших с 01.01.1970 в UTC

    def check_result(self, response):
        """Анализ результата запроса


        :param Response response: Результат запроса
        :return: Справочник из JSON, текст, None в случае веб ошибки
        """

        if not response:  # Если ответ не пришел. Например, при таймауте
            self.on_error('Ошибка сервера: Таймаут')  # Событие ошибки
            return None  # то возвращаем пустое значение
        content = response.content.decode('utf-8')  # Результат запроса
        if response.status_code != 200:  # Если статус ошибки
            self.on_error(
                f'Ошибка сервера: {response.status_code} Запрос: {response.request.path_url} Ответ: {content}')  # Событие ошибки
            return None  # то возвращаем пустое значение
        # self.logger.debug(f'Запрос: {response.request.path_url} Ответ: {content}')  # Для отладки
        try:
            return loads(
                content)  # Декодируем JSON в справочник, возвращаем его. Ошибки также могут приходить в виде JSON
        except JSONDecodeError:  # Если произошла ошибка при декодировании JSON, например, при удалении заявок
            return content  # то возвращаем значение в виде текста

    def msk_datetime_to_utc_timestamp(self, dt) -> int:
        """Перевод московского времени в кол-во секунд, прошедших с 01.01.1970 00:00 UTC

        :param datetime dt: Московское время
        :return: Кол-во секунд, прошедших с 01.01.1970 00:00 UTC
        """
        dt_msk = self.tz_msk.localize(dt)  # Заданное время ставим в зону МСК
        return int(dt_msk.timestamp())  # Переводим в кол-во секунд, прошедших с 01.01.1970 в UTC

    def utc_timestamp_to_msk_datetime(self, seconds) -> datetime:
        """Перевод кол-ва секунд, прошедших с 01.01.1970 00:00 UTC в московское время

        :param int seconds: Кол-во секунд, прошедших с 01.01.1970 00:00 UTC
        :return: Московское время без временнОй зоны
        """
        dt_utc = datetime.utcfromtimestamp(seconds)  # Переводим кол-во секунд, прошедших с 01.01.1970 в UTC
        return self.utc_to_msk_datetime(dt_utc)  # Переводим время из UTC в московское

    def msk_to_utc_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из московского в UTC

        :param datetime dt: Московское время
        :param bool tzinfo: Отображать временнУю зону
        :return: Время UTC
        """
        dt_msk = self.tz_msk.localize(dt)  # Задаем временнУю зону МСК
        dt_utc = dt_msk.astimezone(utc)  # Переводим в UTC
        return dt_utc if tzinfo else dt_utc.replace(tzinfo=None)

    def utc_to_msk_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из UTC в московское

        :param datetime dt: Время UTC
        :param bool tzinfo: Отображать временнУю зону
        :return: Московское время
        """
        dt_utc = utc.localize(dt)  # Задаем временнУю зону UTC
        dt_msk = dt_utc.astimezone(self.tz_msk)  # Переводим в МСК
        return dt_msk if tzinfo else dt_msk.replace(tzinfo=None)

    def default_handler(self, response=None):
        """Пустой обработчик события по умолчанию. Его можно заменить на пользовательский"""
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Выход из класса, например, с with"""
        str = ""

    def __del__(self):
        str = ""
