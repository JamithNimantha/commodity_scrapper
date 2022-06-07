import json
import os
import re
from datetime import datetime, date, timedelta

import psycopg2
import requests
from bs4 import BeautifulSoup

DATE_FORMATE = '%A %B %d %Y'
TIME_FORMATE = '%H:%M %p'
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/97.0.4692.71 Safari/537.36",
    "referer": "tradingeconomics.com", }


def trim(txt):
    return txt.replace('\\n', '').strip()


def get_date(tds):
    txt = tds[7].text.replace('\\n', '').strip()
    txt = txt + "/" + str(date.today().year)
    dt = datetime.strptime(txt, '%b/%d/%Y')
    return dt


def is_valid_panel(panel):
    header = panel.find('thead')
    panel_type = trim(header.tr.th.text)
    if panel_type in ['', None]:
        return False
    else:
        return True


class CommodityScrapper:

    def __init__(self):
        self.conn = None
        self.init_db()
        data_arr = self.scrap_commodities()
        self.upsert_data(data_arr)
        data_arr = self.scrap_commodities_baltic()
        self.upsert_data(data_arr)
        data_arr = self.scrap_commodities_baltic_api()
        self.upsert_data(data_arr)

    def init_db(self):
        env = json.loads(open(f"Data{os.sep}Creadentals.json", "r", encoding='utf-8').read())
        self.conn = psycopg2.connect(
            host=env["host"],
            database=env["database"],
            user=env["user"],
            password=env["password"],
            port=env["port"],
        )

    def scrap_commodities(self):
        html = requests.get("https://tradingeconomics.com/commodities", headers=headers)
        soup = BeautifulSoup(html.text, "lxml")
        panels = soup.findAll("div", {'class': 'panel panel-default'})

        data_arr = []
        for panel in panels:
            # if is_valid_panel(panel):
            df = self.get_panel_data(panel)

            data_arr.extend(df)
        return data_arr

    def scrap_commodities_baltic(self):
        html = requests.get("https://tradingeconomics.com/commodity/baltic", headers=headers)
        soup = BeautifulSoup(html.text, "lxml")
        panel = soup.find("table", {'class': 'table table-hover sortable-theme-minimal table-heatmap'})
        panel.find('thead').extract()
        data_arr = self.get_panel_data_baltic(panel)

        return data_arr

    def scrap_commodities_baltic_api(self):
        html = requests.get("https://finance-api.seekingalpha.com/real_time_quotes?sa_ids=601301")
        json_ = html.json()
        data_arr = []
        for record in json_['real_time_quotes']:
            df = self.get_row_value_baltic_api(record)
            if not self.is_exists(df):
                data_arr.append(df)
        return data_arr

    def get_panel_data(self, panel):

        body = panel.find('tbody')
        rows = body.findAll('tr')
        data_arr = []
        for row in rows:
            df = self.get_row_value(row)
            if df is not None:
                if not self.is_exists(df):
                    data_arr.append(df)
        return data_arr

    def get_panel_data_baltic(self, panel):
        rows = panel.findAll('tr')
        data_arr = []
        for row in rows:
            df = self.get_row_value_baltic(row)
            if df is not None:
                if not self.is_exists(df):
                    data_arr.append(df)
        return data_arr

    def get_row_value(self, row):
        df = {}
        tds = row.findAll('td')
        if trim(tds[0].a.text).__contains__('Baltic'):
            return None
        df['commodity_name'] = trim(tds[0].a.text)
        df['update_date'] = date.today()
        df['update_time'] = datetime.now().time()
        df['price'] = self.get_float(trim(tds[1].text))
        df['change'] = self.get_float(trim(tds[2].text))
        df['day_percent'] = self.get_float(trim(tds[3].text).replace("%", ""))
        df['week_percent'] = self.get_float(trim(tds[4].text).replace("%", ""))
        df['month_percent'] = self.get_float(trim(tds[5].text).replace("%", ""))
        df['yoy_percent'] = self.get_float(trim(tds[6].text).replace("%", ""))
        txt = trim(tds[0].div.text)
        if "/" in txt:
            index = txt.index("/")
            df['currency'] = txt[:index]
            df['quantity'] = txt[index + 1:]
        else:
            df['currency'] = txt
            df['quantity'] = None
        if df['currency'].__contains__('Points'):
            df['currency'] = 'Index'
            df['quantity'] = 'Points'
        df['data_date'] = get_date(tds)
        df['last_price'] = None
        df['last_percent'] = None
        return df

    def get_row_value_baltic(self, row):
        df = {}
        tds = row.findAll('td')
        if not trim(tds[0].a.text).__contains__('Baltic'):
            return None
        df['commodity_name'] = trim(tds[0].a.text)
        df['update_date'] = date.today()
        df['update_time'] = datetime.now().time()
        df['price'] = self.get_float(trim(tds[1].text))
        df['change'] = self.get_float(trim(tds[3].text))
        df['day_percent'] = self.get_float(trim(tds[4].text).replace("%", ""))
        df['week_percent'] = 0
        df['month_percent'] = self.get_float(trim(tds[5].text).replace("%", ""))
        df['yoy_percent'] = self.get_float(trim(tds[6].text).replace("%", ""))
        df['currency'] = 'Index'
        df['quantity'] = None
        df['data_date'] = get_date(tds)
        df['last_price'] = None
        return df

    def get_row_value_baltic_api(self, row):
        today = datetime.today()
        df = {}
        df['commodity_name'] = 'Baltic Dry R'

        week_close = self.get_price_by_updated_date(today - timedelta(days=7), df['commodity_name'])
        month_close = self.get_price_by_updated_date(today - timedelta(days=30), df['commodity_name'])
        year_close = self.get_price_by_updated_date(today - timedelta(days=365), df['commodity_name'])

        df['update_date'] = date.today()
        df['update_time'] = datetime.now().time()
        df['price'] = row['close']
        df['last_price'] = row['prev_close']
        df['last_percent'] = (df['price'] - df['last_price']) / df['last_price']
        df['change'] = df['price'] - row['last']
        df['day_percent'] = round((df['price'] - df['last_price']) / df['last_price'], 4)

        if week_close is not None:
            df['week_percent'] = (week_close - week_close) / week_close
        else:
            df['week_percent'] = None

        if month_close is not None:
            df['month_percent'] = (month_close - month_close) / month_close
        else:
            df['month_percent'] = None

        if year_close is not None:
            df['yoy_percent'] = (year_close - year_close) / year_close
        else:
            df['yoy_percent'] = None

        df['currency'] = 'Index'
        df['quantity'] = None
        df['data_date'] = row['updated_at']
        return df

    def get_price_by_updated_date(self, update_date, name):
        update_date = update_date.strftime('%Y-%m-%d')
        sql = "select price from commodities where commodity_name = '{0}' and update_date = '{1}';" \
            .format(name, update_date)
        curr = self.conn.cursor()
        try:
            curr.execute(sql)
            rst = curr.fetchone()
            if rst is not None:
                return rst[0]
        except Exception as e:
            print("error found in get_price_by_updated_date()")
            print(e)
        return None

    def is_exists(self, df) -> bool:
        """

        :rtype: bool
        """
        sql = """select *
                from commodities
                where commodity_name = '{0}'
                  and price = {1}
                  and change = {2}
                  and day_percent = {3}
                  and week_percent = {4}
                  and month_percent = {5}
                  and yoy_percent = {6}
                  and currency = '{7}'
                  and quantity = '{8}'
                  and data_date = '{9}'
                  and last_price = '{10}'
                  and update_date = '{11}';
                  """.format(
            df['commodity_name'],
            df['price'],
            df['change'],
            df['day_percent'],
            df['week_percent'],
            df['month_percent'],
            df['yoy_percent'],
            df['currency'],
            df['quantity'],
            df['data_date'],
            df['last_price'],
            df['update_date']
        )
        sql = sql.replace("= 'None'", "is null").replace('= None', 'is null')
        curr = self.conn.cursor()
        try:
            curr.execute(sql)
            rst = curr.fetchone()
            if rst is not None:
                return len(rst) > 0
        except Exception as e:
            print("error found in is_exists()")
            print(e)
        return False

    def upsert_data(self, data_arr):
        sql = """
            insert into public.commodities (
                commodity_name,
                update_date,
                update_time,
                price,
                change,
                day_percent,
                week_percent,
                month_percent,
                yoy_percent,
                currency,
                quantity,
                data_date,
                last_price
            )
            values (
                %(commodity_name)s,
                %(update_date)s,
                %(update_time)s,
                %(price)s,
                %(change)s,
                %(day_percent)s,
                %(week_percent)s,
                %(month_percent)s,
                %(yoy_percent)s,
                %(currency)s,
                %(quantity)s,
                %(data_date)s,
                %(last_price)s
            )
            on conflict (commodity_name, update_date) do update
            set
                update_time = excluded.update_time,
                price = excluded.price,
                change = excluded.change,
                day_percent = excluded.day_percent,
                week_percent = excluded.week_percent,
                month_percent = excluded.month_percent,
                yoy_percent = excluded.yoy_percent,
                currency = excluded.currency,
                quantity = excluded.quantity,
                data_date = excluded.data_date
            ;
        """

        curr = self.conn.cursor()
        count = 0
        for row in data_arr:
            print(row)
            try:
                # q =  curr.mogrify(query,column_values)
                # print(row)
                curr.execute(sql, row)
                count = count + 1
            except Exception as e:
                print("error found")
                print(e)
                self.conn.rollback()
                print(row)
                return False
        print(f"Total {count} rows updated Successfully")
        self.conn.commit()

        return True

    @staticmethod
    def remove_non_numeric(value):
        value = value.strip()
        if value and value.strip():
            non_decimal = re.compile(r'[^\d.-]+')
            value = non_decimal.sub('', value)
            return value
        else:
            ""

    def get_float(self, value):

        if value is None:
            return 0
        elif type(value) == str:
            value = self.remove_non_numeric(value)
            if not value is None:
                try:
                    if len(value) > 0:
                        return float(value)
                    return 0
                except Exception:
                    return 0
            else:
                return 0
        else:
            return float(value)

    def __del__(self):
        self.conn.close()


scrapper = CommodityScrapper()
