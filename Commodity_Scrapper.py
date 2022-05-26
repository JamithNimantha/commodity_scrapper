import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import psycopg2
import json
import re

DATE_FORMATE = '%A %B %d %Y'
TIME_FORMATE = '%H:%M %p'
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/97.0.4692.71 Safari/537.36",
    "referer": "https://https://tradingeconomics.com/", }


class Commodity_Srapper:

    def __init__(self):
        self.init_db()
        data_arr = self.scrap_commodities()
        self.upsert_data(data_arr)

    def init_db(self):
        SECRETS = json.loads(open("secrets.json", "r").read())
        self.conn = psycopg2.connect(
            host=SECRETS["POSTGRES_HOST"],
            database=SECRETS["POSTGRES_DB"],
            user=SECRETS["POSTGRES_USER"],
            password=SECRETS["POSTGRES_PASSWORD"],
        )

    def scrap_commodities(self):
        html = requests.get("https://tradingeconomics.com/commodities", headers=headers)
        soup = BeautifulSoup(html.text, "lxml")
        panels = soup.findAll("div", {'class': 'panel panel-default'})

        data_arr = []
        for panel in panels:
            if self.isValidPanel(panel):
                df = self.get_panel_data(panel)
                data_arr.extend(df)
        return data_arr

    def isValidPanel(self, panel):
        header = panel.find('thead')
        panel_type = self.trim(header.tr.th.text)
        if panel_type == 'Index':
            return False
        else:
            return True

    def get_panel_data(self, panel):

        body = panel.find('tbody')
        rows = body.findAll('tr')
        data_arr = []
        for row in rows:
            df = self.get_row_value(row)
            data_arr.append(df)
        return data_arr

    def get_row_value(self, row):
        df = {}
        tds = row.findAll('td')
        df['commodity_name'] = self.trim(tds[0].a.text)
        df['update_date'] = date.today()
        df['update_time'] = datetime.now().time()
        df['price'] = self.getFloat(self.trim(tds[1].text))
        df['change'] = self.getFloat(self.trim(tds[2].text))
        df['day_percent'] = self.getFloat(self.trim(tds[3].text).replace("%", ""))
        df['week_percent'] = self.getFloat(self.trim(tds[4].text).replace("%", ""))
        df['month_percent'] = self.getFloat(self.trim(tds[5].text).replace("%", ""))
        df['yoy_percent'] = self.getFloat(self.trim(tds[6].text).replace("%", ""))
        txt = self.trim(tds[0].div.text)
        if "/" in txt:
            index = txt.index("/")
            df['currency'] = txt[:index]
            df['quantity'] = txt[index + 1:]
        else:
            df['currency'] = txt
            df['quantity'] = "NULL"

        df['data_date'] = self.get_date(tds)
        return df

    def get_date(self, tds):
        txt = tds[7].text.replace('\\n', '').strip()
        txt = txt + "/" + str(date.today().year)
        dt = datetime.strptime(txt, '%b/%d/%Y')
        return dt

    def trim(self, txt):
        return txt.replace('\\n', '').strip()

    def upsert_data(self, data_arr):
        # commodity_name = df['commodity_name']
        # update_date = df['update_date']
        # update_time = df['update_time']
        # price = df['price']
        # change = df['change']
        # day_percent = df['day_percent']
        # week_percent = df['week_percent']
        # month_percent = df['month_percent']
        # yoy_percent = df['yoy_percent']
        # currency = df['currency']
        # quantity = df['quantity']
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
                data_date
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
                %(data_date)s
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

    def removeNonNumeric(self, strValue):
        strValue = strValue.strip()
        if strValue and strValue.strip():
            non_decimal = re.compile(r'[^\d.-]+')
            strValue = non_decimal.sub('', strValue)
            return strValue
        else:
            ""

    def getFloat(self, strValue):

        if strValue is None:
            return 0
        elif type(strValue) == str:
            strValue = self.removeNonNumeric(strValue)
            if not strValue is None:
                try:
                    if len(strValue) > 0:
                        return float(strValue)
                    return 0
                except Exception as e:
                    return 0
            else:
                return 0
        else:
            return float(strValue)

    def __del__(self):
        self.conn.close()


scrapper = Commodity_Srapper()
