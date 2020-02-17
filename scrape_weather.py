{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import datetime\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests as req\n",
    "import pandas as pd\n",
    "from pymongo import MongoClient"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "URL = 'https://www.metaweather.com/api/location/2391279/'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_weather_data(url=URL):\n",
    "    \n",
    "    response = req.get(url)\n",
    "    json_data = response.json()\n",
    "    days = json_data['consolidated_weather']\n",
    "    \n",
    "    df = pd.io.json.json_normalize(days[0])\n",
    "    for day in days[1:]:\n",
    "        df = df.append(pd.io.json.json_normalize(day))\n",
    "        \n",
    "    return df    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [],
   "source": [
    "def clean_df(df):\n",
    "    df['created'] = pd.to_datetime(df['created'], utc=True)\n",
    "    df['applicable_date'] = pd.to_datetime(df['applicable_date']).dt.tz_localize('US/Mountain')\n",
    "    \n",
    "    df.drop(['weather_state_abbr', 'id'], inplace=True, axis=1)\n",
    "    return df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "def store_data(df):\n",
    "    \n",
    "    client = MongoClient()\n",
    "    db = client['weather_test']\n",
    "    collection = db['denver']\n",
    "    collection.insert_many(df.to_dict('records'))\n",
    "    client.close()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [],
   "source": [
    "def daily_scrape():\n",
    "    scraped_today = False\n",
    "    while True:\n",
    "        utc_time = datetime.datetime.utcnow()\n",
    "        if not scraped_today and utc_time.hour == 0 and utc_time.second == 0:\n",
    "            df = get_weather_data()\n",
    "            df = clean_df(df)\n",
    "            store_data(df)\n",
    "            scraped_today = True\n",
    "            time.sleep(1)\n",
    "            \n",
    "    time.sleep(1)\n",
    "    \n",
    "\n",
    "    df = get_weather_data()\n",
    "    df = clean_df(df)\n",
    "    store_data(df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.5.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
