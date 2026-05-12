import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import yaml
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
    force=True,
)


class WeatherCollector:

    def __init__(self, api_key: str, db_engine, config: dict):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.geo_url = "http://api.openweathermap.org/geo/1.0/direct"
        self.engine = db_engine
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.raw_folder = Path(self.config["storage"]["raw_folder"])
        self.raw_folder.mkdir(parents=True, exist_ok=True)

    def get_coords(self, city_name: str) -> tuple[float | None, float | None]:
        params = {
            "q": city_name,
            "limit": self.config["api"]["limit"],
            "appid": self.api_key,
        }

        try:
            response = requests.get(self.geo_url, params=params)
            response.raise_for_status()

            data = response.json()

            if not data:
                self.logger.warning(f"Nie znaleziono współrzędnych dla: {city_name}")
                return None, None

            self.logger.info("API geo odpowiada")
            return data[0]["lat"], data[0]["lon"]

        except Exception as e:
            self.logger.error(f"Wystąpił problem: {e}")
            return None, None

    def fetch_weather(self, lat: float, lon: float) -> dict | None:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": self.config["api"]["units"],
        }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()

            data = response.json()

            if not data:
                self.logger.warning(
                    f"Nie znaleziono informacji o pogodzie dla współrzędnych: {lat}, {lon}"
                )
                return None

            self.logger.info("API weather odpowiada")
            return data

        except Exception as e:
            self.logger.error(f"Problem z API: {e}", exc_info=True)
            return None

    def run_pipeline(self, city: str) -> dict | None:
        lat, lon = self.get_coords(city)

        if lat is None or lon is None:
            self.logger.error(
                f"Nie mogę pobrać pogody, bo nie mam współrzędnych dla miasta: {city}"
            )
            return None

        self.logger.info(f"Współrzędne miasta {city}: {lat}, {lon}")

        weather_data = self.fetch_weather(lat, lon)

        now = datetime.now()
        raw_path = self.raw_folder / f"{city}_{now.strftime('%Y-%m-%d')}_raw.json"

        with open(raw_path, "w") as f:
            json.dump(weather_data, f, indent=4)

        self.logger.info(f"Surowe dane zapisano do: {raw_path}")

        weather_data_transformed = self.transform_data(weather_data)

        self.load_to_db(weather_data_transformed)

        return weather_data_transformed

    def transform_data(self, raw_data: dict) -> dict | None:
        if not raw_data:
            self.logger.error("Brak danych do transformacji")
            return None

        try:
            temp_c = raw_data.get("main", {}).get("temp")
            temp_f = (temp_c * 1.8 + 32) if temp_c is not None else None

            transformed = {
                "city": raw_data.get("name"),
                "lon": raw_data.get("coord", {}).get("lon"),
                "lat": raw_data.get("coord", {}).get("lat"),
                "weather": raw_data.get("weather", [{}])[0].get("main"),
                "conditions": raw_data.get("weather", [{}])[0].get("description"),
                "temp_c": temp_c,
                "temp_f": temp_f,
                "timestamp": datetime.fromtimestamp(raw_data.get("dt")).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "extracted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            self.logger.info(f"Udana transformacja, JSON: {transformed}")
            return transformed

        except Exception as e:
            self.logger.error(f"Problem z transformacją: {e}", exc_info=True)
            return None

    def load_to_db(self, transformed_data: dict) -> None:
        if not transformed_data:
            return

        try:
            df = pd.DataFrame([transformed_data])

            df.to_sql(name="weather_gold", con=self.engine, if_exists="append", index=False)

            self.logger.info(f"Dane dla {transformed_data.get('city')} zapisane do bazy.")

        except Exception as e:
            self.logger.error(f"Błąd zapisu do bazy: {e}", exc_info=True)


if __name__ == "__main__":
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    engine = create_engine(
        f"sqlite:///{config['database']['path']}", echo=False
    )

    collector = WeatherCollector(
        api_key=os.getenv("MY_API_KEY"),
        db_engine=engine,
        config=config,
    )

    for city in config["cities"]:
        collector.run_pipeline(city)
        time.sleep(1)