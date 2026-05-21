import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


class WeatherCollector:

    def __init__(self, api_key: str, db_engine, config: dict):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.geo_url = "http://api.openweathermap.org/geo/1.0/direct"
        self.gios_url = "https://api.gios.gov.pl/pjp-api/v1/rest"
        self.engine = db_engine
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.raw_folder_weather = Path(self.config["storage"]["raw_folder_weather"])
        self.raw_folder_weather.mkdir(parents=True, exist_ok=True)
        self.raw_folder_air = Path(self.config["storage"]["raw_folder_air"])
        self.raw_folder_air.mkdir(parents=True, exist_ok=True)
        self.gold_folder = Path(self.config["storage"]["gold_parquet_path"])
        self.gold_folder.mkdir(parents=True, exist_ok=True)
        self.csv_folder = Path(self.config["storage"]["csv_path"])
        self.csv_folder.mkdir(parents=True, exist_ok=True)

    # --- POGODA ---

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

    # --- JAKOŚĆ POWIETRZA (GIOŚ) ---

    def find_nearest_station(self, lat: float, lon: float) -> dict | None:
        response = requests.get(f"{self.gios_url}/station/findAll")

        if response.status_code != 200:
            self.logger.warning(f"GIOŚ API niedostępne: HTTP {response.status_code}")
            return None

        stations = response.json()["Lista stacji pomiarowych"]

        nearest = min(stations, key=lambda s: (
            (float(s["WGS84 φ N"]) - lat) ** 2 + (float(s["WGS84 λ E"]) - lon) ** 2
        ))

        self.logger.info(f"Najbliższa stacja GIOŚ: {nearest['Nazwa stacji']}")
        return nearest

    def get_sensors(self, station_id: int) -> list:
        response = requests.get(f"{self.gios_url}/station/sensors/{station_id}")

        if response.status_code != 200:
            self.logger.warning(f"Stacja {station_id} niedostępna: HTTP {response.status_code}")
            return None

        sensors = response.json()["Lista stanowisk pomiarowych dla podanej stacji"]

        if "error_code" in sensors:
            self.logger.warning(f"Stacja {station_id} niedostępna: {sensors.get('error_reason')}")
            return None

        wanted = {"PM10", "PM2.5"}
        filtered = [s for s in sensors if s["Wskaźnik - kod"] in wanted]

        self.logger.info(f"Znaleziono sensorów: {[s['Wskaźnik - kod'] for s in filtered]}")
        return filtered

    def fetch_air_quality(self, lat: float, lon: float) -> tuple[dict | None, dict | None] | None:
        nearest = self.find_nearest_station(lat, lon)
        if nearest is None:
            return None, None

        sensors = self.get_sensors(nearest["Identyfikator stacji"])
        if not sensors:
            return None, None

        raw = {
            "station": nearest,
            "sensors": [],
        }

        result = {
            "station_name": nearest["Nazwa stacji"],
            "pm10": None,
            "pm25": None,
        }

        for sensor in sensors:
            if sensor["Wskaźnik - kod"] == "PM10" and result["pm10"] is not None:
                continue
            if sensor["Wskaźnik - kod"] == "PM2.5" and result["pm25"] is not None:
                continue

            response = requests.get(f"{self.gios_url}/data/getData/{sensor['Identyfikator stanowiska']}")

            if response.status_code != 200:
                self.logger.warning(f"Sensor {sensor['Identyfikator stanowiska']} niedostępny: HTTP {response.status_code}")
                continue

            data = response.json()

            if "error_code" in data:
                self.logger.warning(f"Sensor {sensor['Identyfikator stanowiska']} niedostępny: {data.get('error_reason')}")
                continue

            # raw
            raw["sensors"].append({
                "sensor_info": sensor,
                "measurements": data.get("Lista danych pomiarowych", []),
            })

            # przetworzone
            values = data.get("Lista danych pomiarowych", [])
            value = next((v["Wartość"] for v in values if v.get("Wartość") is not None), None)

            if sensor["Wskaźnik - kod"] == "PM10":
                result["pm10"] = value
            elif sensor["Wskaźnik - kod"] == "PM2.5":
                result["pm25"] = value

        return raw, result

    # --- PIPELINE ---

    def run_pipeline(self, city: str) -> dict | None:
        lat, lon = self.get_coords(city)

        if lat is None or lon is None:
            self.logger.error(
                f"Nie mogę pobrać pogody, bo nie mam współrzędnych dla miasta: {city}"
            )
            return None

        self.logger.info(f"Współrzędne miasta {city}: {lat}, {lon}")

        weather_data = self.fetch_weather(lat, lon)
        air_raw, air_quality = self.fetch_air_quality(lat, lon)

        now = datetime.now()
        city_slug = city.replace(" ", "_").lower()
        
        raw_path_wea = self.raw_folder_weather / f"{city_slug}_{now.strftime('%Y-%m-%d')}_raw.json"

        with open(raw_path_wea, "w") as f:
            json.dump(weather_data, f, indent=4)

        self.logger.info(f"Surowe dane zapisano do: {raw_path_wea}")

        if air_raw:
            air_raw_path = self.raw_folder_air / f"{city_slug}_{now.strftime('%Y-%m-%d')}_raw.json"

            with open(air_raw_path, "w") as f:
                json.dump(weather_data, f, indent=4)

            self.logger.info(f"Surowe dane air quality zapisano do: {air_raw_path}")

        weather_data_transformed = self.transform_data(weather_data, air_quality, now)

        if self.config["pipeline_settings"]["save_to_sql"]:
            self.load_to_db(weather_data_transformed)

        if self.config["pipeline_settings"]["save_to_parquet"]:
            self.save_to_parquet(weather_data_transformed, city_slug)

        if self.config["pipeline_settings"]["save_to_csv"]:
            self.save_to_csv(weather_data_transformed)

        return weather_data_transformed

    def transform_data(self, raw_data: dict, air_quality: dict | None = None, extracted_at: datetime | None = None) -> dict | None:
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
                "station_name": air_quality.get("station_name") if air_quality else None,
                "pm10": air_quality.get("pm10") if air_quality else None,
                "pm25": air_quality.get("pm25") if air_quality else None,
                "timestamp": datetime.fromtimestamp(raw_data.get("dt")).strftime("%Y-%m-%d %H:%M:%S"),
                "extracted_at": (extracted_at or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
            }

            self.logger.info(f"Udana transformacja, JSON: {transformed}")
            return transformed

        except Exception as e:
            self.logger.error(f"Problem z transformacją: {e}", exc_info=True)
            return None

    # --- ZAPIS ---

    def load_to_db(self, transformed_data: dict) -> None:
        if not transformed_data:
            self.logger.warning("Brak danych dla metody: load_to_db")
            return

        try:
            df = pd.DataFrame([transformed_data])

            df.to_sql(name="weather_gold", con=self.engine, if_exists="append", index=False)

            self.logger.info(f"Dane dla {transformed_data.get('city')} zapisane do bazy.")

        except Exception as e:
            self.logger.error(f"Błąd zapisu do bazy: {e}", exc_info=True)

    def save_to_parquet(self, transformed_data: dict, city_slug: str) -> None:
        if not transformed_data:
            self.logger.warning("Brak danych dla metody: save_to_parquet")
            return

        try:
            now = datetime.now()
            partition_folder = self.gold_folder / f"year={now.year}" / f"month={now.month:02d}"
            partition_folder.mkdir(parents=True, exist_ok=True)

            parquet_path = partition_folder / f"{city_slug}.parquet"
            df_new = pd.DataFrame([transformed_data])

            if parquet_path.exists():
                df_existing = pd.read_parquet(parquet_path)
                df = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df = df_new

            df = df.drop_duplicates(subset=["city", "timestamp"])

            df.to_parquet(parquet_path, index=False)

            self.logger.info(f"Dane dla {transformed_data.get('city')} zapisane do parquet.")

        except Exception as e:
            self.logger.error(f"Błąd zapisu do parquet: {e}", exc_info=True)

    def save_to_csv(self, transformed_data: dict) -> None:
        if not transformed_data:
            self.logger.warning("Brak danych dla metody: save_to_csv")
            return

        try:
            csv_path = self.csv_folder / "result.csv"
            file_exists = csv_path.exists()

            pd.DataFrame([transformed_data]).to_csv(
                csv_path,
                index=False,
                mode="a",
                header=not file_exists,
            )

            self.logger.info(f"Dane dla {transformed_data.get('city')} zapisane do CSV.")

        except Exception as e:
            self.logger.error(f"Błąd zapisu do CSV: {e}", exc_info=True)


def setup_logging(with_file: bool = False) -> None:
    handlers = [logging.StreamHandler()]

    if with_file:
        handlers.append(logging.FileHandler("pipeline.log", encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


if __name__ == "__main__":
    setup_logging(with_file=False)
    startup_logger = logging.getLogger("startup")

    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        startup_logger.error("Nie znaleziono pliku config.yaml")
        exit(1)
    except yaml.YAMLError as e:
        startup_logger.error(f"Nieprawidłowy format config.yaml: {e}")
        exit(1)

    required_keys = ["cities", "api", "storage", "pipeline_settings"]
    for key in required_keys:
        if key not in config:
            startup_logger.error(f"Brakuje sekcji '{key}' w config.yaml")
            exit(1)

    setup_logging(with_file=True)

    engine = create_engine(
        f"sqlite:///{config['storage']['db_path']}", echo=False
    )

    collector = WeatherCollector(
        api_key=os.getenv("MY_API_KEY"),
        db_engine=engine,
        config=config,
    )

    if collector.api_key is None:
        startup_logger.error("Brak klucza API. Ustaw MY_API_KEY w pliku .env")
        exit(1)

    for city in config["cities"]:
        collector.run_pipeline(city)
        time.sleep(1)
