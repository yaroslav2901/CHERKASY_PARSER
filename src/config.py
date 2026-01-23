# src/config.py
from zoneinfo import ZoneInfo
import os
from pathlib import Path

# ----------------- Налаштування -----------------
TIMEZONE = ZoneInfo("Europe/Kyiv")
REGION = "Cherkasyoblenergo"   # <<<<<<<<<<<<<<<<<< ОБЛЕНЕРГО
BASE_DIR = Path(__file__).parent.parent.absolute()

#SOURCE_JSON = os.path.join(BASE_DIR, "out", f"{REGION}.json")
SOURCE_JSON = os.path.join(BASE_DIR, "out", "Cherkasyoblenergo.json")
SOURCE_IMAGES = os.path.join(BASE_DIR, "out/images")

# ----------------- ПРАВИЛЬНИЙ REPO -----------------
REPO_DIR = "/home/yaroslav/bots/OE_OUTAGE_DATA"

DATA_DIR = os.path.join(REPO_DIR, "data") # папка для json файлів
IMAGES_DIR = os.path.join(REPO_DIR, f"images/{REGION}") # папка для зображень цього регіону


LOG_FILE = os.path.join(BASE_DIR, "logs", "full_log.log")

