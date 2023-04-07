import os
# === Load Repo
from usecase.bmkg_weather_importer import bmkgWeatherImporter, updateLoadBmkgWeatherImporter

mode_builder = os.environ['BUILDERAPP'] # diambil dari run_app.sh

if mode_builder == "normal":
    builder = bmkgWeatherImporter("normal") # dijalankan ketika 'bash ./run_app.sh normal'
elif mode_builder == "update":
    builder = updateLoadBmkgWeatherImporter("update") # dijalankan ketika 'bash ./run_app.sh update'

builder.run()