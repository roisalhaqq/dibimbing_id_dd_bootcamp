import json
import os
import requests
from datetime import datetime
from threading import Thread

# === Load Libs
from config.config_getter import ConfigGetter
from libs.internal_logging import InternalLogging
from libs.api_caller import APICaller
from libs.formatter import Formatter

class bmkgWeatherImporter:
    def __init__(self, mode):
        self.config = ConfigGetter()
        self.api_caller = APICaller()
        self.log = InternalLogging()
        self.mode = mode

    def get_wilayah(self):
        """
            memanggil API https://ibnux.github.io/BMKG-importer/cuaca/wilayah.json
            filter response nya dengan wilayah yang diekspektasikan
            wilayah yang diekspektasikan diambil dari config.yaml
            - DKIJakarta
            - DIYogyakarta
            - JawaBarat
            - Bali
            - KalimantanSelatan
        """

        wilayah_url = f'{self.config.config["bmkg_weather"]["api_base_url"]}/wilayah.json'
        response_wilayah = self.api_caller.api_caller_get(wilayah_url)
        response_wilayah_json = json.loads(response_wilayah.text)

        daerah = self.config.get_bmkg_weather_propinsi()
        filtered_wilayah_json = list(filter(lambda x: x['propinsi'] in daerah,  response_wilayah_json))
        self.log.logging_info(message=f"filtered wilayah json total data: {len(filtered_wilayah_json)}", mode=self.mode)

        return filtered_wilayah_json

    def get_detail_cuaca(self, wilayah_id: str):
        """
            memanggil API https://ibnux.github.io/BMKG-importer/cuaca/{{ wilayah }}.json
            tranform data menjadi format yang ada dalam library formatter (cuaca_result_formatter)
            param: wilayah_id (diextract dari method get_wilayah)
            return: 
            - unique date cuaca dari API
            - format dari hasil yang diharapkan
            {
                "jam_cuaca": ,
                "kode_cuaca": ,
                "cuaca": ,
                "processed_datetime": datetime.now()
            }
        """

        cuaca_detail_url = f'{self.config.config["bmkg_weather"]["api_base_url"]}/{wilayah_id}.json'
        response_cuaca = self.api_caller.api_caller_get(cuaca_detail_url)
        response_cuaca_json = json.loads(response_cuaca.text)
        self.log.logging_info(message=f"total data of raw detail cuaca for wilayah {wilayah_id}: {len(response_cuaca_json)}", mode=self.mode)

        result_detail_cuaca = {}
        unique_date_cuaca = []
        for i in response_cuaca_json:
            date_cuaca = datetime.strptime(i["jamCuaca"], "%Y-%m-%d %H:%M:%S").date()
            date_cuaca_str = date_cuaca.strftime('%Y-%m-%d')
            if date_cuaca_str in unique_date_cuaca:
                formated_detail_cuaca = Formatter.cuaca_result_formatter(i["jamCuaca"], i["kodeCuaca"], i["cuaca"])
                result_detail_cuaca[date_cuaca_str].append(formated_detail_cuaca)
            else:
                result_detail_cuaca[date_cuaca_str] = []
                formated_detail_cuaca = Formatter.cuaca_result_formatter(i["jamCuaca"], i["kodeCuaca"], i["cuaca"])
                result_detail_cuaca[date_cuaca_str].append(formated_detail_cuaca)

                unique_date_cuaca.append(date_cuaca_str)

        return unique_date_cuaca, result_detail_cuaca

    def transformer_and_loader(self, wilayah_id, propinsi, kota):
        """
            transform and load data wilayah untuk setiap tanggal
        """

        self.log.logging_info(message=f"Begin transform and load for {wilayah_id}", mode=self.mode)

        date_cuaca, detail_cuaca = self.get_detail_cuaca(wilayah_id)
        for i in range(len(date_cuaca)):

            self.load_cuaca_to_json(propinsi, kota, date_cuaca[i], detail_cuaca[date_cuaca[i]])
        
        self.log.logging_info(message=f"Finish transform and load for {wilayah_id}", mode=self.mode)

    def path_creator(func):
        """
            path_creator adalah decorator method https://www.geeksforgeeks.org/decorators-in-python/
            sehingga untuk setiap function yang mengaplikasikan decorator method ini
            maka akan menjalankan perintah yang ada didalam function ini terlebih dahulu sebelum mengeksekusi functionnya
            
            args merupakan inputan function nya
            misal method load_cuaca_to_json(self, propinsi, kota, date, data)
            maka args[0] => self, args[1] => propinsi, args[2] => kota, dst
        """

        def wrapper(*args, **kwargs):
            work_dir = args[0].config.workdir
            propinsi = args[1]
            kota = args[2]
            path = Formatter.cuaca_dir_dest_path_formatter(work_dir, propinsi, kota)
            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                    args[0].log.logging_info(message=f"Directory '{path}' created successfully", mode=args[0].mode)
                except OSError as error:
                    args[0].log.logging_error(message=f"Directory '{path}' can not be created", mode=args[0].mode, exception=error)
                    raise
            func(*args)
        return wrapper

    @path_creator
    def load_cuaca_to_json(self, propinsi, kota, date, data):
        """
            function ini menggunakan decorator method path_creator diatas, akan mengeksekusi command sebelum dan sesudah func()
        """

        path = Formatter.cuaca_dest_path_formatter(self.config.workdir, propinsi, kota, date)
        with open(path, 'w') as f:
            json.dump(data, f, default=str)
        self.log.logging_info(message=f"Success load data detail cuaca to '{path}'", mode=self.mode)

    def run(self):
        """
            method untuk mengeksekusi keseluruhan
            menerapkan function Thread untuk menjalankan function transformer_and_loader secara simultan untuk setiap wilayah
        """

        self.log.logging_info(message=f"Begin BMKG Importer", mode=self.mode)
        wilayah = self.get_wilayah()

        threads = []

        for i in wilayah:
            wilayah_id = i["id"]
            propinsi = i["propinsi"]
            kota = i["kota"]

            t = Thread(target=self.transformer_and_loader, args=(wilayah_id, propinsi, kota,))
            threads.append(t)
        
        for x in threads:
            x.start()

        for x in threads:
            x.join()

        self.log.logging_info(message=f"BMKG Importer is finished", mode=self.mode)

class updateLoadBmkgWeatherImporter(bmkgWeatherImporter):
    """
        class yang mewarisi class bmkgWeatherImporter
        dengan melakukan modifikasi method get_detail_cuaca dan transformer_and_loader (polymorphism)
    """

    def get_detail_cuaca(self, wilayah_id):
        """
            memanggil API https://ibnux.github.io/BMKG-importer/cuaca/{{ wilayah }}.json
            tranform data menjadi format yang ada dalam library formatter (cuaca_result_formatter)
            param: wilayah_id (diextract dari method get_wilayah)
            return: 
            - unique date cuaca dari API
            - format dari hasil yang diharapkan
            {
                "jam_cuaca": ,
                "kode_cuaca": ,
                "cuaca": ,
                "processed_datetime": datetime.now()
            }
            => data yang diambil hanya latest / data tanggal terakhir dari response API
        """

        cuaca_detail_url = f'{self.config.config["bmkg_weather"]["api_base_url"]}/{wilayah_id}.json'
        response_cuaca = self.api_caller.api_caller_get(cuaca_detail_url)
        response_cuaca_json = json.loads(response_cuaca.text)
        self.log.logging_info(message=f"total data of raw detail cuaca for wilayah {wilayah_id}: {len(response_cuaca_json)}", mode=self.mode)

        reversed_response_cuaca_json = response_cuaca_json[::-1]
        result_detail_cuaca = []
        last_date_cuaca = ""
        for i in reversed_response_cuaca_json:
            date_cuaca = datetime.strptime(i["jamCuaca"], "%Y-%m-%d %H:%M:%S").date()
            date_cuaca_str = date_cuaca.strftime('%Y-%m-%d')
            if date_cuaca_str == last_date_cuaca:
                formated_detail_cuaca = Formatter.cuaca_result_formatter(i["jamCuaca"], i["kodeCuaca"], i["cuaca"])
                result_detail_cuaca.append(formated_detail_cuaca)
            elif date_cuaca_str != last_date_cuaca and last_date_cuaca == "":
                formated_detail_cuaca = Formatter.cuaca_result_formatter(i["jamCuaca"], i["kodeCuaca"], i["cuaca"])
                result_detail_cuaca.append(formated_detail_cuaca)
            else:
                break
            last_date_cuaca = i["jamCuaca"][:10]

        self.log.logging_info(message=f"last date of new update wilayah {wilayah_id}: {last_date_cuaca} with {len(result_detail_cuaca)} total new update", mode=self.mode)

        return last_date_cuaca, result_detail_cuaca

    def transformer_and_loader(self, wilayah_id, propinsi, kota):
        """
            transform dan load hanya untuk satu tanggal saja
        """

        self.log.logging_info(message=f"Begin transform and load {wilayah_id}", mode=self.mode)

        date_cuaca, detail_cuaca = self.get_detail_cuaca(wilayah_id)
        
        self.load_cuaca_to_json(propinsi, kota, date_cuaca, detail_cuaca)

        self.log.logging_info(message=f"Finish transform and load {wilayah_id}", mode=self.mode)