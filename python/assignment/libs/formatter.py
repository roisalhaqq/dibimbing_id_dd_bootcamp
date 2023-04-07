from datetime import datetime

class Formatter:

    @staticmethod
    def cuaca_dir_dest_path_formatter(workdir, propinsi, kota):
        result_path = f"{workdir}/results/{propinsi}/{kota}/"
        return result_path

    @staticmethod
    def cuaca_dest_path_formatter(workdir, propinsi, kota, date):
        result_path = f"{workdir}/results/{propinsi}/{kota}/{date}.json"
        return result_path

    @staticmethod
    def cuaca_result_formatter(jam_cuaca, kode_cuaca, cuaca):
        data = {
            "jam_cuaca": jam_cuaca,
            "kode_cuaca": kode_cuaca,
            "cuaca": cuaca,
            "processed_datetime": datetime.now()
        }

        return data