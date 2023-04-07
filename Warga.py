class Warga():
    def __init__(self, NIK: str):
        self.nik = NIK

    def nyoblos(self, pilihan: str):
        self.pilihan = pilihan
        print(f"{self.nik} memilih {pilihan}")

    def ambil_pinjol(self, jumlah):
        print(f"{self.nik} ambil pinjol sebanyak {jumlah}")

    def beli_takjil(self, takjil):
        print(f"beli {takjil}")

    auth = "aqweqweqw"

    