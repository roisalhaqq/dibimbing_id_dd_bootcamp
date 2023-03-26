class Warga():
    def __init__(self, NIK: str):
        self.nik = NIK

    def nyoblos(self, pilihan: str):
        self.pilihan = pilihan
        print(f"{self.nik} memilih {pilihan}")
