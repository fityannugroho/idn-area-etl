# 📄 idn-area Extractor

Ekstraksi otomatis kolom `kode` dan `nama wilayah` dari file PDF dengan tabel bergaris (wired table) menggunakan Camelot.

## Instalasi

1. Clone repositori ini
2. Masuk ke direktori proyek
3. Aktifkan virtual environment:
   ```bash
   source .venv/bin/activate
   ```
4. Instal dependensi:
   ```bash
   pip install .
   ```

> **CATATAN:**
> Untuk keluar dari virtual environment, gunakan perintah `deactivate`

## Penggunaan

Untuk mengekstrak data dari file PDF, gunakan perintah berikut:

```bash
idn-area-extractor <path_to_pdf>
```
