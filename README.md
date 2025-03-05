Masing-masing proses disimpan dalam 1 file yang berada di folder src dan di jalankan/di eksekusi di main.py


EXTRACTOR :bisa mengakses file csv dan json di folder data
- membuat fungsi yang bisa mengekstrac data json dan csv
- membuat fungsi untuk menggabungkan hasil ekstrasi dari csv dan json lalu menyimpannya ke folder staging

TRANSFORM :
- bisa membaca file yang sudah di ektrac di folder staging
- membuat fungsi untuk menambah kolom
- membuat fungsi untuk normalisasi semua kolom menjadi snake_case dengan menggunakan regex
- ubah_payment_type 1: cash, 2: credit
- membuat fungsi untuk mengubah dari mil ke kilometer
- meyimpan data hasil tranformasi

LOAD :
- mengambil data yang sudah di tranformasi dan menentukan hasilnya disimpan dimana(result)
- membuat fungsi untuk menampilkan info dari dataframe
- bisa menyimpan dalam format csv atau excel
- def proses load mengeksekusi semua fungsi dalam class LOAD

MAIN :
untuk menjalakan semua script dengan cara meng import nya

