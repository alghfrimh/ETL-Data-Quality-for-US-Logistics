'''
============================================================================================================
Milestone 3

Nama  : Muhammad Al Ghifari
Batch : FTDS-030-HCK

Objective

Program ini mengotomatisasi proses ETL dari PostgreSQL ke Elasticsearch menggunakan pipeline Apache Airflow.
Dataset yang digunakan berisi sekitar 2.000 catatan pengiriman logistik di AS.
============================================================================================================
'''

# Import Libraries
import pandas as pd
import pendulum
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from elasticsearch import Elasticsearch


default_args= {
    'owner': 'Muhammad Al Ghifari',
    'start_date': pendulum.datetime(2024, 11, 1, tz='Asia/Jakarta')} # Tanggal dimulai dari 1 November 2024
'''
Fungsi ditujukan untuk meyusun parameter default untuk DAG Airflow

Parameter
---------
- owner : str --> Nama pemilik DAG.
- start_date : pendulum.DateTime --> Waktu mulai scheduling DAG (timezone-aware, Asia/Jakarta).
'''

# Membuat DAG
with DAG(
    'ETL_P2M3_muhammad_al_ghifari',
    description='Extract from PostgreSQL -> Preprocessing -> Load to Elasticsearch (updates Kibana dashboard)',
    schedule_interval='10,20,30 9 * * SAT', # Scheduling setiap hari Sabtu (dimulai dari tanggal 2 November 2024), jam 09:10 sampai 09:30
    default_args=default_args, 
    catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    '''
    Fungsi ini ditujukan untuk mendefinisikan satu workflow (DAG) bernama `ETL_P2M3_muhammad_al_ghifari` untuk menjalankan pipeline ETL secara terjadwal.

    Parameter
    ---------
    - `dag_id` : `'ETL_P2M3_muhammad_al_ghifari'`
    - `schedule_interval` : `'10,20,30 9 * * SAT'` --> jalan pada menit 10/20/30, jam 09:00, setiap hari Sabtu.
    - `default_args.start_date` : `2024-11-01 Asia/Jakarta` --> titik awal penjadwalan DAG.
    - `catchup` : `False` --> menjalankan hanya jadwal ke depan, tanpa backfill.
    - `start` --> Node dummy sebagai penanda awal alur DAG.
    - `end` --> Node dummy sebagai penanda akhir alur DAG .
    
    Contoh Penggunaan
    -----------------
    (chaining)
    start >> extract() >> preprocess_data() >> load() >> end
    '''

    # Proses ekstraksi dari database PostgreSQL
    @task()
    def extract():
        database = "airflow"
        username = "airflow"
        password = "airflow"
        host = "postgres"

        postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

        engine = create_engine(postgres_url)
        conn = engine.connect()

        # Ekstraksi dataset menggunakan query, lalu menyimpan hasilnya dalam bentuk CSV
        df = pd.read_sql('SELECT * FROM table_m3', conn)
        df.to_csv('/opt/airflow/data/P2M3_muhammad_al_ghifari_data_raw.csv', index=False)

        print("Success INSERT")
    '''
    Fungsi merupakan proses ekstraksi data raw dari database PostgreSQL dan menyimpannya ke file CSV 
    sebagai input tahap preprocessing (Data Cleaning).

    Langkah kerja
    -------------
    1) Menyusun URL koneksi PostgreSQL dengan kredensial:
        - host=`postgres`
        - database=`airflow`
        - username=`airflow`
        - password=`airflow`.
    2) Membuat engine dan membuka koneksi.
    3) Menjalankan query: 'SELECT * FROM table_m3' (Pengambilan data dari database PostgreSQL).
    4) Menyimpan hasilnya ke CSV: /opt/airflow/data/P2M3_muhammad_al_ghifari_data_raw.csv.
    5) Menampilkan output keberhasilan.
    '''

    # Proses data cleaning (preprocessing)
    @task()
    def preprocess_data():
        # Load data raw untuk dilakukan data cleaning
        df = pd.read_csv('/opt/airflow/data/P2M3_muhammad_al_ghifari_data_raw.csv')

        # Normalisasi nama kolom
        df.columns = (df.columns
              .str.strip()                              # Menghapus spasi/tab di awal dan akhir
              .str.lower()                              # Mengubah semua huruf menjadi lowercase
              .str.replace(r'\s+', '_', regex=True)     # Mengganti spasi/tab antar kata (1+ whitespace) menjadi 1 underscore
              .str.replace(r'[^\w]', '', regex=True))   # Menghapus simbol-simbol
        '''
        Fungsi ini merapikan nama kolom supaya seragam dan aman dipakai di step berikutnya: menghapus whitespaces di awal dan akhir kata, mengubah semua huruf
        menjadi lowercase, mengganti spasi/tab di tengah menjadi underscore (snake_case), lalu membuang simbol yang tidak perlu. Tujuannya agar nama kolom konsisten,
        gampang dipanggil di kode/query, dan mengurangi error saat join/merge, ekspor ke SQL/Elasticsearch, atau bikin visualisasi.
        '''

        # Normalisasi values: menghapus whitespaces dan standarisasi missing
        obj = df.select_dtypes('object').columns
        df[obj] = df[obj].apply(lambda s: s.str.strip())
        df.replace(r'(?i)^(na|n/a|null|none|nan|unknown|\-|–)$',
           pd.NA, regex=True, inplace=True)
        '''
        Fungsi ini untuk merapikan nilai teks: spasi/tab di awal–akhir sel, lalu mengubah non-standar missing values (na, n/a, null, none, nan, unknown, -)
        menjadi nilai standar (NaN). Tujuannya agar missing values non-standar tidak dianggap valid, sehingga dapat dilakukannya konversi tipe data dan 
        analisis selanjutnya lebih konsisten.
        '''
        
        # Mengganti tipe data pada kolom 'shipment_date' dan 'delivery_date' menjadi datetime
        df['shipment_date'] = pd.to_datetime(df['shipment_date'], format='%Y-%m-%d', errors='coerce')
        df['delivery_date'] = pd.to_datetime(df['delivery_date'], format='%Y-%m-%d', errors='coerce')
        '''
        Fungsi ini mengonversi kolom 'shipment_date' dan 'delivery_date' ke tipe datetime. Selain itu, parameter errors='coerce' membuat nilai yang kosong/formatnya
        salah otomatis menjadi NaT (bukan error). Tujuannya agar tanggal konsisten dan siap dipakai untuk operasi waktu—misalnya hitung selisih hari, filter rentang tanggal, 
        sort, dan validasi—tanpa crash karena format yang tidak valid.
        '''

        # Handling nilai negatif pada kolom numerik
        for c in ['weight_kg', 'distance_miles', 'transit_days', 'cost']:
            df.loc[df[c].lt(0, fill_value=False), c] = pd.NA
        '''
        Fungsi ini ditujukan untuk mengganti nilai negatif pada kolom numerik (weight_kg, distance_miles, transit_days, cost) menjadi NaN. Alasannya karena nilai negatif
        pada kolom tersebut dianggap sebagai nilai yang tidak valid dalam konteks logistik, sehingga pendekatan tersebut saya lakukan sebagai missing values untuk menjaga
        konsistensi analisis.
        '''

        # Handling missing values
        # 1. Imputasi kolom 'delivery_date'
        df['delivery_date'] = df['delivery_date'].fillna(
        df['shipment_date'] + pd.to_timedelta(df['transit_days'], unit='D'))

        # 2. Imputasi kolom 'cost'
        denom = df['weight_kg'] * df['distance_miles']
        valid_rate = df['cost'].notna() & denom.gt(0)

        df.loc[valid_rate, 'rate'] = df.loc[valid_rate, 'cost'] / denom[valid_rate]

        avg_rate = df.groupby('carrier')['rate'].median()

        for carrier, rate in avg_rate.items():
            mask = (df['carrier'] == carrier) & (df['cost'].isna()) & denom.gt(0)
            df.loc[mask, 'cost'] = df.loc[mask, 'weight_kg'] * df.loc[mask, 'distance_miles'] * rate

        df.drop(columns='rate', inplace=True)
        '''
        Handling missing values yang saya lakukan menggunakan imputasi dinamis yang menyesuaikan dengan konteks bisnis logistik, bukan sekadar mengisi dengan nilai rata-rata.
        Dengan cara ini, imputasi tetap relevan dengan pola nyata di lapangan dan hasil analisis menjadi lebih efisien dan akurat.

        1) Imputasi 'delivery_date'
        Untuk kolom 'delivery_date', saya isi dengan `shipment_date + transit_days`. Pendekatan ini saya lakukan karena berdasarkan konteks pengiriman logistik, 
        lama transit memang menjadi acuan standar (SLA) estimasi waktu barang tiba. Dengan begitu, data tetap bisa dianalisis untuk ketepatan pengiriman (OTD) 
        meskipun ada nilai yang hilang.
        
        2) Imputasi 'cost'
        Untuk kolom 'cost', saya menghitung terlebih dahulu tarif dasar per carrier (`rate = cost / (weight_kg * distance_miles)`) lalu mengambil median per carrier.
        Pendekatan ini saya lakukan karena dalam praktik logistik, biaya pengiriman sangat dipengaruhi oleh berat dan jarak, namun tiap carrier memiliki kebijakan
        tarif yang berbeda. Dengan pendekatan ini, baris yang cost-nya hilang bisa diisi berdasarkan pola tarif carrier masing-masing, sehingga hasil estimasi lebih
        realistis dan konsisten dibanding hanya pakai median global.
        '''
       
        # Menghapus data yang duplikat
        # 1. Menghapus duplikasi data secara keseluruhan
        df = df.drop_duplicates()

        # 2. Kalau 'shipment_id' tidak unik, kolaps dengan fungsi pertama
        if 'shipment_id' in df.columns and not df['shipment_id'].is_unique:
            df = (df.sort_values(
            by=['shipment_id', df['delivery_date'].notna(), 'delivery_date', 'shipment_date'],
            ascending=[True, False, False, False]) # pilih yang ada delivery_date & paling baru
            .drop_duplicates(subset='shipment_id', keep='first'))
        '''
        Fungsi ini untuk menghapus duplikasi data: pertama membuang baris yang identik, lalu jika terdapat 'shipment_id' muncul lebih dari satu, memilih baris paling informatif
        (ada 'delivery_date'; kalau sama-sama ada, ambil yang terbaru). Pendekatan ini saya lakukan agar metrik dan visualisasi (OTD, delay, cost) tidak double-count, sehingga
        tetap menjaga catatan pengiriman yang sah.
        '''

        # Menyimpan hasil preprocessing dalam bentuk CSV
        print("Preprocessed data is Success")
        print(df.head())
        df.to_csv('/opt/airflow/data/P2M3_muhammad_al_ghifari_data_cleaned.csv', index=False)

    # Proses data loading di Elasticsearch
    @task()
    def load():

        es = Elasticsearch("http://elasticsearch:9200")
        print(es.ping())
        # Read CSV file
        df = pd.read_csv('/opt/airflow/data/P2M3_muhammad_al_ghifari_data_cleaned.csv', 
                         parse_dates=['shipment_date','delivery_date'])

        # Load data ke ElasticSearch
        for i, row in df.iterrows():
            if pd.isna(row['shipment_date']):
                continue
            doc_id = f"{row['shipment_id']}_{row['shipment_date'].strftime('%Y%m%d')}"

            res=es.index(
                index="logistics_project",
                id=doc_id,
                doc_type="doc",
                body=row.to_dict())
    '''
    Fungsi ini bertujuan untuk melakukan tahap "Load" dalam pipeline ETL, yaitu mengirimkan hasil preprocessing yang sudah disimpan ke CSV (data cleaned) ke dalam Elasticsearch.
    Data ini nantinya digunakan untuk update dashboard Kibana.

    Langkah kerja
    -------------
    1) Membuka koneksi ke Elasticsearch melalui endpoint `http://elasticsearch:9200` dan memastikan koneksi aktif (`es.ping()`).
    2) Membaca file CSV hasil preprocessing (`P2M3_muhammad_al_ghifari_data_cleaned.csv`) dengan parsing kolom tanggal (`shipment_date` dan `delivery_date`) agar langsung bertipe datetime.
    3) Melakukan iterasi per barisnya:
        - Jika `shipment_date` kosong (NaT), baris dilewati agar tidak membuat dokumen invalid.
        - Membuat dokumen unik `doc_id` berbentuk `{shipment_id}_{shipment_date}`, sehingga setiap kombinasi shipment dan tanggal hanya tersimpan satu kali di Elasticsearch (mencegah duplikasi).
        - Mengirim data baris (`row.to_dict()`) ke indeks Elasticsearch bernama `logistics_project`.
    '''

    # Pipeline Apache Airflow
    start >> extract() >> preprocess_data() >> load() >> end