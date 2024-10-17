'''
========================================================================================================

Program ini dibuat untuk membangun sistem automasi yang akan dijalankan didalam airflow.

========================================================================================================
'''


# import library
from airflow.models import DAG
import numpy as np
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine 
import pandas as pd
from elasticsearch import Elasticsearch, helpers

#fungsi load csv ke postgres
def load_csv_to_postgres():
    '''
    Fungsi ini dibuat untuk memuat file csv ke dalam postgres
    '''
    # mendefinisikan informasi untuk keperluan koneksi ke postgre
    database = "ml3"
    username = "postgres"
    password = "postgres"
    host = "postgres"

    # membuat URL koneksi ke postgre
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # membuat engine dan koneksi ke postgre
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # membaca file csv dan menyimpannya ke postgre dengan nama tabel: table_m3
    df = pd.read_csv('/opt/airflow/dags/P2M3_ciputra_wangsa_data_raw.csv')
    df.to_sql('table_m3', conn, index=False, if_exists='replace')  
    

def ambil_data():
    '''
    Fungsi ini dibuat untuk mengambil data dari postgre dan menyimpannya kedalam csv
    '''
    # mendefinisikan informasi untuk keperluan koneksi ke postgre
    database = "ml3"
    username = "postgres"
    password = "postgres"
    host = "postgres"

    # membuat URL koneksi ke postgre
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # membuat engine dan koneksi ke postgre
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # mengambil data dari tabel 'table_m3' dan menyimpannya sebagai file CSV
    df = pd.read_sql_query("select * from table_m3", conn) 
    df.to_csv('/opt/airflow/dags/data_from_postgre.csv', sep=',', index=False)

def preprocessing():
    '''
    Fungsi ini dibuat untuk melakukan processing data yang sudah diambil dan disimpan dari postgre
    '''
    # membaca file CSV yang telah diambil dari PostgreSQL
    df = pd.read_csv('/opt/airflow/dags/data_from_postgre.csv')

    # mengubah nama kolom menjadi huruf kecil dan mengganti spasi dengan underscore
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # menghapus data duplikat dan menyimpan data pertama saja
    df = df.drop_duplicates(keep='first')

    # menhapus baris yang ada missing value
    df = df.dropna()

    # mengubah beberapa kolom menjadi tipe data integer menggunakan looping
    cols = ['transaction_id', 'ratings', 'customer_id', 'year', 'age']
    for col in cols:
        df[col] = df[col].astype(int)

    # mengubah format kolom 'time' menjadi format jam:menit saja
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.strftime('%H:%M')

    '''
    Karena tidak ada kolom unik bahkan dari customer_id dari dataset, sehingga dilakukan penggabungan beberapa
    identias dari customer agar menjadi id unique. Unique id yang dibuat:
    - 3 karakter pertama nama customer
    - ID customer
    - Huruf pertama dari gender customer (M/F)
    - 3 karakter pertama dari country customer
    '''
    # membuat kolom 'unique_id' berdasarkan kombinasi beberapa kolom
    df['unique_id'] = (df['name'].str[:3].str.upper() + df['customer_id'].astype(str) +
                    df['gender'].str[0] + df.groupby('customer_id').cumcount().astype(str) +
                    df['country'].str[:2].str.upper())

    # mendefinisikan format tanggal yang mungkin ada di kolom 'date'
    date_formats = ['%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%m-%d-%Y']

    # karena format tanggal yang ada di kolom date sangat beragam, maka dibuat function untuk menyesuaikan format tanggal
    def parse_date(date_str):
        '''
        Fungsi ini dibuat untuk menyeragamkan format tanggal yang ada pada dataset agar nanti bisa diolah
        '''
        # handling missing value pada kolom date
        if pd.isna(date_str):
            return pd.NaT
        for fmt in date_formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue
        print(f"Failed to parse date: {date_str}") # print pesan jika gagal mem-parsing tanggal
        return pd.NaT  # mengembalikan Not a Time jika semua format gagal

    # memeriksa nilai tanggal asli sebelum pemrosesan
    print("Original Date Values Before Parsing:")
    print(df['date'].unique())  # print nilai tanggal unik sebelum diproses

    # Apply the function to the date column
    df['date'] = df['date'].apply(parse_date)

    # menerapkan fungsi untuk kolom tanggal
    print("Parsed Date Values:")
    print(df['date'].unique())  # print nilai tanggal unik setelah diproses

    # mengubah format kolom 'date' menjadi format yyyy-MM-dd sesuai default elasticsearch
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # menyimpan data yang telah diproses ke file CSV
    df.to_csv('/opt/airflow/dags/P2M3_ciputra_wangsa_data_clean.csv', index=False)
    
def upload_to_elasticsearch():
    '''
    Fungsi ini digunakan untuk melakukan load/indexing data csv yang sudah bersih ke elasticsearch
    '''
    # membuat koneksi ke Elasticsearch
    es = Elasticsearch("http://elasticsearch:9200")

    # membaca file CSV yang telah diproses
    df = pd.read_csv('/opt/airflow/dags/P2M3_ciputra_wangsa_data_clean.csv')

    # mendefinisikan mapping untuk indeks Elasticsearch
    mapping = {
        "mappings": {
            "properties": {
                "customer_id": {"type": "long"},
                "name": {"type": "keyword"},
                "email": {"type": "keyword"},
                "phone": {"type": "keyword"},
                "address": {"type": "text"},
                "city": {"type": "keyword"},
                "state": {"type": "keyword"},
                "zipcode": {"type": "keyword"},
                "country": {"type": "keyword"},
                "age": {"type": "long"},
                "gender": {"type": "keyword"},
                "income": {"type": "keyword"},
                "customer_segment": {"type": "keyword"},
                "date": {"type": "date", "format": "yyyy-MM-dd"},
                "year": {"type": "long"},
                "month": {"type": "keyword"},
                "time": {"type": "keyword"},
                "total_purchases": {"type": "float"},
                "amount": {"type": "float"},
                "total_amount": {"type": "float"},
                "product_category": {"type": "keyword"},
                "product_brand": {"type": "keyword"},
                "product_type": {"type": "keyword"},
                "feedback": {"type": "text"},
                "shipping_method": {"type": "keyword"},
                "payment_method": {"type": "keyword"},
                "order_status": {"type": "keyword"},
                "ratings": {"type": "long"},
                "products": {"type": "keyword"},
                "unique_id": {"type": "keyword"}
            }
        }
    }
    
    # menghapus indeks jika sudah ada
    if es.indices.exists(index="table_mile3"):
        es.indices.delete(index="table_mile3")
    
    # membuat indeks dengan mapping yang telah didefinisikan
    es.indices.create(index="table_mile3", body=mapping)
    
    # menyiapkan dokumen untuk melakukan pengindeksan
    actions = []
    for i, row in df.iterrows():
        doc = row.to_dict()
        # mengonversi tipe numerik dari numpy/pandas ke tipe native Python
        for key, value in doc.items():
            if pd.isna(value):
                doc[key] = None
            elif isinstance(value, (pd.Int64Dtype, pd.Float64Dtype, np.int64, np.float64)):
                doc[key] = float(value) if isinstance(value, (np.float64, pd.Float64Dtype)) else int(value)
        
        action = {
            "_index": "table_mile3",
            "_id": str(i),
            "_source": doc
        }
        actions.append(action)
    
    # melakukan penguploadan ke elasticsearch
    try:
        success, failed = helpers.bulk(es, actions, raise_on_error=False)
        print(f"Successfully indexed {success} documents. Failed: {failed}")
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
        
# mendefinisikan argumen default untuk DAG   
default_args = {
    'owner': 'ciputra wangsa', 
    'start_date': datetime(2024, 10, 11, 13, 30)
}

# mendefinisikan DAG baru dengan nama "postgre_clean_elk"
with DAG(
    "postgre_clean_elk", 
    description='Milestone_3',
    schedule_interval='30 23 * * *', 
    default_args=default_args, 
    catchup=False
) as dag:
    
    # mendefinisikan task untuk load data CSV ke PostgreSQL (load_csv_to_postgres)
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) 
    
    # mendefinisikan task untuk mengambil data CSV dari PostgreSQL (ambil_data)
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data) 
    
    # mendefinisikan task untuk melakukan processing data data CSV yang sudah diambil dari PostgreSQL (preprocessing)
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # mendefinisikan task untuk melakukan load datayang sudah di processing ke elasticsearch (upload_to_elasticsearch)
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    # mengatur urutan eksekusi task
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data
