# Visualisasi Airflow dan Kibana 📊 - ![Airflow](https://img.shields.io/badge/Airflow-17B3A3?style=for-the-badge&logo=Apache-Airflow&logoColor=white) ![Kibana](https://img.shields.io/badge/Kibana-005571?style=for-the-badge&logo=Kibana&logoColor=white) ![Elastic Search] (https://img.shields.io/badge/Elastic_Search-005571?style=for-the-badge&logo=elasticsearch&logoColor=white)

Repositori ini berisi proyek visualisasi data menggunakan Apache Airflow dan Kibana. Proyek ini bertujuan untuk mengautomasi alur kerja pengolahan data dan menyajikannya secara visual menggunakan Kibana. Repositori ini mencakup file Python yang menjelaskan proses secara menyeluruh, mulai dari pengaturan DAG di Airflow, pengindexan data di Elastic Search hingga visualisasi data di Kibana.

## Daftar Isi 🗒️
1. [Project Overview](#project-overview-)
2. [Metode yang Digunakan](#metode-yang-digunakan-)
3. [File yang Tersedia](#file-yang-tersedia-)
4. [Cara Menggunakan Project Ini](#cara-menggunakan-project-ini-)
5. [Dependencies](#dependencies-)
6. [Libraries](#libraries-)
7. [Author](#author-)

## Project Overview 📝

Dalam proyek ini, saya menggunakan Apache Airflow untuk mengautomasi proses ETL (Extract, Transform, Load) dan menggunakan Kibana untuk visualisasi data yang telah diproses. Beberapa langkah utama yang dicakup dalam proyek ini adalah:

1. **Pengaturan Airflow**:
    - Mengonfigurasi dan menjalankan Airflow untuk mengelola alur kerja.

2. **Pengembangan DAG**:
    - Membangun DAG (Directed Acyclic Graph) untuk mendefinisikan alur kerja data.

3. **Transformasi Data**:
    - Melakukan transformasi data yang diperlukan sebelum visualisasi.

4. **Visualisasi dengan Kibana**:
    - Menggunakan Kibana untuk menyajikan data dalam format yang dapat dianalisis secara visual.

## Metode yang Digunakan 🛠️

- ETL (Extract, Transform, Load)
- Visualisasi Data
- Automasi Proses

## File yang Tersedia 📂

- `airflow_DAG.py`: Skrip Python yang berisi definisi DAG di Airflow, termasuk langkah-langkah dalam proses ETL.
- `great_expectations.ipynb`: Jupyter Notebook yang berisi validasi data menggunakan Great Expectations.
- `(folder) images`: Hasil visualisasi dari Kibana.
  
## Cara Menggunakan Project Ini 💻

1. Clone repositori ini ke dalam lokal Anda:
    ```bash
    git clone https://github.com/ciputrawangsa/Visualization-Airflow-Kibana.git
    ```

2. Jalankan Apache Airflow untuk mengelola alur kerja:
    ```bash
    airflow webserver --port 8080
    airflow scheduler
    ```

3. Jalankan DAG di Airflow untuk memulai proses ETL.

4. Akses Kibana untuk melihat visualisasi data.

## Dependencies ⚙️

- ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) 3.10.14
- ![Apache Airflow](https://img.shields.io/badge/Airflow-17B3A3?style=for-the-badge&logo=Apache-Airflow&logoColor=white)

## Libraries 📚
- Apache Airflow
- Pandas
- NumPy
- Elasticsearch
- Kibana
- Sqlalchemy
- Datetime

## Author ✍️
**Ciputra Wangsa**
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ciputra-wangsa/)
