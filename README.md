# Dropout Big Data Pipeline

Pipeline big data untuk mensimulasikan ingestion data mahasiswa menggunakan Rust, Apache Kafka, Parquet, dan Apache Spark.

Project ini berfokus pada alur big data:

```text
CSV Dataset
    -> Rust Producer
    -> Apache Kafka
    -> Rust Consumer
    -> Parquet Files
    -> Apache Spark / PySpark
    -> CSV / JSON / SVG Output
```

## Anggota Kelompok

| No. | Nama | NIM |
|---:|---|---|
| 1 | Muhammad Rafly Yahya Ramadhan | 123140148 |
| 2 | Muhammad Fauzan Naufal | 123140150 |
| 3 | Bagas Dwi Ajitya | 123140181 |
| 4 | Ahmad Aufamahdi Salam | 123140092 |
| 5 | Dela Puspita Sari | 123140080 |
| 6 | Varasina Farmadani | 123140107 |
| 7 | I Gede Krisna Yoga Saputra | 123140163 |

## Tech Stack

```text
Rust
Apache Kafka
Zookeeper
Apache Spark / PySpark
Parquet
Podman
```

## Struktur Project

```text
tubes-bigdata/
├── data/
├── logs/
├── scripts/
│   ├── 00_download_dataset.py
│   └── 01_ensure_kafka_topic.py
├── src/
│   ├── dropout-bigdata-pipeline-rust/
│   │   ├── Cargo.toml
│   │   └── src/
│   └── spark_training_parquet.py
├── Makefile
├── docker-compose.yml
└── Dockerfile.workspace
```

## Menjalankan Container

Jalankan service:

```bash
podman compose up -d
```

Cek container:

```bash
podman ps
```

Masuk ke workspace:

```bash
podman exec -it dev-workspace bash
```

Masuk ke direktori kerja:

```bash
cd /workspace
```

## Menjalankan Pipeline

Jalankan pipeline penuh:

```bash
make run
```

Contoh dengan parameter eksplisit:

```bash
make run RATE=0 SCALE=1 IDLE_TIMEOUT=10
```

Parameter:

```text
RATE=0           producer mengirim data secepat mungkin
RATE=500         producer mengirim sekitar 500 record/s
SCALE=1          dataset dikirim 1 kali
SCALE=5          dataset dikirim 5 kali
IDLE_TIMEOUT=10  consumer berhenti setelah 10 detik tidak menerima data
```

Contoh simulasi rate terbatas:

```bash
make run RATE=500 SCALE=1 IDLE_TIMEOUT=10
```

Contoh simulasi data lebih besar:

```bash
make run RATE=1000 SCALE=5 IDLE_TIMEOUT=10
```

## Evaluasi Skalabilitas

Untuk menjalankan evaluasi skala 1x, 5x, dan 10x:

```bash
make eval-scalability RATE=500 IDLE_TIMEOUT=10
```

Skala data:

```text
SCALE 1x  -> sekitar 4.424 records
SCALE 5x  -> sekitar 22.120 records
SCALE 10x -> sekitar 44.240 records
```

## Output

Output Parquet:

```text
data/preprocessed/
```

Output log dan evaluasi:

```text
logs/
```

Contoh file output:

```text
producer_rust_*.json
training_results_rust_*.json
model_metrics_rust_*.csv
feature_importance_rust_*.csv
rf_quality_summary_rust_*.csv
system_performance_summary_rust_*.csv
throughput_benchmark.svg
```

Output untuk dashboard:

```text
data/tableau_exports/ml_dropout_predictions.csv
```

## Perintah Makefile

Menampilkan bantuan:

```bash
make help
```

Menyiapkan dataset:

```bash
make prepare-data
```

Memastikan Kafka topic tersedia:

```bash
make prepare-kafka
```

Build binary Rust:

```bash
make build
```

Menjalankan pipeline penuh:

```bash
make run
```

Menjalankan evaluasi skalabilitas:

```bash
make eval-scalability
```

Membersihkan log, Parquet, dan export CSV:

```bash
make clean
```

Membersihkan build artifact Rust:

```bash
make clean-build
```

## Kafka Cold Start

Pada run pertama, Kafka kadang sudah hidup tetapi topic belum tersedia. Jika consumer mulai sebelum topic dibuat, error seperti ini bisa terjadi:

```text
UnknownTopicOrPartition
```

Akibatnya consumer dapat menerima 0 record, folder Parquet kosong, dan Spark gagal membaca schema.

Project ini menangani masalah tersebut melalui:

```text
scripts/01_ensure_kafka_topic.py
```

Script tersebut dijalankan oleh target:

```bash
make prepare-kafka
```

Target `make run` sudah memanggil `prepare-kafka` sebelum consumer dan producer dijalankan.

Jika Kafka baru menyala dan mesin lambat, gunakan timeout lebih besar:

```bash
make run RATE=0 SCALE=1 IDLE_TIMEOUT=10
```

## Tableau export

Output dari program berupa csv, output itu perlu di buka dengan aplikasi tableau desktop dan di export menjadi format .twbx dengan cara seperti berikut,  

### 1. Buka CSV di Tableau

1. Buka **Tableau Desktop**.
2. Di halaman awal, pilih **Connect → To a File → Text file**.
3. Pilih file `.csv`.
4. Tableau akan masuk ke tab **Data Source**.
5. Cek apakah datanya sudah terbaca benar:

   * delimiter koma/semicolon sesuai,
   * header kolom benar,
   * tipe data seperti number, date, string sudah tepat.

### 2. Buat worksheet/dashboard

1. Klik tab **Sheet 1**.
2. Drag field ke **Rows**, **Columns**, **Marks**, atau **Filters**.
3. Buat visualisasi sesuai kebutuhan.
4. Kalau perlu dashboard: klik **New Dashboard**, lalu masukkan sheet yang sudah dibuat.

### 3. Simpan sebagai `.twbx`

1. Klik **File → Save As**.

2. Pada bagian **Save as type**, pilih:

   **Tableau Packaged Workbook (*.twbx)**

3. Beri nama file.

4. Klik **Save**.

# Rust Build Cache

Agar Rust tidak mengunduh dependency dan rebuild dari nol setiap kali container dibuat ulang, `docker-compose.yml` menggunakan named volume:

```text
cargo-registry
cargo-git
cargo-target
```

Volume tersebut menyimpan:

```text
/usr/local/cargo/registry
/usr/local/cargo/git
/workspace/src/dropout-bigdata-pipeline-rust/target
```

Setelah build pertama, perintah berikutnya seharusnya jauh lebih cepat:

```bash
make build
```

Jangan jalankan ini kecuali ingin rebuild dari awal:

```bash
make clean-build
```

## Menghentikan Container

Keluar dari workspace:

```bash
exit
```

Matikan service:

```bash
podman compose down
```

Jika muncul error container name already in use:

```bash
podman rm -f zookeeper kafka dev-workspace
podman compose up -d
```

Jika masih ada dependent container yang tersisa, cek semua container:

```bash
podman ps -a
```

Lalu hapus container yang terkait project ini secara manual:

```bash
podman rm -f <container_id>
```

## Catatan

Project ini ditujukan untuk simulasi lokal dan kebutuhan tugas.

Konfigurasi default Kafka:

```text
127.0.0.1:9092
```

Karena project menggunakan `network_mode: host`, tidak diperlukan konfigurasi `ports:` pada `docker-compose.yml`.

Setup ini tidak ditujukan untuk production atau server publik.
