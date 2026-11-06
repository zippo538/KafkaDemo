# KafkaDemo --- .NET 10 Web API + Apache Kafka + PostgreSQL

Project contoh **Monolith Web API** menggunakan .NET 10, Apache Kafka,
PostgreSQL, Confluent.Kafka, Entity Framework Core, dan Serilog. Project
menyediakan REST API untuk membuat dan membaca order, serta Kafka
Producer dan Background Consumer untuk pemrosesan event secara
asynchronous.

> **Catatan konfigurasi:** README ini mengikuti konfigurasi yang
> digunakan dalam percakapan: API `http://localhost:5077`, Kafka dari
> host Windows `localhost:29092`, Kafka dari jaringan Docker
> `kafka:9092`, dan PostgreSQL `localhost:5432`. Periksa
> `docker-compose.yml`, `appsettings.json`, dan
> `Properties/launchSettings.json` di repository Anda. Jika port atau
> nama service berbeda, gunakan nilai aktual dari file tersebut.

## 1. Gambaran arsitektur

``` text
Client / Postman / Katalon
          |
          v
.NET 10 Web API (OrdersController)
          |
          v
      OrderService
       /       \
      v         v
PostgreSQL   Kafka Producer
                 |
                 v
          Kafka: order-created
                 |
                 v
          Kafka Consumer
                 |
                 v
       Business processing
                 |
                 +---- gagal setelah retry ---> order-created-dlt
```

-   **POST `/api/orders`**: membuat order dan menerbitkan event
    `OrderCreated`.
-   **GET `/api/orders`**: membaca daftar order langsung dari
    PostgreSQL.
-   **GET `/api/orders/{id}`**: membaca detail order langsung dari
    PostgreSQL.
-   **Kafka Producer**: mengirim event ke topic Kafka.
-   **Kafka Consumer**: membaca dan memproses event, menggunakan manual
    offset commit, retry, dan Dead Letter Topic (DLT).
-   **Serilog**: mencatat log ke Console dan file.
-   **Correlation ID**: menghubungkan log/request dengan event Kafka
    jika sudah diterapkan pada versi project.

GET tidak membaca data dari Kafka. Kafka digunakan untuk distribusi
event dan pemrosesan asynchronous, sedangkan PostgreSQL menjadi
penyimpanan data order.

## 2. Prasyarat

Pastikan perangkat memiliki:

1.  **.NET 10 SDK**
    -   Periksa: `dotnet --version`
    -   Hasil harus menunjukkan versi SDK `10.x`.
2.  **Docker Engine di WSL2** atau Docker Desktop.
    -   Untuk setup WSL: Docker Engine harus aktif di distro WSL yang
        digunakan.
    -   Periksa dari terminal WSL: `docker version` dan
        `docker compose version`.
3.  **WSL2** (jika Docker dijalankan di WSL).
    -   Periksa dari PowerShell: `wsl --list --verbose`.
4.  **Git** (opsional, jika mengambil project dari repository).
5.  **Postman** atau **Katalon Studio** untuk pengujian API.

## 3. Clone atau buka project

Jika project berada di repository Git:

``` bash
git clone <URL_REPOSITORY>
cd KafkaDemo
```

Jika project sudah tersedia di komputer, buka terminal pada direktori
yang berisi `KafkaDemo.csproj`.

Contoh PowerShell:

``` powershell
cd C:\Users\<USER>\Downloads\KafkaDemo
```

Contoh WSL jika source berada di drive C:

``` bash
cd /mnt/c/Users/<USER>/Downloads/KafkaDemo
```

Jalankan perintah .NET dari terminal Windows atau WSL sesuai SDK dan
lingkungan yang digunakan. Jangan mencampur konfigurasi alamat
`localhost` untuk host Windows dengan nama DNS service Docker.

## 4. Konfigurasi aplikasi

### 4.1 `appsettings.json`

Pastikan bagian konfigurasi memiliki nilai yang sesuai dengan lingkungan
Anda. Contoh untuk menjalankan .NET API langsung di Windows sementara
Kafka dan PostgreSQL berjalan di Docker/WSL:

``` json
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Port=5432;Database=orderdb;Username=postgres;Password=postgres123"
  },
  "Kafka": {
    "BootstrapServers": "localhost:29092",
    "OrderCreatedTopic": "order-created",
    "OrderCreatedDltTopic": "order-created-dlt",
    "ConsumerGroupId": "order-service-group",
    "RetryCount": 3,
    "RetryDelayMilliseconds": 1000
  }
}
```

Sesuaikan username, password, database, dan port PostgreSQL dengan
`docker-compose.yml` Anda. Jangan menyimpan password production di
repository; gunakan User Secrets atau environment variables.

Untuk .NET API yang berjalan **di dalam Docker Compose network**, alamat
Kafka biasanya menggunakan nama service dan port internal, misalnya
`kafka:9092`, bukan `localhost:29092`. Untuk API yang berjalan langsung
di Windows, gunakan port host yang dipublikasikan oleh Compose, misalnya
`localhost:29092`.

### 4.2 `Properties/launchSettings.json`

Periksa `applicationUrl`. Contoh:

``` json
{
  "profiles": {
    "http": {
      "commandName": "Project",
      "applicationUrl": "http://localhost:5077"
    }
  }
}
```

Jika file Anda menetapkan port lain, gunakan port tersebut untuk request
API dan Katalon.

### 4.3 Serilog

Pastikan konfigurasi `Serilog` tersedia di `appsettings.json` dan
package yang diperlukan terpasang. Contoh package:

``` bash
dotnet add package Serilog.AspNetCore
dotnet add package Serilog.Settings.Configuration
dotnet add package Serilog.Sinks.Console
dotnet add package Serilog.Sinks.File
dotnet add package Serilog.Enrichers.Environment
dotnet add package Serilog.Enrichers.Thread
```

Konfigurasi Serilog yang sudah ada di project akan menentukan level
minimum, output Console, dan lokasi file. Pada contoh setup sebelumnya,
file log disimpan di `logs/` dengan rolling harian.

## 5. Jalankan Kafka dan PostgreSQL

Jalankan perintah dari direktori yang memiliki `docker-compose.yml`:

``` bash
docker compose up -d
```

Periksa status container:

``` bash
docker compose ps
```

Lihat log Kafka:

``` bash
docker compose logs -f kafka
```

Lihat log PostgreSQL:

``` bash
docker compose logs -f postgres
```

> Nama service pada perintah `docker compose logs` harus sama dengan
> nama service di `docker-compose.yml`. Jika service PostgreSQL bukan
> `postgres`, ganti sesuai file Compose.

### 5.1 Jika Docker dijalankan di WSL2

Dari PowerShell Windows, periksa nama distro:

``` powershell
wsl --list --verbose
```

Kemudian jalankan perintah Docker melalui distro tersebut. Ganti
`Ubuntu` jika nama distro Anda berbeda:

``` powershell
wsl -d Ubuntu -- docker compose ps
```

Untuk menjalankan Compose dari folder project yang berada di drive C:

``` powershell
wsl -d Ubuntu
```

Lalu di terminal WSL:

``` bash
cd /mnt/c/Users/<USER>/Downloads/KafkaDemo
docker compose up -d
docker compose ps
```

Jika Katalon berjalan di Windows dan perlu mengontrol Docker yang berada
di WSL, script Katalon dapat menjalankan `wsl.exe`, misalnya:

``` powershell
wsl -d Ubuntu -- docker ps
```

Nama distro dan nama container harus disesuaikan dengan hasil aktual.
Jangan menganggap nama container selalu `kafka`; periksa dengan
`docker ps`.

## 6. Buat Kafka topics

Consumer project menggunakan `AllowAutoCreateTopics = false`, sehingga
topic perlu dibuat terlebih dahulu.

Perintah berikut mengasumsikan: - Nama container Kafka adalah `kafka`. -
Image Kafka menyediakan CLI di `/opt/kafka/bin`. - Broker internal
container dapat diakses melalui `localhost:9092`.

Buat topic utama:

``` bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic order-created \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Buat topic DLT:

``` bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic order-created-dlt \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Periksa daftar topic:

``` bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Periksa detail topic:

``` bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic order-created \
  --bootstrap-server localhost:9092
```

Jika nama container, lokasi CLI, atau port internal berbeda, sesuaikan
perintah. Jika topic sudah ada, perintah `--create` dapat menampilkan
pesan bahwa topic sudah tersedia; itu bukan berarti topic harus dibuat
ulang.

## 7. Restore, build, dan jalankan .NET API

Dari root project:

``` bash
dotnet restore
dotnet build
```

Jika build berhasil, jalankan:

``` bash
dotnet run
```

Pastikan output menampilkan alamat listening, misalnya:

``` text
Now listening on: http://localhost:5077
Application started. Press Ctrl+C to shut down.
```

Consumer seharusnya menampilkan log sejenis:

``` text
Kafka consumer started. Topic=order-created, Group=order-service-group
```

Jika hanya log Consumer yang muncul tetapi tidak ada `Now listening on`,
periksa startup aplikasi dan output exception lainnya. Consumer started
tidak dengan sendirinya membuktikan HTTP server sudah listening.

Hentikan API dengan `Ctrl+C`.

## 8. Uji API

Gunakan base URL yang benar-benar ditampilkan oleh `dotnet run`. Contoh
di bawah menggunakan `http://localhost:5077`.

### 8.1 Health check

``` http
GET http://localhost:5077/health
```

Jika endpoint `/health` tersedia, respons yang diharapkan adalah HTTP
200.

### 8.2 Buat order

``` http
POST http://localhost:5077/api/orders
Content-Type: application/json
```

Body:

``` json
{
  "customerName": "Katalon Test",
  "totalAmount": 250000
}
```

Jika Producer berhasil menerbitkan event dan controller mengembalikan
`Accepted`, respons yang diharapkan adalah HTTP 202 dan berisi
`orderId`.

### 8.3 Ambil semua order

``` http
GET http://localhost:5077/api/orders
```

Endpoint ini membaca PostgreSQL.

### 8.4 Ambil order berdasarkan ID

``` http
GET http://localhost:5077/api/orders/<ORDER_ID>
```

Ganti `<ORDER_ID>` dengan ID dari respons POST. Jika tidak ditemukan,
endpoint seharusnya mengembalikan HTTP 404.

## 9. Memantau log Serilog

Jika konfigurasi Serilog mengarahkan log ke `logs/log-.txt`, periksa
folder `logs` pada root project.

PowerShell:

``` powershell
Get-Content .\logs\log-*.txt -Wait
```

WSL/Linux:

``` bash
tail -f logs/log-*.txt
```

Log Console juga dapat dilihat langsung pada terminal tempat
`dotnet run` dijalankan.

Hindari mencatat password, access token, connection string, atau data
pribadi customer ke log.

## 10. Menjalankan test Katalon

Project pengujian Katalon yang disiapkan berisi skenario berikut:

  -----------------------------------------------------------------------
  Test Case               Skenario                Ekspektasi utama
  ----------------------- ----------------------- -----------------------
  TC01                    Create Order            HTTP 202 dan `orderId`

  TC02                    Get All Orders          HTTP 200 dan array
                                                  `data`

  TC03                    Get Order By ID         HTTP 200 dan ID sesuai

  TC04                    Invalid Order           HTTP 400

  TC05                    Order Not Found         HTTP 404

  TC06                    50 concurrent requests  Request berhasil dan ID
                                                  unik

  TC07                    100 sequential burst    Request berhasil dan ID
                          requests                unik

  TC08                    Kafka broker            Gagal publish saat
                          outage/recovery         broker mati dan pulih
                                                  setelah broker aktif
  -----------------------------------------------------------------------

Sebelum menjalankan test: 1. Pastikan Kafka dan PostgreSQL aktif. 2.
Pastikan topic sudah dibuat. 3. Jalankan .NET API. 4. Pastikan `baseUrl`
di script Katalon sesuai dengan alamat API. 5. Jalankan TC01--TC07
terlebih dahulu. 6. Jalankan TC08 hanya di environment development lokal
yang boleh mengalami penghentian Kafka.

### Catatan TC08 pada Windows + WSL

Jika Katalon berjalan di Windows sedangkan Docker CLI berada di WSL,
script perlu memanggil Docker melalui WSL, bukan menjalankan `docker`
langsung. Contoh:

``` groovy
List<String> command = [
    'wsl.exe',
    '-d',
    'Ubuntu',
    '--',
    'docker',
    'ps'
]

Process p = new ProcessBuilder(command)
    .redirectErrorStream(true)
    .start()

String output = p.inputStream.getText('UTF-8')
int exitCode = p.waitFor()

assert exitCode == 0 : output
```

Ganti `Ubuntu` dengan nama distro dari `wsl --list --verbose`. Nama
container Kafka juga harus diverifikasi dengan `docker ps`.

## 11. Troubleshooting

### 11.1 `Unknown topic or partition`

Penyebab umum: topic belum dibuat, nama topic tidak cocok, atau aplikasi
terhubung ke broker/cluster yang berbeda.

Periksa:

``` bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Pastikan `order-created` dan `order-created-dlt` tersedia pada broker
yang sama dengan yang digunakan aplikasi.

### 11.2 `Broker: Coordinator load in progress`

Dapat muncul sementara ketika broker baru dimulai atau coordinator
sedang melakukan inisialisasi. Tunggu broker siap, lalu periksa kembali
log. Jika berulang terus, periksa log Kafka dan status broker.

### 11.3 API tidak bisa terhubung ke Kafka

Pastikan alamat bootstrap sesuai dengan tempat API berjalan:

  -----------------------------------------------------------------------
  Lokasi API                          Contoh alamat Kafka
  ----------------------------------- -----------------------------------
  API berjalan langsung di Windows    `localhost:29092` (jika port
                                      tersebut dipublish oleh Compose)

  API berjalan dalam Docker Compose   `kafka:9092` (jika service bernama
                                      `kafka`)

  CLI dijalankan di container Kafka   `localhost:9092`
  -----------------------------------------------------------------------

Alamat di atas adalah contoh berdasarkan pola konfigurasi; port aktual
ditentukan oleh `docker-compose.yml` dan advertised listener Kafka.

### 11.4 Katalon: `Cannot run program "docker"`

Katalon tidak menemukan Docker CLI Windows. Jika Docker dijalankan di
WSL, panggil `wsl.exe -d <NAMA_DISTRO> -- docker ...` dari script
Katalon. Pastikan perintah tersebut berhasil terlebih dahulu dari
PowerShell.

### 11.5 Consumer tidak memproses event

Periksa: - Topic benar dan tersedia. - `BootstrapServers` benar. -
Consumer group yang digunakan benar. - Pesan benar-benar diterbitkan
oleh Producer. - Log Consumer dan offset Kafka. - Apakah pesan sudah
dikonsumsi oleh consumer group tersebut sebelumnya.

`AutoOffsetReset = Earliest` hanya berlaku ketika group belum memiliki
offset yang valid; pengaturan ini tidak mengulang otomatis semua pesan
yang sudah di-commit oleh group.

### 11.6 Katalon concurrent test gagal atau timeout

Mulai dari jumlah request dan thread yang kecil. Periksa exception
lengkap, response HTTP, log .NET, PostgreSQL, serta resource mesin.
Kegagalan test API tidak otomatis berarti Kafka yang bermasalah.

## 12. Catatan reliability dan batasan implementasi

1.  **HTTP 202 bukan bukti Consumer selesai.** Itu hanya menunjukkan API
    mengembalikan Accepted setelah tahapan yang dilakukan
    controller/Producer berhasil.
2.  **GET order membaca PostgreSQL**, bukan status pemrosesan Consumer.
3.  **Idempotency perlu diuji dengan EventId yang sama.** Dua POST order
    normal biasanya membuat EventId berbeda, sehingga tidak menguji
    duplicate event yang sesungguhnya.
4.  **Transactional Outbox direkomendasikan.** Jika aplikasi menyimpan
    order ke PostgreSQL lalu publish ke Kafka sebagai dua operasi
    terpisah, kegagalan di antara keduanya dapat menyebabkan order
    tersimpan tanpa event. Outbox menyimpan perubahan bisnis dan event
    dalam satu transaksi database, lalu publisher terpisah mengirim
    event.
5.  **Retry dan DLT bukan pengganti idempotency.** Consumer harus aman
    jika pesan dikirim ulang.
6.  **Load test ini bukan benchmark produksi.** Tentukan SLO,
    throughput, latency percentile, dan ukuran data terlebih dahulu.
    Pantau consumer lag, CPU, RAM, database connection pool, dan error
    rate.

## 13. Perintah ringkas sehari-hari

Urutan startup:

``` bash
# 1. Jalankan dari folder yang berisi docker-compose.yml
docker compose up -d

# 2. Periksa container
docker compose ps

# 3. Pastikan topic tersedia (buat jika belum ada)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

# 4. Restore dan build aplikasi
dotnet restore
dotnet build

# 5. Jalankan API
dotnet run
```

Urutan shutdown:

``` bash
# Hentikan API dengan Ctrl+C terlebih dahulu.

# Hentikan container tanpa menghapus volume
docker compose stop
```

Untuk menghapus container dan network Compose:

``` bash
docker compose down
```

Perintah `docker compose down -v` juga menghapus named volumes dan dapat
menghapus data PostgreSQL/Kafka. **Jangan gunakan `-v` kecuali memang
ingin menghapus seluruh data development.**

------------------------------------------------------------------------

## Referensi

-   [.NET documentation](https://learn.microsoft.com/dotnet/)
-   [Confluent.Kafka .NET
    client](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html)
-   [Apache Kafka
    documentation](https://kafka.apache.org/documentation/)
-   [Docker Compose documentation](https://docs.docker.com/compose/)
-   [Serilog.AspNetCore](https://github.com/serilog/serilog-aspnetcore)
-   [Katalon Studio documentation](https://docs.katalon.com/)
