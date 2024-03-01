# Gunakan python:3.11.4 sebagai base image
FROM python:3.11.4

# Setel direktori kerja di dalam container
WORKDIR /app

# Salin konten direktori saat ini ke dalam container di /app
COPY . /app

# Install semua dependensi yang diperlukan yang tercantum di requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Jalankan script ETL ketika container diluncurkan
CMD ["python", "etl.py"]
