# Gunakan python:3.11.4 sebagai base image
FROM python:3.11.4

# Setel direktori kerja di dalam container
WORKDIR /app

# Salin konten direktori saat ini ke dalam container di /app
COPY . /app

# Install semua dependensi yang diperlukan yang tercantum di requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Buat port 5000 tersedia untuk dunia di luar container
EXPOSE 5000

# Tetapkan variabel lingkungan
ENV FLASK_APP=app.py

# Jalankan app.py ketika container diluncurkan
CMD ["flask", "run", "--host=0.0.0.0"]
