# 1. Use an official, lightweight Python 3.10 Linux image
FROM python:3.10-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Install system dependencies required by PostgreSQL (psycopg2)
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# 4. Copy ONLY requirements first (Enterprise Best Practice: Docker Layer Caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of your application code into the container
COPY . .

# 6. Expose Streamlit's default port
EXPOSE 8501

# 7. Default command to run the UI
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]