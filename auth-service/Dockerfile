FROM python:3.11-slim
WORKDIR /app

# Copy and install dependencies first (better caching)
COPY auth-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared code
COPY shared ./shared

# Copy auth service code (changes most frequently, so last)
COPY auth-service/app ./app

# Run the service
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
