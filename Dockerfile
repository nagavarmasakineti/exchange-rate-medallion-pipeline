# 1. Use an official, lightweight Python image
FROM python:3.11-slim

# 2. Prevent Python from writing .pyc files and force clean logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Set the working directory inside the container
WORKDIR /app

# 4. Copy the requirements file first to optimize build caching
COPY requirements.txt .

# 5. Install your external dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy ALL your pipeline scripts into the container
COPY . .

# 7. Run your main pipeline orchestrator when the container starts
CMD ["python", "run_pipeline.py"]