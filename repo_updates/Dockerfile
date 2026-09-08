# ==========================================
# DOCKERFILE REFERENCE: FOR CLOUD RUN
# ==========================================
# This single multi-stage Dockerfile can be adapted 
# for either your data collector or your frontend dashboard app.

# Use an official lightweight Python runtime as a parent image
FROM python:3.11-slim

# Set system environment variables
ENV PYTHONUNBUFFERED=1 \
    PORT=8080 \
    APP_HOME=/app

# Set the working directory inside the container
WORKDIR $APP_HOME

# Install system dependencies if required (gcc, git, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy local code over to the container image
COPY . ./

# Install Python production dependencies
# Ensure your requirements.txt matches your target script's imports!
RUN pip install --no-cache-dir -r requirements.txt

# Inform Docker that the container listens on the specified port at runtime
EXPOSE 8080

# =========================================================================
# CHOOSE YOUR RUNTIME COMMAND BELOW (Comment/Uncomment based on deployment)
# =========================================================================

# OPTION A: For your Ingestion Pipeline (Flask API script)
CMD ["python", "main.py"]

# OPTION B: For your User Dashboard (Streamlit App script)
# CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
