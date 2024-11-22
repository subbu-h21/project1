# Use the official Python 3.10 slim image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy all files from the current directory to the container's working directory
COPY . .

# Install system dependencies, including Rust and Cargo
RUN apt-get update && apt-get install -y \
    curl build-essential libssl-dev \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && apt-get clean

# Set Rust in the PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Upgrade pip
RUN pip install --upgrade pip

# Create a virtual environment
RUN python -m venv /venv

# Install dependencies using the virtual environment's pip
RUN /venv/bin/pip install --no-cache-dir -r requirements.txt

# Set the virtual environment's `bin` directory in the PATH
ENV PATH="/venv/bin:$PATH"

# Expose port 5000 for Flask
EXPOSE 5000

# Start the application with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]
