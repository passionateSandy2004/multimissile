# Dockerfile for MultiThreadMissile - Universal Product Extractor
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    DISPLAY=:99

# Install system dependencies for Chrome and Selenium in one step
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    curl \
    ca-certificates \
    fonts-liberation \
    procps \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    gpg \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Verify Chrome installation
RUN google-chrome --version || echo "Chrome installation verification"

# Install matching ChromeDriver using new Chrome for Testing API
RUN set -eux; \
    CHROME_VERSION="$(google-chrome --version | awk '{print $3}')" ; \
    CHROME_MAJOR="${CHROME_VERSION%%.*}" ; \
    CHROME_BUILD="${CHROME_VERSION}" ; \
    echo "Chrome version: ${CHROME_VERSION}, Major: ${CHROME_MAJOR}" ; \
    # Use Chrome for Testing API for Chrome 115+ \
    if [ "${CHROME_MAJOR}" -ge 115 ]; then \
        DRIVER_VERSION="$(curl -sSf "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_MAJOR}" || echo "")" ; \
        if [ -z "${DRIVER_VERSION}" ]; then \
            DRIVER_VERSION="$(curl -sSf "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE" || echo "")" ; \
        fi ; \
        if [ -n "${DRIVER_VERSION}" ]; then \
            wget -q -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/${DRIVER_VERSION}/linux64/chromedriver-linux64.zip" || \
            wget -q -O /tmp/chromedriver.zip "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${DRIVER_VERSION}/linux64/chromedriver-linux64.zip" ; \
        fi ; \
    else \
        # Fallback to old API for Chrome < 115 \
        DRIVER_VERSION="$(curl -sSf "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_MAJOR}" || curl -sSf "https://chromedriver.storage.googleapis.com/LATEST_RELEASE")" ; \
        wget -q -O /tmp/chromedriver.zip "https://chromedriver.storage.googleapis.com/${DRIVER_VERSION}/chromedriver_linux64.zip" ; \
    fi ; \
    if [ -f /tmp/chromedriver.zip ]; then \
        unzip -q /tmp/chromedriver.zip -d /tmp/ ; \
        if [ -f /tmp/chromedriver-linux64/chromedriver ]; then \
            mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver ; \
        elif [ -f /tmp/chromedriver ]; then \
            mv /tmp/chromedriver /usr/local/bin/chromedriver ; \
        fi ; \
        chmod +x /usr/local/bin/chromedriver ; \
        rm -rf /tmp/chromedriver.zip /tmp/chromedriver-linux64 /tmp/chromedriver ; \
        chromedriver --version || echo "ChromeDriver installation verification failed" ; \
    else \
        echo "ERROR: Failed to download ChromeDriver" ; \
        exit 1 ; \
    fi

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY main.py .

# Set Python path
ENV PYTHONPATH=/app \
    CHROMEDRIVER_PATH=/usr/local/bin/chromedriver \
    CHROME_BIN=/usr/bin/google-chrome

# Create a non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check - check if Python process is running (for background worker)
# Using ps to check if main.py process exists
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD ps aux | grep -v grep | grep "python.*main.py" || exit 1

# Run the application
CMD ["python", "main.py"]

