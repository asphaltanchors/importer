# Use Node.js 20 as base
FROM node:20-slim

# Install wget and supercronic
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Install supercronic
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64 \
    SUPERCRONIC=/usr/local/bin/supercronic
RUN wget -q "$SUPERCRONIC_URL" -O "$SUPERCRONIC" && \
    chmod +x "$SUPERCRONIC"

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Generate Prisma client
RUN --mount=type=secret,id=DATABASE_URL \
    DATABASE_URL=$(cat /run/secrets/DATABASE_URL) && \
    npx prisma generate

# Build TypeScript
RUN npm run build

# Set up crontab
RUN echo "0 5 * * * cd /app && npm start" > /app/crontab

# Start supercronic with our crontab
CMD ["/usr/local/bin/supercronic", "/app/crontab"]
