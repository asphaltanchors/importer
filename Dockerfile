# Build stage
FROM node:22-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npx prisma generate
RUN npm run build

# Production stage
FROM node:22-alpine
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules/@prisma ./node_modules/@prisma
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma
COPY package*.json ./
RUN npm ci --only=production

# Install supercronic
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64
RUN apk add --no-cache wget && \
    wget -q "$SUPERCRONIC_URL" -O /usr/local/bin/supercronic && \
    chmod +x /usr/local/bin/supercronic && \
    apk del wget

# Set up crontab
RUN echo "0 5 * * * cd /app && node dist/process-daily-imports.js" > /app/crontab

CMD ["/usr/local/bin/supercronic", "/app/crontab"]
