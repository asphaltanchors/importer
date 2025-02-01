# Build stage
FROM node:22-alpine AS builder
RUN apk add --no-cache openssl
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npx prisma generate
RUN npm run build

# Production stage
FROM node:22-alpine
RUN apk add --no-cache openssl wget
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/prisma ./prisma
COPY --from=builder /app/node_modules/@prisma ./node_modules/@prisma
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma
COPY package*.json ./
RUN npm ci --only=production

# Set up architecture-specific supercronic
ARG TARGETARCH
ENV SUPERCRONIC_VERSION=v0.2.33
RUN SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${TARGETARCH} && \
    wget -q "$SUPERCRONIC_URL" -O /usr/local/bin/supercronic && \
    chmod +x /usr/local/bin/supercronic 

# Set up crontab and entrypoint
RUN echo "0 5 * * * cd /app && node dist/process-daily-imports.js" > /app/crontab
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

CMD ["/app/docker-entrypoint.sh"]
