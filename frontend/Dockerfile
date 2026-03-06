# Stage 1: Build the React frontend
FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY . .
RUN npm run build


# Stage 2: Serve with Caddy
FROM caddy:2.11-alpine

COPY --from=builder /app/dist /srv
