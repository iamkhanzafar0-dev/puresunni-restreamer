FROM node:20-alpine

RUN apk add --no-cache ffmpeg
WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY public ./public

ENV NODE_ENV=production
ENV DESTINATIONS_FILE=/data/destinations.json
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 3000 1935
CMD ["npm", "start"]
