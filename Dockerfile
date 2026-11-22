FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN apk add --no-cache ffmpeg && npm install --omit=dev

COPY server.js ./
COPY public ./public

ENV NODE_ENV=production

EXPOSE 3000
CMD ["npm", "start"]
