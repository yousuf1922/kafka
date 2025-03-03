FROM node:18-alpine
WORKDIR /app
COPY package.json .
RUN apk add --no-cache bash && npm install && apk del bash
COPY . .
EXPOSE 3000
CMD ["npm", "start"]