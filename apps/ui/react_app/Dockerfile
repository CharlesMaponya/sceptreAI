FROM node:24.18.0-alpine3.23 AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.28.3-alpine3.23
RUN apk del \
      nginx \
      nginx-module-acme \
      nginx-module-geoip \
      nginx-module-image-filter \
      nginx-module-njs \
      nginx-module-xslt \
    && apk add --no-cache nginx \
    && apk upgrade --no-cache
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
ENV NGINX_ENVSUBST_FILTER=API_SERVICE_HOST \
    NGINX_ENVSUBST_OUTPUT_DIR=/etc/nginx/http.d \
    API_SERVICE_HOST=api
EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1/ || exit 1
