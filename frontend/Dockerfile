FROM node:22-alpine AS build

WORKDIR /src
COPY frontend/package.json frontend/build.mjs frontend/index.html frontend/styles.css frontend/app.js frontend/config.js ./
RUN npm run build

FROM nginx:1.27-alpine

ENV API_BASE_URL=/api \
    BACKEND_URL=http://messagequeue-backend:8080

COPY --from=build /src/dist/ /usr/share/nginx/html/
COPY frontend/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY frontend/docker-entrypoint.d/20-messagequeue-config.sh /docker-entrypoint.d/20-messagequeue-config.sh
RUN chmod +x /docker-entrypoint.d/20-messagequeue-config.sh \
    && chown -R nginx:nginx /usr/share/nginx/html /etc/nginx/templates /etc/nginx/conf.d /docker-entrypoint.d /var/cache/nginx /var/run /run

USER 101:101

# The nginx image entrypoint expands BACKEND_URL in the template. This keeps
# the browser same-origin while allowing a chart to set the release-scoped
# backend Service URL without rebuilding the image.
