FROM nginx

COPY docker/start.sh /
COPY docker/buildConfig.sh /
RUN chmod +x /start.sh /buildConfig.sh

COPY docker/ui.nginx /etc/nginx/conf.d/default.conf
COPY target/site/ /usr/share/nginx/html/

ARG VERSION
ENV PATHLING_UI_VERSION=$VERSION

CMD ["/start.sh"]
