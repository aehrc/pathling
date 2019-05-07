FROM nginx
ARG version

COPY docker/start.sh /
COPY docker/buildConfig.sh /
RUN chmod +x /start.sh /buildConfig.sh

COPY docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY dist/ /usr/share/nginx/html/

ENV CLINSIGHT_UI_VERSION=$version

CMD ["/start.sh"]
