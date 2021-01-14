FROM nginx

LABEL copyright="Copyright © 2018-2021, Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source Software Licence Agreement." \
  author="John Grimes <John.Grimes@csiro.au>"

COPY _docker/site.nginx /etc/nginx/conf.d/default.conf
COPY _site/ /usr/share/nginx/html/

EXPOSE 80