FROM php:8.4-apache

# System deps + PHP extensions Symfony commonly needs
RUN apt-get update && apt-get install -y \
    git unzip libzip-dev libpq-dev \
 && docker-php-ext-install zip pdo pdo_pgsql \
 && rm -rf /var/lib/apt/lists/*

# Enable Apache modules Symfony typically needs
RUN a2enmod rewrite headers env

# Set Apache DocumentRoot to Symfony /public
ENV APACHE_DOCUMENT_ROOT=/var/www/html/public
RUN sed -ri -e 's!/var/www/html!${APACHE_DOCUMENT_ROOT}!g' /etc/apache2/sites-available/000-default.conf \
 && sed -ri -e 's!<Directory /var/www/>!<Directory ${APACHE_DOCUMENT_ROOT}/>!g' /etc/apache2/apache2.conf \
 && sed -ri -e 's/AllowOverride None/AllowOverride All/g' /etc/apache2/apache2.conf

# Install Composer
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/html

# Copy app source
COPY . /var/www/html

# App env defaults (Render should override DATABASE_URL in dashboard)
ENV APP_ENV=prod
ENV APP_DEBUG=0

# Install PHP deps (production)
# Install PHP deps (production)
RUN HOME=/tmp git config --global --add safe.directory /var/www/html || true
RUN chown -R www-data:www-data /var/www/html
RUN COMPOSER_ALLOW_SUPERUSER=1 \
    COMPOSER_MEMORY_LIMIT=-1 \
    composer install \
      --no-dev \
      --no-interaction \
      --no-progress \
      --prefer-dist \
      --no-scripts \
      --optimize-autoloader

# Ensure writable directories for Symfony (cache, sessions, logs)
RUN mkdir -p var/cache var/log var/sessions \
 && chown -R www-data:www-data var \
 && chmod -R ug+rwX var


EXPOSE 80

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN sed -i 's/\r//' /usr/local/bin/docker-entrypoint.sh \
 && chmod +x /usr/local/bin/docker-entrypoint.sh
CMD ["/usr/local/bin/docker-entrypoint.sh"]
