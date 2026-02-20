FROM php:8.3-apache

# Install system deps + PHP extensions Symfony commonly needs
RUN apt-get update && apt-get install -y \
    git unzip libzip-dev libpq-dev \
 && docker-php-ext-install zip pdo pdo_pgsql \
 && a2enmod rewrite

# Install Composer
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/html

# Copy app source
COPY . /var/www/html

# Install PHP deps (production)
RUN composer install --no-dev --optimize-autoloader

# Symfony needs writable var/
RUN chown -R www-data:www-data /var/www/html/var

# Point Apache to Symfony /public
ENV APACHE_DOCUMENT_ROOT=/var/www/html/public
RUN sed -ri -e 's!/var/www/html!/var/www/html/public!g' /etc/apache2/sites-available/*.conf \
 && sed -ri -e 's!/var/www/!/var/www/html/public!g' /etc/apache2/apache2.conf /etc/apache2/conf-available/*.conf
