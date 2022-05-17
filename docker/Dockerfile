ARG PHP_VERSION=8.0

FROM php:${PHP_VERSION}-fpm

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpng-dev \
    libjpeg62-turbo-dev \
    libfreetype6-dev \
    locales \
    libzip-dev \
    zip \
    unzip \
    jpegoptim optipng pngquant gifsicle \
    vim \
    git \
    curl

# Install extensions
RUN docker-php-ext-install pdo_mysql exif pcntl gd zip

# Install composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# Install latest xDebug
ARG XDEBUG_VERSION
RUN pecl install xdebug${XDEBUG_VERSION}

# Clean
RUN apt-get -y autoremove \
    && apt-get clean

ARG WWWUSER
ARG WWWGROUP

RUN groupadd --force -g $WWWGROUP sail
RUN useradd -ms /bin/bash --no-user-group -g $WWWGROUP -u $WWWUSER sail

USER $WWWUSER:$WWWGROUP

# Expose port 9000 and start php-fpm server
EXPOSE 9000

CMD ["php-fpm"]
