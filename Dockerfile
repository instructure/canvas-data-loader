FROM rust:1.23.0

WORKDIR /usr/src/canvas-data-loader
COPY . .

RUN apt-get update && apt-get -y --no-install-recommends install clang cron && apt-get clean

RUN cargo install && rm -rf ~/.cargo/registry ~/.cargo/git

RUN (crontab -l ; echo "0 4 * * * cd /usr/src/canvas-data-loader && RUST_LOG=info /usr/local/cargo/bin/cdl-runner > /proc/1/fd/1 2>/proc/1/fd/2") | crontab

CMD ["cron", "-f"]
