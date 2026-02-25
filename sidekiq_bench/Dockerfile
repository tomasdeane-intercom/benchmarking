FROM ruby:3.3-slim

RUN apt-get update && apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Gemfile ./
RUN bundle install --jobs=4

COPY worker.rb .

CMD bundle exec sidekiq -r ./worker.rb -q benchmark -c ${SIDEKIQ_CONCURRENCY:-10}
