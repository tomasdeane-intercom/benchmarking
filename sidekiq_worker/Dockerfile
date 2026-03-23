FROM ruby:3.3-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY sidekiq_worker/Gemfile /app/Gemfile
COPY sidekiq_worker/Gemfile.lock /app/Gemfile.lock
RUN bundle install

COPY sidekiq_worker/worker.rb /app/worker.rb

CMD ["sh", "-lc", "exec bundle exec sidekiq -r ./worker.rb -q \"${BENCHMARK_QUEUE:-benchmark}\" -c \"${SIDEKIQ_THREADS:-1}\""]
