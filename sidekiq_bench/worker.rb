# frozen_string_literal: true

require "sidekiq"
require "redis"
require "json"
require "digest"
require "net/http"
require "uri"
require "connection_pool"

# ---------------------------------------------------------------------------
# Sidekiq server configuration
# ---------------------------------------------------------------------------
Sidekiq.configure_server do |config|
  config.redis = { url: ENV.fetch("REDIS_URL", "redis://redis:6379/0") }
  config.concurrency = ENV.fetch("SIDEKIQ_CONCURRENCY", "10").to_i
end

STUB_SERVER_URL = ENV.fetch("STUB_SERVER_URL", "http://stub-server:8080")
METRICS_POOL_SIZE = ENV.fetch("METRICS_POOL_SIZE", "5").to_i
HTTP_POOL_SIZE = ENV.fetch("HTTP_POOL_SIZE", "10").to_i

# ---------------------------------------------------------------------------
# Connection pools
# ---------------------------------------------------------------------------

# Pool of Redis connections for recording metrics — eliminates thread contention
# on the single global connection used previously.
METRICS_POOL = ConnectionPool.new(size: METRICS_POOL_SIZE, timeout: 5) do
  Redis.new(url: ENV.fetch("METRICS_REDIS_URL", "redis://redis:6379/15"))
end

# Pool of persistent Net::HTTP connections for I/O workload — avoids
# TCP setup/teardown overhead on every request.
stub_uri = URI(STUB_SERVER_URL)
HTTP_POOL = ConnectionPool.new(size: HTTP_POOL_SIZE, timeout: 10) do
  http = Net::HTTP.new(stub_uri.host, stub_uri.port)
  http.open_timeout = 5
  http.read_timeout = 15
  http.keep_alive_timeout = 30
  http.start
  http
end

# ---------------------------------------------------------------------------
# Benchmark worker
# ---------------------------------------------------------------------------
class BenchmarkWorker
  include Sidekiq::Job
  sidekiq_options queue: "benchmark", retry: false

  def perform(message_json)
    msg = JSON.parse(message_json)
    dequeue_time = Time.now.to_f

    success = execute_workload(msg)
    complete_time = Time.now.to_f
    run_id = msg.fetch("run_id", "default")

    metric = JSON.dump({
      "id"            => msg["id"],
      "system"        => "sidekiq",
      "workload"      => msg.fetch("workload", "unknown"),
      "run_id"        => run_id,
      "enqueue_time"  => msg["enqueue_time"],
      "dequeue_time"  => dequeue_time,
      "complete_time" => complete_time,
      "latency_ms"    => ((complete_time - msg["enqueue_time"]) * 1000).round(2),
      "processing_ms" => ((complete_time - dequeue_time) * 1000).round(2),
      "success"       => success,
    })

    METRICS_POOL.with do |r|
      r.pipelined do |pipe|
        pipe.lpush("bench:completed:#{run_id}", metric)
        pipe.incr("bench:count:#{run_id}")
      end
    end
  rescue StandardError => e
    run_id = msg&.fetch("run_id", "default") rescue "default"
    METRICS_POOL.with do |r|
      r.lpush(
        "bench:errors:#{run_id}",
        JSON.dump({ "error" => e.message, "time" => Time.now.to_f })
      )
    end
  end

  private

  def execute_workload(msg)
    workload = msg.fetch("workload", "cpu_light")
    payload  = msg.fetch("payload", {})

    case workload
    when "cpu_light"
      data = "benchmark-#{msg['id']}"
      iterations = payload.fetch("iterations", 1000)
      iterations.times { data = Digest::SHA256.hexdigest(data) }
      true

    when "io_bound"
      delay_ms = payload.fetch("delay_ms", 50)
      http_get_pooled("/delay/#{delay_ms}")

    when "fanout"
      data = "fanout-#{msg['id']}"
      100.times { data = Digest::SHA256.hexdigest(data) }
      true

    when "delayed"
      target_time = msg["target_time"]
      if target_time && Time.now.to_f < target_time
        sleep(target_time - Time.now.to_f)
      end
      data = "delayed-#{msg['id']}"
      100.times { data = Digest::SHA256.hexdigest(data) }
      true

    else
      true
    end
  end

  # Persistent HTTP GET with automatic reconnect on stale connections.
  def http_get_pooled(path)
    HTTP_POOL.with do |http|
      response = http.get(path)
      response.code == "200"
    end
  rescue IOError, Errno::EPIPE, Errno::ECONNRESET, Net::OpenTimeout => e
    # Connection went stale — open a fresh one and retry once
    HTTP_POOL.with do |http|
      http.finish rescue nil
      http.start
      response = http.get(path)
      response.code == "200"
    end
  end
end
