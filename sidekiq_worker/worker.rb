require "digest"
require "json"
require "net/http"
require "redis"
require "sidekiq"
require "uri"

REDIS_URL = ENV.fetch("REDIS_URL", "redis://redis:6379/0")
METRICS_REDIS_URL = ENV.fetch("METRICS_REDIS_URL", "redis://metrics-redis:6379/0")
STUB_SERVER_URL = ENV.fetch("STUB_SERVER_URL", "http://stub-server:8080")
QUEUE_NAME = ENV.fetch("BENCHMARK_QUEUE", "benchmark")

Sidekiq.configure_server do |config|
  config.redis = { url: REDIS_URL }
end

METRICS = Redis.new(url: METRICS_REDIS_URL)

class BenchmarkWorker
  include Sidekiq::Job

  sidekiq_options queue: QUEUE_NAME, retry: false

  CPU_LIGHT_ITERATIONS = 1000
  BURSTY_AND_DELAYED_ITERATIONS = 100
  IO_BOUND_DELAY_MS = 50

  def perform(message_json)
    message = JSON.parse(message_json)
    run_id = message.fetch("run_id")
    workload = message.fetch("workload")
    dequeue_time = Time.now.to_f
    work_start_time = nil

    if workload == "delayed" && message["target_time"]
      remaining = message["target_time"].to_f - Time.now.to_f
      sleep(remaining) if remaining > 0
      work_start_time = Time.now.to_f
    end

    case workload
    when "cpu_light", "bursty_ingest", "delayed"
      seed_prefix =
        case workload
        when "cpu_light" then "benchmark"
        when "bursty_ingest" then "bursty_ingest"
        else "delayed"
        end
      iterations = workload == "cpu_light" ? CPU_LIGHT_ITERATIONS : BURSTY_AND_DELAYED_ITERATIONS
      data = "#{seed_prefix}-#{message.fetch('id')}"
      iterations.times do
        data = Digest::SHA256.hexdigest(data)
      end
    when "io_bound"
      url = URI("#{STUB_SERVER_URL}/delay/#{IO_BOUND_DELAY_MS}")
      response = Net::HTTP.get_response(url)
      raise "unexpected status #{response.code}" unless response.code == "200"
    else
      raise "unknown workload #{workload}"
    end

    complete_time = Time.now.to_f
    record = {
      id: message.fetch("id"),
      system: "sidekiq",
      workload: message.fetch("workload"),
      run_id: run_id,
      enqueue_time: message.fetch("enqueue_time").to_f,
      dequeue_time: dequeue_time,
      complete_time: complete_time,
      latency_ms: (complete_time - message.fetch("enqueue_time").to_f) * 1000.0,
      processing_ms: (complete_time - (work_start_time || dequeue_time)) * 1000.0,
      success: true
    }
    record[:target_time] = message["target_time"].to_f if message["target_time"]
    record[:work_start_time] = work_start_time if work_start_time
    METRICS.multi do |multi|
      multi.rpush("bench:completed:#{run_id}", JSON.generate(record))
      multi.incr("bench:accounted:#{run_id}")
    end
  rescue StandardError => e
    error_record = { _type: "error", error: e.message, time: Time.now.to_f }
    METRICS.multi do |multi|
      multi.rpush("bench:errors:#{run_id}", JSON.generate(error_record))
      multi.incr("bench:accounted:#{run_id}")
    end
  end
end
