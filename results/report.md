# Kafka vs Sidekiq — Benchmark Report

Generated: 2026-02-18 15:01:16

Total runs: 56

---

## Summary Comparison

### cpu_light

| Metric | Kafka | Sidekiq |
|--------|------:|--------:|
| Consumed msg/s | 2668.22 | 2238.69 |
| Produce msg/s | 222331.84 | 53232.59 |
| Latency p50 (ms) | 1506.42 | 1605.12 |
| Latency p95 (ms) | 2567.72 | 3008.8 |
| Latency p99 (ms) | 2666.14 | 3128.83 |
| Latency max (ms) | 2690.25 | 3159.23 |
| Drain time (s) | 2.55 | 3.16 |
| Error rate (%) | 0.0 | 0.0 |
| Runs | 7 run(s) | 7 run(s) |

### delayed

| Metric | Kafka | Sidekiq |
|--------|------:|--------:|
| Consumed msg/s | 1277.95 | 845.94 |
| Produce msg/s | 196600.01 | 14277.64 |
| Latency p50 (ms) | 2508.08 | 6707.24 |
| Latency p95 (ms) | 4805.74 | 7579.28 |
| Latency p99 (ms) | 4979.75 | 7636.14 |
| Latency max (ms) | 5023.56 | 7649.1 |
| Drain time (s) | 4.88 | 7.65 |
| Error rate (%) | 0.0 | 0.0 |
| Runs | 7 run(s) | 7 run(s) |

### fanout

| Metric | Kafka | Sidekiq |
|--------|------:|--------:|
| Consumed msg/s | 10888.8 | 6201.57 |
| Produce msg/s | 46305.72 | 25557.71 |
| Latency p50 (ms) | 207.56 | 459.37 |
| Latency p95 (ms) | 408.59 | 834.3 |
| Latency p99 (ms) | 568.55 | 868.47 |
| Latency max (ms) | 576.14 | 878.52 |
| Drain time (s) | 0.4 | 0.88 |
| Error rate (%) | 0.0 | 0.0 |
| Runs | 7 run(s) | 7 run(s) |

### io_bound

| Metric | Kafka | Sidekiq |
|--------|------:|--------:|
| Consumed msg/s | 278.54 | 377.21 |
| Produce msg/s | 227667.19 | 61988.72 |
| Latency p50 (ms) | 63963.56 | 17443.76 |
| Latency p95 (ms) | 121745.65 | 33096.55 |
| Latency p99 (ms) | 126931.63 | 34507.66 |
| Latency max (ms) | 128230.08 | 34851.91 |
| Drain time (s) | 128.08 | 34.85 |
| Error rate (%) | 0.0 | 0.0 |
| Runs | 7 run(s) | 7 run(s) |

---

## Detailed Run Results

### kafka-cpu_light-20260211_163455-df78165e
- **System**: kafka  **Workload**: cpu_light
- **Messages**: 5000 ok / 0 err
- **Throughput**: 2812.06 msg/s consumed
- **Latency**: p50=884.5 p95=1673.03 p99=1742.72 max=1759.37 ms
- **Timing**: total=1.778s produce=0.02s drain=1.758s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10

### kafka-cpu_light-20260211_163506-18782c27
- **System**: kafka  **Workload**: cpu_light
- **Messages**: 5000 ok / 0 err
- **Throughput**: 2822.92 msg/s consumed
- **Latency**: p50=900.48 p95=1662.63 p99=1735.95 max=1753.36 ms
- **Timing**: total=1.771s produce=0.02s drain=1.751s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10

### kafka-cpu_light-20260211_163517-79ac3080
- **System**: kafka  **Workload**: cpu_light
- **Messages**: 5000 ok / 0 err
- **Throughput**: 2818.43 msg/s consumed
- **Latency**: p50=860.34 p95=1664.07 p99=1737.13 max=1755.41 ms
- **Timing**: total=1.774s produce=0.02s drain=1.754s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10

### kafka-cpu_light-20260218_135033-fd10715b
- **System**: kafka  **Workload**: cpu_light
- **Messages**: 200 ok / 0 err
- **Throughput**: 41.46 msg/s consumed
- **Latency**: p50=4781.24 p95=4817.02 p99=4820.2 max=4821.08 ms
- **Timing**: total=4.824s produce=0.987s drain=3.836s
- **Config**: messages=200, warmup=0, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-cpu_light-20260218_145522-8110dd34
- **System**: kafka  **Workload**: cpu_light
- **Messages**: 10000 ok / 0 err
- **Throughput**: 3454.78 msg/s consumed
- **Latency**: p50=1003.58 p95=2666.77 p99=2821.95 max=2860.54 ms
- **Timing**: total=2.895s produce=0.037s drain=2.857s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-cpu_light-20260218_145538-4066c27b
- **System**: kafka  **Workload**: cpu_light
- **Messages**: 10000 ok / 0 err
- **Throughput**: 3486.74 msg/s consumed
- **Latency**: p50=1046.32 p95=2632.78 p99=2794.09 max=2832.54 ms
- **Timing**: total=2.868s produce=0.037s drain=2.831s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-cpu_light-20260218_145550-24cfb623
- **System**: kafka  **Workload**: cpu_light
- **Messages**: 10000 ok / 0 err
- **Throughput**: 3241.14 msg/s consumed
- **Latency**: p50=1068.45 p95=2857.75 p99=3010.93 max=3049.44 ms
- **Timing**: total=3.085s produce=0.038s drain=3.048s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-delayed-20260211_165143-28cb6cf5
- **System**: kafka  **Workload**: delayed
- **Messages**: 5000 ok / 0 err
- **Throughput**: 995.64 msg/s consumed
- **Latency**: p50=2500.88 p95=4750.7 p99=4950.23 max=5000.69 ms
- **Timing**: total=5.022s produce=0.022s drain=5.0s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10, delay_seconds=5.0

### kafka-delayed-20260211_165156-607cbbf8
- **System**: kafka  **Workload**: delayed
- **Messages**: 5000 ok / 0 err
- **Throughput**: 995.7 msg/s consumed
- **Latency**: p50=2500.41 p95=4751.03 p99=4950.33 max=5000.92 ms
- **Timing**: total=5.022s produce=0.022s drain=4.999s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10, delay_seconds=5.0

### kafka-delayed-20260211_165210-02df3d8c
- **System**: kafka  **Workload**: delayed
- **Messages**: 5000 ok / 0 err
- **Throughput**: 965.86 msg/s consumed
- **Latency**: p50=2501.31 p95=5132.88 p99=5152.0 max=5156.71 ms
- **Timing**: total=5.177s produce=0.023s drain=5.154s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10, delay_seconds=5.0

### kafka-delayed-20260218_135052-041820b9
- **System**: kafka  **Workload**: delayed
- **Messages**: 200 ok / 0 err
- **Throughput**: 39.97 msg/s consumed
- **Latency**: p50=2550.57 p95=4751.87 p99=4951.52 max=5001.13 ms
- **Timing**: total=5.004s produce=1.016s drain=3.988s
- **Config**: messages=200, warmup=0, partitions=12, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-delayed-20260218_145746-38e2c8b2
- **System**: kafka  **Workload**: delayed
- **Messages**: 10000 ok / 0 err
- **Throughput**: 1982.33 msg/s consumed
- **Latency**: p50=2501.22 p95=4751.09 p99=4951.55 max=5002.69 ms
- **Timing**: total=5.045s produce=0.043s drain=5.002s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-delayed-20260218_145800-4172ac13
- **System**: kafka  **Workload**: delayed
- **Messages**: 10000 ok / 0 err
- **Throughput**: 1982.44 msg/s consumed
- **Latency**: p50=2501.82 p95=4751.05 p99=4951.48 max=5002.02 ms
- **Timing**: total=5.044s produce=0.043s drain=5.001s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-delayed-20260218_145814-808dc8a2
- **System**: kafka  **Workload**: delayed
- **Messages**: 10000 ok / 0 err
- **Throughput**: 1983.7 msg/s consumed
- **Latency**: p50=2500.35 p95=4751.53 p99=4951.17 max=5000.79 ms
- **Timing**: total=5.041s produce=0.042s drain=4.999s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-fanout-20260211_165117-ce7d677a
- **System**: kafka  **Workload**: fanout
- **Messages**: 5000 ok / 0 err
- **Throughput**: 9504.0 msg/s consumed
- **Latency**: p50=225.46 p95=409.75 p99=429.12 max=433.89 ms
- **Timing**: total=0.526s produce=0.093s drain=0.433s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10, fanout_factor=10

### kafka-fanout-20260211_165126-e0914eed
- **System**: kafka  **Workload**: fanout
- **Messages**: 5000 ok / 0 err
- **Throughput**: 9185.12 msg/s consumed
- **Latency**: p50=232.25 p95=435.6 p99=456.66 max=461.77 ms
- **Timing**: total=0.544s produce=0.083s drain=0.461s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10, fanout_factor=10

### kafka-fanout-20260211_165136-aa2b907c
- **System**: kafka  **Workload**: fanout
- **Messages**: 5000 ok / 0 err
- **Throughput**: 7623.32 msg/s consumed
- **Latency**: p50=320.03 p95=537.52 p99=558.48 max=563.45 ms
- **Timing**: total=0.656s produce=0.093s drain=0.563s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10, fanout_factor=10

### kafka-fanout-20260218_135047-83845b2e
- **System**: kafka  **Workload**: fanout
- **Messages**: 200 ok / 0 err
- **Throughput**: 184.14 msg/s consumed
- **Latency**: p50=56.87 p95=118.53 p99=1045.27 max=1045.72 ms
- **Timing**: total=1.086s produce=1.019s drain=0.067s
- **Config**: messages=200, warmup=0, partitions=12, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-fanout-20260218_145719-adbbcc75
- **System**: kafka  **Workload**: fanout
- **Messages**: 10000 ok / 0 err
- **Throughput**: 14867.4 msg/s consumed
- **Latency**: p50=220.6 p95=520.9 p99=552.11 max=562.44 ms
- **Timing**: total=0.673s produce=0.227s drain=0.446s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-fanout-20260218_145729-2eb69879
- **System**: kafka  **Workload**: fanout
- **Messages**: 10000 ok / 0 err
- **Throughput**: 21191.72 msg/s consumed
- **Latency**: p50=156.49 p95=315.84 p99=363.38 max=376.78 ms
- **Timing**: total=0.472s produce=0.164s drain=0.307s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-fanout-20260218_145739-28dcf69d
- **System**: kafka  **Workload**: fanout
- **Messages**: 10000 ok / 0 err
- **Throughput**: 13665.87 msg/s consumed
- **Latency**: p50=241.19 p95=522.02 p99=574.85 max=588.91 ms
- **Timing**: total=0.732s produce=0.194s drain=0.538s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-io_bound-20260211_163525-193e52c5
- **System**: kafka  **Workload**: io_bound
- **Messages**: 5000 ok / 0 err
- **Throughput**: 17.79 msg/s consumed
- **Latency**: p50=140397.76 p95=266959.91 p99=278200.51 max=281064.77 ms
- **Timing**: total=281.083s produce=0.02s drain=281.063s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10

### kafka-io_bound-20260211_164042-7e229350
- **System**: kafka  **Workload**: io_bound
- **Messages**: 5000 ok / 0 err
- **Throughput**: 17.72 msg/s consumed
- **Latency**: p50=140749.45 p95=267985.16 p99=279355.36 max=282180.41 ms
- **Timing**: total=282.197s produce=0.017s drain=282.179s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10

### kafka-io_bound-20260211_164601-84d12968
- **System**: kafka  **Workload**: io_bound
- **Messages**: 5000 ok / 0 err
- **Throughput**: 17.74 msg/s consumed
- **Latency**: p50=141133.83 p95=267700.78 p99=279016.69 max=281841.53 ms
- **Timing**: total=281.858s produce=0.018s drain=281.841s
- **Config**: messages=5000, warmup=500, partitions=6, concurrency=10

### kafka-io_bound-20260218_135042-2efb90ae
- **System**: kafka  **Workload**: io_bound
- **Messages**: 200 ok / 0 err
- **Throughput**: 133.21 msg/s consumed
- **Latency**: p50=1290.4 p95=1473.68 p99=1493.2 max=1498.27 ms
- **Timing**: total=1.501s produce=1.015s drain=0.486s
- **Config**: messages=200, warmup=0, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-io_bound-20260218_145559-4cf5e49d
- **System**: kafka  **Workload**: io_bound
- **Messages**: 10000 ok / 0 err
- **Throughput**: 621.78 msg/s consumed
- **Latency**: p50=8049.89 p95=15244.42 p99=15901.93 max=16052.8 ms
- **Timing**: total=16.083s produce=0.035s drain=16.048s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-io_bound-20260218_145626-11b2643e
- **System**: kafka  **Workload**: io_bound
- **Messages**: 10000 ok / 0 err
- **Throughput**: 573.29 msg/s consumed
- **Latency**: p50=8082.88 p95=16340.67 p99=17196.15 max=17404.42 ms
- **Timing**: total=17.443s produce=0.044s drain=17.399s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### kafka-io_bound-20260218_145654-23c6d89a
- **System**: kafka  **Workload**: io_bound
- **Messages**: 10000 ok / 0 err
- **Throughput**: 568.23 msg/s consumed
- **Latency**: p50=8040.72 p95=16514.93 p99=17357.59 max=17568.33 ms
- **Timing**: total=17.598s produce=0.038s drain=17.56s
- **Config**: messages=10000, warmup=500, partitions=12, concurrency=25, worker_threads=25, sidekiq_batch_size=100, kafka_compression=snappy, kafka_acks=1

### sidekiq-cpu_light-20260211_165227-e7c57771
- **System**: sidekiq  **Workload**: cpu_light
- **Messages**: 5000 ok / 0 err
- **Throughput**: 1001.6 msg/s consumed
- **Latency**: p50=2309.62 p95=4341.61 p99=4508.71 max=4551.29 ms
- **Timing**: total=4.992s produce=0.441s drain=4.551s
- **Config**: messages=5000, warmup=500, concurrency=10

### sidekiq-cpu_light-20260211_165238-c3387f6f
- **System**: sidekiq  **Workload**: cpu_light
- **Messages**: 5000 ok / 0 err
- **Throughput**: 1001.61 msg/s consumed
- **Latency**: p50=2368.1 p95=4324.48 p99=4491.82 max=4534.09 ms
- **Timing**: total=4.992s produce=0.459s drain=4.533s
- **Config**: messages=5000, warmup=500, concurrency=10

### sidekiq-cpu_light-20260211_165249-d688bccf
- **System**: sidekiq  **Workload**: cpu_light
- **Messages**: 5000 ok / 0 err
- **Throughput**: 1039.32 msg/s consumed
- **Latency**: p50=2203.19 p95=4150.1 p99=4324.56 max=4366.35 ms
- **Timing**: total=4.811s produce=0.445s drain=4.366s
- **Config**: messages=5000, warmup=500, concurrency=10

### sidekiq-cpu_light-20260218_135108-e90437b5
- **System**: sidekiq  **Workload**: cpu_light
- **Messages**: 200 ok / 0 err
- **Throughput**: 2458.07 msg/s consumed
- **Latency**: p50=53.69 p95=75.95 p99=78.11 max=78.96 ms
- **Timing**: total=0.081s produce=0.003s drain=0.078s
- **Config**: messages=200, warmup=0, concurrency=25, worker_threads=25, sidekiq_batch_size=100

### sidekiq-cpu_light-20260218_145845-608fa405
- **System**: sidekiq  **Workload**: cpu_light
- **Messages**: 10000 ok / 0 err
- **Throughput**: 3538.16 msg/s consumed
- **Latency**: p50=1402.61 p95=2597.09 p99=2699.86 max=2726.42 ms
- **Timing**: total=2.826s produce=0.1s drain=2.726s
- **Config**: messages=10000, warmup=500, concurrency=25, worker_threads=25, sidekiq_batch_size=100

### sidekiq-cpu_light-20260218_145855-830c84b3
- **System**: sidekiq  **Workload**: cpu_light
- **Messages**: 10000 ok / 0 err
- **Throughput**: 3640.2 msg/s consumed
- **Latency**: p50=1310.8 p95=2511.95 p99=2614.1 max=2640.77 ms
- **Timing**: total=2.747s produce=0.107s drain=2.64s
- **Config**: messages=10000, warmup=500, concurrency=25, worker_threads=25, sidekiq_batch_size=100

### sidekiq-cpu_light-20260218_145904-35b5afc6
- **System**: sidekiq  **Workload**: cpu_light
- **Messages**: 10000 ok / 0 err
- **Throughput**: 2991.85 msg/s consumed
- **Latency**: p50=1587.81 p95=3060.45 p99=3184.66 max=3216.71 ms
- **Timing**: total=3.342s produce=0.126s drain=3.216s
- **Config**: messages=10000, warmup=500, concurrency=25, worker_threads=25, sidekiq_batch_size=100

### sidekiq-delayed-20260211_165708-4d1221d7
- **System**: sidekiq  **Workload**: delayed
- **Messages**: 5000 ok / 0 err
- **Throughput**: 669.44 msg/s consumed
- **Latency**: p50=6017.8 p95=7042.02 p99=7111.0 max=7125.14 ms
- **Timing**: total=7.469s produce=0.344s drain=7.125s
- **Config**: messages=5000, warmup=500, concurrency=10, delay_seconds=5.0

### sidekiq-delayed-20260211_165722-fd8c8f9e
- **System**: sidekiq  **Workload**: delayed
- **Messages**: 5000 ok / 0 err
- **Throughput**: 572.34 msg/s consumed
- **Latency**: p50=7394.99 p95=8294.38 p99=8352.03 max=8365.46 ms
- **Timing**: total=8.736s produce=0.371s drain=8.365s
- **Config**: messages=5000, warmup=500, concurrency=10, delay_seconds=5.0

### sidekiq-delayed-20260211_165737-34319db3
- **System**: sidekiq  **Workload**: delayed
- **Messages**: 5000 ok / 0 err
- **Throughput**: 689.75 msg/s consumed
- **Latency**: p50=5892.45 p95=6816.62 p99=6884.55 max=6897.64 ms
- **Timing**: total=7.249s produce=0.352s drain=6.897s
- **Config**: messages=5000, warmup=500, concurrency=10, delay_seconds=5.0

### sidekiq-delayed-20260218_135115-64cf1e5b
- **System**: sidekiq  **Workload**: delayed
- **Messages**: 200 ok / 0 err
- **Throughput**: 20.13 msg/s consumed
- **Latency**: p50=9897.9 p95=9917.44 p99=9918.69 max=9918.92 ms
- **Timing**: total=9.935s produce=0.017s drain=9.919s
- **Config**: messages=200, warmup=0, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100

### sidekiq-delayed-20260218_150034-546a1ef3
- **System**: sidekiq  **Workload**: delayed
- **Messages**: 10000 ok / 0 err
- **Throughput**: 1605.33 msg/s consumed
- **Latency**: p50=3794.35 p95=5502.05 p99=5571.01 max=5587.53 ms
- **Timing**: total=6.229s produce=0.642s drain=5.587s
- **Config**: messages=10000, warmup=500, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100

### sidekiq-delayed-20260218_150047-2ade94d9
- **System**: sidekiq  **Workload**: delayed
- **Messages**: 10000 ok / 0 err
- **Throughput**: 1109.04 msg/s consumed
- **Latency**: p50=7838.27 p95=8333.11 p99=8371.43 max=8381.22 ms
- **Timing**: total=9.017s produce=0.636s drain=8.381s
- **Config**: messages=10000, warmup=500, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100

### sidekiq-delayed-20260218_150103-0734bda4
- **System**: sidekiq  **Workload**: delayed
- **Messages**: 10000 ok / 0 err
- **Throughput**: 1255.52 msg/s consumed
- **Latency**: p50=6114.93 p95=7149.35 p99=7244.27 max=7267.82 ms
- **Timing**: total=7.965s produce=0.697s drain=7.268s
- **Config**: messages=10000, warmup=500, concurrency=25, delay_seconds=5.0, worker_threads=25, sidekiq_batch_size=100

### sidekiq-fanout-20260211_165647-43978e8f
- **System**: sidekiq  **Workload**: fanout
- **Messages**: 5000 ok / 0 err
- **Throughput**: 2706.29 msg/s consumed
- **Latency**: p50=652.88 p95=1209.68 p99=1256.92 max=1269.29 ms
- **Timing**: total=1.848s produce=0.579s drain=1.269s
- **Config**: messages=5000, warmup=500, concurrency=10, fanout_factor=10

### sidekiq-fanout-20260211_165655-f2a0bce6
- **System**: sidekiq  **Workload**: fanout
- **Messages**: 5000 ok / 0 err
- **Throughput**: 2627.98 msg/s consumed
- **Latency**: p50=751.25 p95=1164.68 p99=1200.57 max=1213.27 ms
- **Timing**: total=1.903s produce=0.69s drain=1.212s
- **Config**: messages=5000, warmup=500, concurrency=10, fanout_factor=10

### sidekiq-fanout-20260211_165704-5a1ed8a6
- **System**: sidekiq  **Workload**: fanout
- **Messages**: 5000 ok / 0 err
- **Throughput**: 2989.98 msg/s consumed
- **Latency**: p50=578.63 p95=1093.12 p99=1143.75 max=1156.27 ms
- **Timing**: total=1.672s produce=0.517s drain=1.156s
- **Config**: messages=5000, warmup=500, concurrency=10, fanout_factor=10

### sidekiq-fanout-20260218_135112-6e489bda
- **System**: sidekiq  **Workload**: fanout
- **Messages**: 200 ok / 0 err
- **Throughput**: 6504.66 msg/s consumed
- **Latency**: p50=12.27 p95=19.8 p99=20.36 max=20.86 ms
- **Timing**: total=0.031s produce=0.01s drain=0.02s
- **Config**: messages=200, warmup=0, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100

### sidekiq-fanout-20260218_150016-96ed7333
- **System**: sidekiq  **Workload**: fanout
- **Messages**: 10000 ok / 0 err
- **Throughput**: 8806.21 msg/s consumed
- **Latency**: p50=424.31 p95=848.16 p99=891.9 max=907.32 ms
- **Timing**: total=1.136s produce=0.229s drain=0.906s
- **Config**: messages=10000, warmup=500, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100

### sidekiq-fanout-20260218_150023-d92ad336
- **System**: sidekiq  **Workload**: fanout
- **Messages**: 10000 ok / 0 err
- **Throughput**: 10081.0 msg/s consumed
- **Latency**: p50=399.99 p95=734.1 p99=764.69 max=773.22 ms
- **Timing**: total=0.992s produce=0.22s drain=0.772s
- **Config**: messages=10000, warmup=500, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100

### sidekiq-fanout-20260218_150030-cbab94d4
- **System**: sidekiq  **Workload**: fanout
- **Messages**: 10000 ok / 0 err
- **Throughput**: 9694.9 msg/s consumed
- **Latency**: p50=396.27 p95=770.57 p99=801.07 max=809.41 ms
- **Timing**: total=1.031s produce=0.222s drain=0.809s
- **Config**: messages=10000, warmup=500, concurrency=25, fanout_factor=10, worker_threads=25, sidekiq_batch_size=100

### sidekiq-io_bound-20260211_165257-7d9cd65e
- **System**: sidekiq  **Workload**: io_bound
- **Messages**: 5000 ok / 0 err
- **Throughput**: 76.24 msg/s consumed
- **Latency**: p50=32600.77 p95=61947.95 p99=64579.74 max=65207.6 ms
- **Timing**: total=65.585s produce=0.377s drain=65.208s
- **Config**: messages=5000, warmup=500, concurrency=10

### sidekiq-io_bound-20260211_165415-9f618dcc
- **System**: sidekiq  **Workload**: io_bound
- **Messages**: 5000 ok / 0 err
- **Throughput**: 76.21 msg/s consumed
- **Latency**: p50=32612.31 p95=61894.87 p99=64518.36 max=65206.55 ms
- **Timing**: total=65.61s produce=0.403s drain=65.206s
- **Config**: messages=5000, warmup=500, concurrency=10

### sidekiq-io_bound-20260211_165533-e1638085
- **System**: sidekiq  **Workload**: io_bound
- **Messages**: 5000 ok / 0 err
- **Throughput**: 76.13 msg/s consumed
- **Latency**: p50=32615.01 p95=61950.07 p99=64579.72 max=65208.65 ms
- **Timing**: total=65.675s produce=0.467s drain=65.208s
- **Config**: messages=5000, warmup=500, concurrency=10

### sidekiq-io_bound-20260218_135110-2399f036
- **System**: sidekiq  **Workload**: io_bound
- **Messages**: 200 ok / 0 err
- **Throughput**: 546.77 msg/s consumed
- **Latency**: p50=209.39 p95=317.88 p99=363.26 max=363.46 ms
- **Timing**: total=0.366s produce=0.003s drain=0.363s
- **Config**: messages=200, warmup=0, concurrency=25, worker_threads=25, sidekiq_batch_size=100

### sidekiq-io_bound-20260218_145910-e0854de6
- **System**: sidekiq  **Workload**: io_bound
- **Messages**: 10000 ok / 0 err
- **Throughput**: 621.9 msg/s consumed
- **Latency**: p50=8026.78 p95=15171.59 p99=15830.68 max=15985.33 ms
- **Timing**: total=16.08s produce=0.095s drain=15.985s
- **Config**: messages=10000, warmup=500, concurrency=25, worker_threads=25, sidekiq_batch_size=100

### sidekiq-io_bound-20260218_145933-b242d875
- **System**: sidekiq  **Workload**: io_bound
- **Messages**: 10000 ok / 0 err
- **Throughput**: 621.88 msg/s consumed
- **Latency**: p50=8017.35 p95=15172.16 p99=15829.9 max=15984.27 ms
- **Timing**: total=16.08s produce=0.096s drain=15.984s
- **Config**: messages=10000, warmup=500, concurrency=25, worker_threads=25, sidekiq_batch_size=100

### sidekiq-io_bound-20260218_145956-80bf6882
- **System**: sidekiq  **Workload**: io_bound
- **Messages**: 10000 ok / 0 err
- **Throughput**: 621.34 msg/s consumed
- **Latency**: p50=8024.74 p95=15221.33 p99=15851.96 max=16007.51 ms
- **Timing**: total=16.094s produce=0.087s drain=16.007s
- **Config**: messages=10000, warmup=500, concurrency=25, worker_threads=25, sidekiq_batch_size=100
