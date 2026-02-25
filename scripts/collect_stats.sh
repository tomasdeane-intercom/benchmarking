#!/usr/bin/env bash
#
# Collects Docker container resource stats (CPU%, MEM) at 1-second intervals
# and writes them to a CSV file.
#
# Usage:
#   ./scripts/collect_stats.sh <output_file> [container_filter...]
#   ./scripts/collect_stats.sh results/stats.csv kafka-consumer sidekiq-worker
#
# Send SIGTERM or SIGINT to stop collection.
set -euo pipefail

OUTPUT="${1:?Usage: $0 <output.csv> [container_names...]}"
shift
FILTERS=("$@")

echo "timestamp,container,cpu_pct,mem_usage_mb,mem_limit_mb,mem_pct,net_in_mb,net_out_mb" > "$OUTPUT"

trap 'echo "Stats collection stopped ($(wc -l < "$OUTPUT") rows → $OUTPUT)"; exit 0' INT TERM

while true; do
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}}' 2>/dev/null | while IFS=',' read -r name cpu mem mem_pct net; do
        # Apply filter if specified
        if [ ${#FILTERS[@]} -gt 0 ]; then
            match=false
            for f in "${FILTERS[@]}"; do
                if [[ "$name" == *"$f"* ]]; then match=true; break; fi
            done
            $match || continue
        fi

        # Parse CPU (remove %)
        cpu_val="${cpu//%/}"

        # Parse memory: "123.4MiB / 7.789GiB" → usage_mb, limit_mb
        mem_usage=$(echo "$mem" | sed 's/ \/ .*//' | sed 's/[[:space:]]//g')
        mem_limit=$(echo "$mem" | sed 's/.* \/ //' | sed 's/[[:space:]]//g')
        usage_mb=$(echo "$mem_usage" | awk '{
            if (index($0,"GiB")) { gsub(/GiB/,"",$0); printf "%.1f", $0*1024 }
            else if (index($0,"MiB")) { gsub(/MiB/,"",$0); printf "%.1f", $0 }
            else if (index($0,"KiB")) { gsub(/KiB/,"",$0); printf "%.1f", $0/1024 }
            else { gsub(/B/,"",$0); printf "%.1f", $0/1048576 }
        }')
        limit_mb=$(echo "$mem_limit" | awk '{
            if (index($0,"GiB")) { gsub(/GiB/,"",$0); printf "%.1f", $0*1024 }
            else if (index($0,"MiB")) { gsub(/MiB/,"",$0); printf "%.1f", $0 }
            else if (index($0,"KiB")) { gsub(/KiB/,"",$0); printf "%.1f", $0/1024 }
            else { gsub(/B/,"",$0); printf "%.1f", $0/1048576 }
        }')
        mem_pct_val="${mem_pct//%/}"

        # Parse net I/O: "1.23MB / 4.56MB"
        net_in=$(echo "$net" | sed 's/ \/ .*//' | sed 's/[[:space:]]//g')
        net_out=$(echo "$net" | sed 's/.* \/ //' | sed 's/[[:space:]]//g')
        net_in_mb=$(echo "$net_in" | awk '{
            if (index($0,"GB")) { gsub(/GB/,"",$0); printf "%.2f", $0*1000 }
            else if (index($0,"MB")) { gsub(/MB/,"",$0); printf "%.2f", $0 }
            else if (index($0,"kB")) { gsub(/kB/,"",$0); printf "%.2f", $0/1000 }
            else { gsub(/B/,"",$0); printf "%.2f", $0/1000000 }
        }')
        net_out_mb=$(echo "$net_out" | awk '{
            if (index($0,"GB")) { gsub(/GB/,"",$0); printf "%.2f", $0*1000 }
            else if (index($0,"MB")) { gsub(/MB/,"",$0); printf "%.2f", $0 }
            else if (index($0,"kB")) { gsub(/kB/,"",$0); printf "%.2f", $0/1000 }
            else { gsub(/B/,"",$0); printf "%.2f", $0/1000000 }
        }')

        echo "$ts,$name,$cpu_val,$usage_mb,$limit_mb,$mem_pct_val,$net_in_mb,$net_out_mb"
    done >> "$OUTPUT"
    sleep 1
done
