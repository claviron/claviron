#!/usr/bin/env bash

# Watch claviron - master and worker memory (copied from claviron-full's watch.claviron.sh)
# This script displays RSS memory and AnonHugePages for the claviron master process and its workers

set -euo pipefail

# Configuration
REFRESH_INTERVAL="${1:-5}"  # Default 5 seconds, can be overridden with first argument
PROCESS_NAME="${PROCESS_NAME:-edge-server}"
SHOW_MASTER="${SHOW_MASTER:-1}"  # Set to 0 to hide master, or use: SHOW_MASTER=0 ./watch.edge.sh

# Colors for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Function to display the monitoring data
monitor_workers() {
    clear

    # Find the master process
    MASTER=$(pgrep -xo "$PROCESS_NAME" 2>/dev/null || true)

    if [[ -z "$MASTER" ]]; then
        echo -e "${RED}${BOLD}Error: No '$PROCESS_NAME' master process found${NC}"
        echo "Make sure $PROCESS_NAME is running."
        return 1
    fi

    # Get Claviron worker children. Other app-owned child processes are not
    # included; the supervisor labels worker argv[0] as `[worker #N]`.
    PIDS=""
    for candidate in $(ps --ppid "$MASTER" -o pid= 2>/dev/null || true); do
        cmdline=$(tr '\0' ' ' < "/proc/$candidate/cmdline" 2>/dev/null || true)
        if [[ "$cmdline" == *"[worker #"*"]"* ]]; then
            PIDS="${PIDS} ${candidate}"
        fi
    done

    # Header
    echo -e "${CYAN}${BOLD}╔════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║${NC}  ${BOLD}Claviron Worker Monitor${NC}                                      ${CYAN}${BOLD}║${NC}"
    echo -e "${CYAN}${BOLD}╚════════════════════════════════════════════════════════════════════╝${NC}"
    echo -e "${YELLOW}Time:${NC} $(date '+%Y-%m-%d %H:%M:%S')"
    echo -e "${YELLOW}Master PID:${NC} $MASTER"
    echo -e "${YELLOW}Worker Count:${NC} $(echo $PIDS | wc -w | tr -d ' ')"
    echo ""

    # Table header
    echo -e "${BOLD}┌────────┬──────────┬──────┬──────────┬─────────────┬─────────────┬──────────┐${NC}"
    printf "${BOLD}│ %6s │ %8s │ %4s │ %8s │ %11s │ %11s │ %8s │${NC}\n" "PID" "Worker" "%CPU" "Uptime" "RSS_KB" "Private_KB" "AnonHP"
    echo -e "${BOLD}├────────┼──────────┼──────┼──────────┼─────────────┼─────────────┼──────────┤${NC}"

    # Process data
    total_rss=0
    total_private=0
    total_ahp=0

    # Master process (minimal display) - optional
    if [[ "$SHOW_MASTER" == "1" ]] && [[ -d "/proc/$MASTER" ]]; then
        read rss cpu etime < <(ps -o rss=,%cpu=,etime= -p "$MASTER" 2>/dev/null || echo "0 0 ?")
        etime=$(echo "$etime" | xargs)  # trim whitespace
        if [[ -r "/proc/$MASTER/smaps_rollup" ]]; then
            private=$(awk '/^Private_/{sum+=$2} END{print sum+0}' "/proc/$MASTER/smaps_rollup" 2>/dev/null || echo "0")
            ahp=$(awk '/^AnonHugePages:/{print $2}' "/proc/$MASTER/smaps_rollup" 2>/dev/null || echo "0")
        else
            private=0
            ahp=0
        fi

        printf "${GREEN}│ %6d │ %8s │ %4.1f │ %8s │ %11s │ %11s │ %8s │${NC}\n" \
            "$MASTER" "master" "$cpu" "$etime" "$(printf "%'d" "$rss")" "$(printf "%'d" "$private")" "$(printf "%'d" "$ahp")"

        total_rss=$((total_rss + rss))
        total_private=$((total_private + private))
        total_ahp=$((total_ahp + ahp))
    fi

    # Worker processes (detailed)
    for p in $PIDS; do
        # Check if process still exists
        if [[ ! -d "/proc/$p" ]]; then
            continue
        fi

        # Worker identity is a display label only. Runtime identity comes from
        # the supervisor's validated private environment bootstrap.
        worker_id=$(tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | sed -n 's/.*\[worker #\([0-9][0-9]*\)\].*/\1/p' | head -1)
        worker_id="${worker_id:-?}"

        # Get memory, CPU, and uptime info from ps
        read rss cpu etime < <(ps -o rss=,%cpu=,etime= -p "$p" 2>/dev/null || echo "0 0 ?")
        etime=$(echo "$etime" | xargs)  # trim whitespace

        # Get detailed memory from smaps_rollup
        if [[ -r "/proc/$p/smaps_rollup" ]]; then
            private=$(awk '/^Private_/{sum+=$2} END{print sum+0}' "/proc/$p/smaps_rollup" 2>/dev/null || echo "0")
            ahp=$(awk '/^AnonHugePages:/{print $2}' "/proc/$p/smaps_rollup" 2>/dev/null || echo "0")
        else
            private=0
            ahp=0
        fi

        # Format numbers with thousand separators
        rss_formatted=$(printf "%'d" "$rss")
        private_formatted=$(printf "%'d" "$private")
        ahp_formatted=$(printf "%'d" "$ahp")

        # Color based on memory usage
        color_start="${NC}"
        color_end="${NC}"

        if [[ "$ahp" -gt 51200 ]]; then
            # High AnonHP (>50MB)
            color_start="${RED}"
        elif [[ "$ahp" -gt 0 ]]; then
            color_start="${YELLOW}"
        fi

        printf "${color_start}│ %6d │ worker#%-2s │ %4.1f │ %8s │ %11s │ %11s │ %8s │${color_end}\n" \
            "$p" "$worker_id" "$cpu" "$etime" "$rss_formatted" "$private_formatted" "$ahp_formatted"

        # Add to totals
        total_rss=$((total_rss + rss))
        total_private=$((total_private + private))
        total_ahp=$((total_ahp + ahp))
    done

    # Table footer
    echo -e "${BOLD}├────────┴──────────┴──────┴──────────┴─────────────┴─────────────┴──────────┤${NC}"

    # Format totals with thousand separators
    total_rss_formatted=$(printf "%'d" "$total_rss")
    total_private_formatted=$(printf "%'d" "$total_private")
    total_ahp_formatted=$(printf "%'d" "$total_ahp")

    # Calculate in MB
    total_rss_mb=$(awk "BEGIN {printf \"%.2f\", $total_rss/1024}")
    total_private_mb=$(awk "BEGIN {printf \"%.2f\", $total_private/1024}")
    total_ahp_mb=$(awk "BEGIN {printf \"%.2f\", $total_ahp/1024}")

    # Color totals based on leak indicators
    ahp_color="${NC}"
    if [[ "$total_ahp" -gt 0 ]]; then
        ahp_color="${YELLOW}"
    fi

    printf "${BOLD}│ ${BLUE}RSS:${NC} ${BOLD}%s KB (%s MB)${NC} │ ${BLUE}Private:${NC} ${BOLD}%s KB (%s MB)${NC} ${BOLD}│${NC}\n" \
        "$total_rss_formatted" "$total_rss_mb" "$total_private_formatted" "$total_private_mb"
    printf "${BOLD}│ ${BLUE}AnonHugePages:${NC} ${ahp_color}${BOLD}%s KB (%s MB)${NC} - Leak indicator ${BOLD}│${NC}\n" \
        "$total_ahp_formatted" "$total_ahp_mb"
    echo -e "${BOLD}└────────────────────────────────────────────────────────────────────────┘${NC}"

    echo ""
    echo -e "${YELLOW}Refresh interval: ${REFRESH_INTERVAL}s | Press Ctrl+C to exit${NC}"
}

# Main loop
echo -e "${CYAN}Starting Claviron worker monitor...${NC}"
echo -e "${CYAN}Refresh interval: ${REFRESH_INTERVAL} seconds${NC}"
if [[ "$SHOW_MASTER" == "0" ]]; then
    echo -e "${CYAN}Mode: Workers only (master hidden)${NC}"
fi
echo ""

# Check if we're on Linux (this script requires /proc filesystem)
if [[ ! -d "/proc" ]]; then
    echo -e "${RED}Error: This script requires a Linux system with /proc filesystem${NC}"
    exit 1
fi

# Trap Ctrl+C for clean exit
trap 'echo -e "\n${GREEN}Monitor stopped.${NC}"; exit 0' INT

# Run the monitor loop
while true; do
    if ! monitor_workers; then
        sleep "$REFRESH_INTERVAL"
        continue
    fi
    sleep "$REFRESH_INTERVAL"
done
