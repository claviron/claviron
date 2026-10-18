#!/usr/bin/env bash
# Portable PHP-FPM pool watcher for Claviron (or any FastCGI client host)
# Shows per-pool active/idle/total, listen queue, slow req, accepted, max_active,
# max_children_reached and sums RSS/Private/AnonHugePages of pool workers.
# Requires: bash, /proc, and (recommended) cgi-fcgi, jq
#
#  - Watch for LQ (listen queue) - if it goes above 0, increase workers
#  - Watch MaxChHit (max children reached) - if it increases, you're hitting pm.max_children limit
#
#  - Accepted - Total requests processed since pool started
#  - MaxActive - Peak concurrent active processes (129 in your case)
#  - MaxChHit - How many times you hit pm.max_children limit (0 = good!)
#  - Slow - Slow requests (took longer than request_slowlog_timeout)
#
#  To see request rate:
#  Watch the script for a minute - the Accepted counter increases in real-time. For example:
#  - If it goes from 10,993 → 11,053 in 60 seconds = 1 req/sec
#
#  Note: This counter resets to 0 when you restart PHP-FPM:
#  systemctl restart php-fpm
set -euo pipefail

INTERVAL=5
PATH_TPL="/status"                 # all pools share the same /status path
DEBUG=0

have() { command -v "$1" > /dev/null 2>&1; }

detect_socket_dir() {
	if [[ -d /run/php-fpm ]]; then
		echo "/run/php-fpm"
	elif [[ -d /run/php ]]; then
		echo "/run/php"
	else
		echo "/run/php-fpm"
	fi
}

SOCK_DIR="$(detect_socket_dir)"

# Strip CGI headers (e.g., "Status: 200 OK", "Content-Type: ...")
# Keep body starting after the first blank line.
# Handles both LF and CRLF line endings.
strip_cgi_headers() {
	awk 'BEGIN{hdr=1} {gsub(/\r$/,""); if(hdr){if($0=="") {hdr=0; next} else next} else print}'
}

usage() {
	cat << USAGE
Usage: $(basename "$0") [-i SECS] [-P PATH_TEMPLATE] [-d] pool [pool2 ...]
  -i   refresh interval seconds (default: 5)
  -P   status path template (default: /status)
  -d   debug mode (show detailed diagnostics and exit)

Socket path is auto-detected:
  - /run/php-fpm/<pool>.sock if /run/php-fpm exists
  - /run/php/<pool>.sock if /run/php exists

Examples:
  $(basename "$0") www
  $(basename "$0") -i 2 webmail developer
  $(basename "$0") -d webmail    # diagnose issues with webmail pool

Troubleshooting:
  1. If socket not found: Check if pool is running (systemctl status php-fpm)
  2. If permission denied: Run with sudo or add user to php-fpm group
  3. If status page disabled: Add 'pm.status_path = /status' to pool config
  4. If cgi-fcgi not found: Install fcgi package (yum install fcgi)
USAGE
}

# -------- parse args --------
while getopts ":i:P:dh" opt; do
	case "$opt" in
		i) INTERVAL="$OPTARG" ;;
		P) PATH_TPL="$OPTARG" ;;
		d) DEBUG=1 ;;
		h)
			usage
			exit    0
			;;
		\?)
			echo "Unknown option: -$OPTARG" >&2
			usage
			exit                                          2
			;;
		:)
			echo "Option -$OPTARG requires an argument." >&2
			usage
			exit                                                       2
			;;
	esac
done
shift $((OPTIND - 1))

if [[ $# -lt 1 ]]; then
	usage
	exit 2
fi

POOLS=("$@")

sock_for() { printf "%s/%s.sock" "$SOCK_DIR" "$1"; }
path_for() { echo "$PATH_TPL"; }

fetch_status() {
	local sock="$1" path="$2" qs="${3:-json}"

	# Check if socket exists
	if [[ ! -S "$sock" ]]; then
		echo "ERROR:SOCKET_NOT_FOUND:$sock" >&2
		return 1
	fi

	# Check if socket is readable
	if [[ ! -r "$sock" ]]; then
		echo "ERROR:PERMISSION_DENIED:$sock" >&2
		return 1
	fi

	if have cgi-fcgi; then
		local output err_file
		err_file=$(mktemp)
		output=$(SCRIPT_NAME="$path" SCRIPT_FILENAME="$path" REQUEST_METHOD=GET QUERY_STRING="$qs" \
			cgi-fcgi -bind -connect "$sock" 2> "$err_file" | strip_cgi_headers)
		local ret=$?
		if [[ $ret -ne 0 ]]; then
			local err_msg
			err_msg=$(cat "$err_file" 2> /dev/null || echo "unknown error")
			rm -f "$err_file"
			echo "ERROR:CGI_FCGI_FAILED:$err_msg" >&2
			return 1
		fi
		rm -f "$err_file"
		echo "$output"
	else
		echo "ERROR:CGI_FCGI_NOT_FOUND" >&2
		return 1
	fi
}

parse_status_to_kv() {
	local body="$1"
	if have jq; then
		if printf '%s' "$body" | jq -e . > /dev/null 2>&1; then
			printf '%s' "$body" \
				| jq -r 'to_entries[] | "\(.key)=\(.value)"' 2> /dev/null \
				| sed 's/ /_/g'
			return
		fi
	fi
	sed -n 's/^\([^:]\+\):[[:space:]]\{1,\}\(.*\)$/\1=\2/p' <<< "$body" | sed 's/ /_/g'
}

# Sum memory for all php-fpm workers of a given pool name
pool_mem_stats() {
	local pool="$1"
	local pids rs pr ah
	pids=$(pgrep -f "php-fpm: pool ${pool}" || true)
	local count=0 rss=0 private=0 ahp=0
	for p in $pids; do
		[[ -d /proc/$p ]] || continue
		rs=$(awk '/Rss:/ {print $2}' /proc/$p/smaps_rollup 2> /dev/null || echo 0)
		pr=$(awk '/^Private_/ {s+=$2} END{print s+0}' /proc/$p/smaps_rollup 2> /dev/null || echo 0)
		ah=$(awk '/^AnonHugePages:/ {print $2}' /proc/$p/smaps_rollup 2> /dev/null || echo 0)
		rss=$((rss + rs))
		private=$((private + pr))
		ahp=$((ahp + ah))
		count=$((count + 1))
	done
	echo "$count $rss $private $ahp"
}

kb_to_mb() { awk -v k="$1" 'BEGIN{printf "%.2f", k/1024.0}'; }

divider() { printf '%s\n' "────────────────────────────────────────────────────────────────────────────"; }

render_header() {
	clear
	echo "Claviron PHP-FPM Watcher"
	echo "Time: $(date '+%Y-%m-%d %H:%M:%S')   Interval: ${INTERVAL}s"
	divider
	printf "%-20s %4s %5s %6s %3s %5s %5s %10s %10s %10s %10s  | %s\n" \
		"Pool" "Act" "Idle" "Total" "LQ" "LQmax" "Slow" "Accepted" "MaxActive" "MaxChHit" "MaxChild" "workers/RSS/Priv/AHP"
}

render_pool_line() {
	local name="$1" act="$2" idle="$3" total="$4" lq="$5" lqmax="$6" slow="$7" acc="$8" maxact="$9" maxhit="${10}" maxch="${11}" wcount="${12}" rss="${13}" priv="${14}" ahp="${15}"
	printf "%-20s %4s %5s %6s %3s %5s %5s %10s %10s %10s %10s  | %3s / %7.2fMB / %7.2fMB / %7.2fMB\n" \
		"$name" "$act" "$idle" "$total" "$lq" "$lqmax" "$slow" "$acc" "$maxact" "$maxhit" "$maxch" \
		"$wcount" "$(kb_to_mb "$rss")" "$(kb_to_mb "$priv")" "$(kb_to_mb "$ahp")"
}

render_footer() {
	local sum_act="$1" sum_idle="$2" sum_total="$3" sum_rss="$4" sum_priv="$5" sum_ahp="$6"
	divider
	printf "TOTAL   Active=%s  Idle=%s  Total=%s  |  RSS=%0.2fMB  Private=%0.2fMB  AnonHP=%0.2fMB\n" \
		"$sum_act" "$sum_idle" "$sum_total" "$(kb_to_mb "$sum_rss")" "$(kb_to_mb "$sum_priv")" "$(kb_to_mb "$sum_ahp")"
	echo "Ctrl+C to exit."
}

diagnose_pool() {
	local name="$1"
	local sock path
	sock="$(sock_for "$name")"
	path="/status"

	echo "=== Diagnostics for pool: $name ==="
	echo ""
	echo "Socket: $sock"
	echo "Status Path: $path"
	echo ""

	echo "Checks:"
	echo -n "  1. Socket exists... "
	if [[ -S "$sock" ]]; then
		echo "✓ YES"
	else
		echo "✗ NO"
		echo "  → Check if pool is running: systemctl status php-fpm"
		exit 1
	fi

	echo -n "  2. Socket readable... "
	if [[ -r "$sock" ]]; then
		echo "✓ YES"
	else
		echo "✗ NO"
		ls -la "$sock"
		echo "  → Run as root: sudo $0 -d $name"
		exit 1
	fi

	echo -n "  3. cgi-fcgi installed... "
	if have cgi-fcgi; then
		echo "✓ YES"
	else
		echo "✗ NO"
		echo "  → Install: sudo yum install fcgi"
		exit 1
	fi

	echo -n "  4. Fetching status... "
	body="$(fetch_status "$sock" "$path" json 2>&1 || true)"
	if [[ "$body" == ERROR:* ]]; then
		echo "✗ FAILED"
		echo ""
		echo "Error: $body"
		echo ""
		echo "Testing plain text format..."
		body_plain="$(fetch_status "$sock" "$path" "" 2>&1 || true)"
		if [[ "$body_plain" == ERROR:* ]]; then
			echo "✗ Status endpoint not responding"
			echo ""
			echo "Make sure pool config has: pm.status_path = /status"
		else
			echo "✓ Plain text works:"
			echo "$body_plain" | head -c 300
		fi
		exit 1
	else
		echo "✓ YES"
		echo ""
		echo "Status response:"
		echo "$body" | head -c 500
		echo ""
		echo ""
		echo "✓ All checks passed!"
	fi
}

main_loop() {
	while :; do
		render_header
		local g_act=0 g_idle=0 g_total=0 g_rss=0 g_priv=0 g_ahp=0

		for name in "${POOLS[@]}"; do
			sock="$(sock_for "$name")"
			path="$(path_for "$name")"

			# Try JSON format first
			body="$(fetch_status "$sock" "$path" json 2>&1 || true)"

			# Check for specific errors
			if [[ "$body" == ERROR:* ]]; then
				local err_type="${body#ERROR:}"
				err_type="${err_type%%:*}"
				local err_detail="${body#*:*:}"

				case "$err_type" in
					SOCKET_NOT_FOUND)
						printf "%-20s  ✗ Socket not found: %s\n" "$name" "$sock"
						;;
					PERMISSION_DENIED)
						printf "%-20s  ✗ Permission denied: %s (try: sudo or usermod -aG www-data \$USER)\n" "$name" "$sock"
						;;
					CGI_FCGI_NOT_FOUND)
						printf "%-20s  ✗ cgi-fcgi not installed (yum install fcgi)\n" "$name"
						;;
					CGI_FCGI_FAILED)
						printf "%-20s  ✗ Connection failed: %s\n" "$name" "$err_detail"
						;;
					*)
						printf "%-20s  ✗ Unknown error: %s\n" "$name" "$body"
						;;
				esac
				continue
			fi

			# Try plain text format if JSON failed
			if [[ -z "$body" ]]; then
				body="$(fetch_status "$sock" "$path" "" 2>&1 || true)"
				if [[ "$body" == ERROR:* ]]; then
					printf "%-20s  ✗ Status page disabled? Check pm.status_path in pool config\n" "$name"
					continue
				fi
			fi

			if [[ -z "$body" ]]; then
				printf "%-20s  ✗ No response from socket\n" "$name"
				continue
			fi

			kv="$(parse_status_to_kv "$body")"
			act=0 idle=0 total=0 lq=0 lqmax=0 slow=0 acc=0 maxact=0 maxhit=0 maxch=0
			while IFS='=' read -r k v; do
				case "$k" in
					active_processes)  act="$v" ;;
					idle_processes)    idle="$v" ;;
					total_processes)   total="$v" ;;
					listen_queue)      lq="$v" ;;
					max_listen_queue)  lqmax="$v" ;;
					slow_requests)     slow="$v" ;;
					accepted_conn)     acc="$v" ;;
					max_active_processes) maxact="$v" ;;
					max_children_reached) maxhit="$v" ;;
					pm.max_children | max_children) maxch="$v" ;;
				esac
			done <<< "$kv"

			read wcount rss priv ahp < <(pool_mem_stats "$name")

			g_act=$((g_act + act))
			g_idle=$((g_idle + idle))
			g_total=$((g_total + total))
			g_rss=$((g_rss + rss))
			g_priv=$((g_priv + priv))
			g_ahp=$((g_ahp + ahp))

			render_pool_line "$name" "$act" "$idle" "$total" "$lq" "$lqmax" "$slow" "$acc" "$maxact" "$maxhit" "$maxch" "$wcount" "$rss" "$priv" "$ahp"
		done

		render_footer "$g_act" "$g_idle" "$g_total" "$g_rss" "$g_priv" "$g_ahp"
		sleep "$INTERVAL"
		clear
	done
}

# Helpful notes if deps are missing
if ! have cgi-fcgi; then
	echo "Note: 'cgi-fcgi' not found. Install package 'fcgi' for status queries." >&2
fi
if ! have jq; then
	echo "Note: 'jq' not found. Falling back to plain-text parser." >&2
fi

# Run debug mode or normal mode
if [[ $DEBUG -eq 1 ]]; then
	for pool in "${POOLS[@]}"; do
		diagnose_pool "$pool"
		echo ""
	done
else
	main_loop
fi
