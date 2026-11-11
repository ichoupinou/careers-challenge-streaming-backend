"""
Follow the alarm feed (`GET /alarms/stream`) like a real consumer and check
that it never misses or repeats an alarm, including across a service
restart.

alarm_id is dense (1, 2, 3, ...), so a correct feed delivers every id after
the starting cursor exactly once and in order. On disconnect this
reconnects with `Last-Event-ID`, as a browser EventSource would.

Run until SIGTERM/SIGINT (or --duration), then print a JSON summary and exit
non-zero on any gap or duplicate.

    python3 scripts/sse_check.py --target http://localhost:8080 --out summary.json

Stdlib only.
"""

import argparse
import json
import signal
import sys
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


class Stop(Exception):
    pass


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--target", default="http://localhost:8080")
    p.add_argument("--since", type=int, default=0, help="start after this alarm_id")
    p.add_argument("--duration", type=float, default=0, help="seconds; 0 = until signalled")
    p.add_argument("--out", help="also write the summary JSON here")
    args = p.parse_args()

    def on_signal(*_):
        raise Stop()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    if args.duration > 0:
        signal.signal(signal.SIGALRM, on_signal)
        signal.setitimer(signal.ITIMER_REAL, args.duration)

    last = args.since
    received = 0
    gaps, duplicates = [], 0
    reconnects, disconnected_at, downtime = 0, None, 0.0

    try:
        while True:
            req = Request(f"{args.target.rstrip('/')}/alarms/stream",
                          headers={"Last-Event-ID": str(last), "Accept": "text/event-stream"})
            try:
                with urlopen(req, timeout=30) as resp:
                    if disconnected_at is not None:
                        downtime += time.monotonic() - disconnected_at
                        disconnected_at = None
                    for raw in resp:
                        line = raw.decode().rstrip("\n")
                        if not line.startswith("id:"):
                            continue
                        alarm_id = int(line[3:].strip())
                        if alarm_id <= last:
                            duplicates += 1
                            continue
                        if alarm_id != last + 1:
                            gaps.append([last + 1, alarm_id - 1])
                        last = alarm_id
                        received += 1
            except (URLError, OSError, ValueError):
                pass
            # Stream ended or the service is down: reconnect from `last`.
            if disconnected_at is None:
                disconnected_at = time.monotonic()
            reconnects += 1
            time.sleep(0.1)
    except Stop:
        pass

    summary = {
        "since": args.since,
        "received": received,
        "last_alarm_id": last,
        "gaps": gaps,
        "duplicates": duplicates,
        "reconnects": reconnects,
        "disconnected_seconds": round(downtime, 3),
        "ok": not gaps and duplicates == 0,
    }
    text = json.dumps(summary)
    print(text)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
