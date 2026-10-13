"""Touch every page of a fixed buffer and print how many passes finished.

Used only inside a disposable systemd scope. Every page is read and written,
so a MemoryHigh below the buffer size has to fault those pages back in.
That is the throughput cost. The buffer stays allocated for the whole run.
"""

import sys
import time

mb = int(sys.argv[1])
secs = float(sys.argv[2])
buf = bytearray(mb * 1024 * 1024)
end = time.monotonic() + secs
rounds = 0
# One byte per page. A coarser stride leaves pages cold, and MemoryHigh can
# reclaim them without the loop ever noticing.
step = 4096
while time.monotonic() < end:
    total = 0
    for i in range(0, len(buf), step):
        buf[i] = (buf[i] + 1) & 255
        total += buf[i]
    # Keep the read live so a run cannot delete the loop.
    if total < 0:
        break
    rounds += 1
print(rounds)
