#!/bin/sh
# A deliberately hostile job. It tries the things a malicious submitter would try and reports whether the
# sandbox stopped each one. Every line should say BLOCKED; a line that says ESCAPED means the sandbox failed.
# Exit status is 0 only if everything was blocked.

failed=0
blocked() { echo "BLOCKED  $1"; }
escaped() { echo "ESCAPED  $1"; failed=1; }

# 1. Run as root?
if [ "$(id -u)" = "0" ]; then escaped "running as root"; else blocked "not root (uid $(id -u))"; fi

# 2. Capabilities (should be empty: all dropped).
cap=$(grep CapEff /proc/self/status | awk '{print $2}')
if [ "$cap" = "0000000000000000" ]; then blocked "no capabilities (CapEff=$cap)"; else escaped "capabilities present (CapEff=$cap)"; fi

# 3. Write to the root filesystem (read-only).
if touch /pwned 2>/dev/null; then escaped "wrote to the root filesystem"; else blocked "root filesystem is read-only"; fi

# 4. Reach the Docker daemon.
if [ -S /var/run/docker.sock ] || [ -e /run/docker.sock ]; then escaped "docker socket is visible"; else blocked "no docker socket"; fi

# 5. Reach the network (default: none).
if wget -T 3 -q -O /dev/null http://1.1.1.1 2>/dev/null; then escaped "reached the internet"; else blocked "no network access"; fi
if wget -T 3 -q -O /dev/null http://169.254.169.254/latest/meta-data/ 2>/dev/null; then escaped "reached the cloud metadata service"; else blocked "cloud metadata service unreachable"; fi

# 6. See the host's processes or files.
if [ "$(ls /proc | grep -c '^[0-9]')" -gt 20 ]; then escaped "sees many host processes"; else blocked "own PID namespace ($(ls /proc | grep -c '^[0-9]') processes visible)"; fi
if [ -e /home/adhyan ] || [ -e /host ]; then escaped "sees host paths"; else blocked "no host paths visible"; fi

# 7. Fork bomb, bounded: a child shell tries to start 600 processes; the PID limit must stop it long before that.
# Once the limit is hit nothing in this container can fork, not even this script, so the attack runs in a child
# that records how far it got with a shell builtin (no fork needed), and we read the result only after the
# short-lived sleepers have exited (the command substitution waits for them to close the pipe).
rm -f /tmp/forks
: $(sh -c 'i=0; while [ $i -lt 600 ]; do echo $i > /tmp/forks; sleep 4 & i=$((i + 1)); done' 2>&1)
reached=$(cat /tmp/forks 2>/dev/null)
if [ -z "$reached" ]; then escaped "the fork test produced no result"
elif [ "$reached" -lt 400 ]; then blocked "process limit held (the fork bomb stopped after $reached of 600 processes)"
else escaped "started $reached processes"; fi

# 8. Fill the disk: write up to 500 MB to the work area; the quota must stop it first.
before=$(df -m /tmp 2>/dev/null | awk 'NR==2{print $4}')
dd if=/dev/zero of=/tmp/fill bs=1M count=500 2>/dev/null
size=$(du -m /tmp/fill 2>/dev/null | awk '{print $1}')
if [ "${size:-0}" -ge 500 ]; then escaped "wrote 500 MB to the scratch area"; else blocked "scratch area is size-limited (${size:-0} MB written, ${before:-?} MB was free)"; fi

if [ $failed -eq 0 ]; then echo "RESULT: the sandbox held"; else echo "RESULT: SANDBOX FAILURE"; fi
exit $failed
