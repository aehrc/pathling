"""Writes Patient.ndjson with N patients whose birthDate increases with the id (day precision)."""
import datetime
import sys

n = int(sys.argv[2])
start = datetime.date(1920, 1, 1)
span = 36500
with open(sys.argv[1] + "/Patient.ndjson", "w") as f:
    for i in range(n):
        d = (start + datetime.timedelta(days=i * span // n)).isoformat()
        f.write(
            '{"resourceType":"Patient","id":"p%d","birthDate":"%s",'
            '"name":[{"family":"F%d","period":{"start":"%s"}}]}\n' % (i, d, i % 1000, d)
        )
