import os
import json
import gzip
from pathlib import Path

# Path from user's ls output
# /Users/mikko.honkanen/.edgar/_tcache/data.sec.gov/api-xbrl-companyfacts-CIK0000002488.json
home = Path.home()
tcache_path = home / ".edgar" / "_tcache" / "data.sec.gov" / "api-xbrl-companyfacts-CIK0000002488.json"

print(f"Checking path: {tcache_path}")
if not tcache_path.exists():
    print("File not found!")
    exit(1)

content = tcache_path.read_bytes()
print(f"Read {len(content)} bytes")

# Try to decompress
try:
    decompressed = gzip.decompress(content)
    print("Decompressed successfully")
    text = decompressed.decode('utf-8')
    data = json.loads(text)
    print("Parsed JSON successfully")
    print(f"CIK: {data.get('cik')}")
    print(f"Entity: {data.get('entityName')}")
except Exception as e:
    print(f"Error processing: {e}")
    # Maybe it's not compressed?
    try:
        text = content.decode('utf-8')
        data = json.loads(text)
        print("Parsed JSON (uncompressed) successfully")
        print(f"CIK: {data.get('cik')}")
    except Exception as e2:
        print(f"Error parsing as uncompressed: {e2}")
