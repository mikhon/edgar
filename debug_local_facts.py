import os
import sys
from edgar import Company, use_local_storage

# Enable local storage
os.environ['EDGAR_USE_LOCAL_DATA'] = '1'
# If the user downloaded to default ~/.edgar, we might not need to set path, but let's be sure.
# The user's screenshot has .edgar at the root of the view, implying it might be a hidden folder in home or current project.
# Since the user ran `download_edgar_data` in the previous turn (even if it failed, they said "now I have downloaded"),
# it likely went to the default location or the one configured.
use_local_storage(True)

print("Testing with CIK 1750 (AAR CORP) from local storage")
company = Company(1750)
try:
    facts = company.get_facts()
    if facts:
        print("Facts loaded successfully")
        print(f"Facts count: {len(facts)}")
        # Print a bit to verify
        print(str(facts)[:200])
    else:
        print("Facts returned None")
except Exception as e:
    print(f"Caught exception: {e}")
    import traceback
    traceback.print_exc()
