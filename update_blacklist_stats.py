#!/usr/bin/env python3
import re

# Read the harvester file
with open('src/harvester/merchant_harvester.py', 'r') as f:
    content = f.read()

# Update stats dictionaries to include blacklisted counter
pattern = r'stats = \{\s+"total_products_processed": 0,'
replacement = 'stats = {\n        "total_products_processed": 0,\n        "blacklisted": 0,  # Count of products skipped due to blacklisted terms in title,'
content = re.sub(pattern, replacement, content)

# Update summary sections to include blacklisted counter in output
pattern = r'logger\.info\(f"- Products processed: \{stats\[\'total_products_processed\'\]\}"\)\s+logger\.info\(f"- Unique sellers found:'
replacement = 'logger.info(f"- Products processed: {stats[\'total_products_processed\']}")\n    logger.info(f"- Products blacklisted by title: {stats.get(\'blacklisted\', 0)}")\n    logger.info(f"- Unique sellers found:'
content = re.sub(pattern, replacement, content)

# Write the updated content back
with open('src/harvester/merchant_harvester.py', 'w') as f:
    f.write(content)

print("Updated merchant_harvester.py with blacklisted stats tracking")
