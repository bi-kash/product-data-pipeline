#!/bin/bash
# Example script showing different ways to run the harvester
# with different keyword and category configurations

# Make sure you're in the project root directory
cd "$(dirname "$0")/.."

# Activate the virtual environment
source .venv/bin/activate

echo "===== Example 1: Run a category-only harvest (recommended) ====="
echo "Setting USE_KEYWORDS=false in .env"
# Modify .env to use only categories
sed -i.bak 's/USE_KEYWORDS=true/USE_KEYWORDS=false/' .env

# Run a small delta harvest with a limit
echo "Running delta harvest with category-only search (limit: 20 products)"
python main.py harvest:delta --limit 20

echo ""
echo "===== Example 2: Run a harvest with both keywords and categories ====="
echo "Setting USE_KEYWORDS=true in .env"
# Modify .env to use both keywords and categories
sed -i.bak 's/USE_KEYWORDS=false/USE_KEYWORDS=true/' .env

# Run a small delta harvest with a limit
echo "Running delta harvest with keywords and categories (limit: 20 products)"
python main.py harvest:delta --limit 20

echo ""
echo "===== Showing harvest status ====="
echo "This will show the job runs and which keywords/categories were used"
python main.py harvest:status

echo ""
echo "===== Restoring original .env configuration ====="
# Restore original .env from backup
if [ -f .env.bak ]; then
    mv .env.bak .env
    echo ".env restored from backup"
fi

echo ""
echo "Done! Check the harvest:status output to see how keywords and categories were used."
echo "For more details, read the SEARCH_CONFIG.md documentation."
