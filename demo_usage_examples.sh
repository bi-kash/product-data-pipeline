#!/bin/bash

# Product Data Pipeline Demo Usage Examples
# ========================================

echo "🚀 Product Data Pipeline Demo Examples"
echo "======================================"
echo

echo "1. 📋 Show help and options:"
echo "   python demo_pipeline.py --help"
echo

echo "2. 🎮 Run interactive demo (recommended):"
echo "   python demo_pipeline.py"
echo "   → Will ask for confirmations before each destructive action"
echo "   → Guides you through API authorization setup"
echo "   → Shows progress and results for each step"
echo

echo "3. 🏃 Run automated demo (skip confirmations):"
echo "   python demo_pipeline.py --skip-confirmation"
echo "   → ⚠️  WARNING: Will delete data without asking!"
echo "   → Still prompts for API authorization code"
echo "   → Use only if you're sure about the consequences"
echo

echo "4. 📊 Alternative: Run individual modules manually:"
echo "   # Step-by-step approach"
echo "   python main.py create_session --code YOUR_CODE"
echo "   python main.py harvest:init --limit 100"
echo "   python main.py filter:products"
echo "   python main.py detect:duplicates"
echo "   python main.py airtable:create-base --name 'My Demo Base'"
echo "   python main.py airtable:sync"
echo

echo "5. 🔍 Check results after demo:"
echo "   python main.py harvest:status"
echo "   python main.py detect:status"
echo "   # Check your Airtable base for synchronized data"
echo

echo "📝 Notes:"
echo "• The demo will DELETE ALL DATABASE DATA"
echo "• Make sure your .env file has proper AliExpress API credentials"
echo "• The demo creates a fresh Airtable base or uses existing one"
echo "• Results include harvested products, duplicate detection, and Airtable sync"
echo "• Demo takes approximately 10-15 minutes to complete"
echo

echo "🎯 Demo Results You'll See:"
echo "• ~100 products harvested from AliExpress"
echo "• Seller approval statuses (80% whitelisted, 10% blacklisted, 10% pending)"
echo "• Filtered products based on business rules"
echo "• Duplicate detection with pHash + CLIP analysis"
echo "• Complete Airtable base with anonymized product data"
echo "• Statistics and performance metrics"