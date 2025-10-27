#!/usr/bin/env python3
"""
Automated Pipeline Demo Script (Command-Based)
==============================================

This script demonstrates the complete product data pipeline workflow by:
1. Cleaning the database (with safety confirmation)
2. Setting up fresh API session
3. Running merchant harvest
4. Randomly assigning seller approval statuses
5. Filtering products
6. Detecting duplicates
7. Creating Airtable base and syncing data

Usage:
    python demo_pipeline.py [--skip-confirmation]

Options:
    --skip-confirmation: Skip safety confirmation prompts (use with caution)
"""

import os
import sys
import random
import sqlite3
import argparse
import subprocess
from pathlib import Path

def print_banner(text, char="="):
    """Print a banner with the given text."""
    width = 80
    print(f"\n{char * width}")
    print(f"{text:^{width}}")
    print(f"{char * width}\n")

def confirm_action(message, default=False):
    """Ask for user confirmation."""
    default_text = "Y/n" if default else "y/N"
    response = input(f"{message} [{default_text}]: ").strip().lower()
    
    if not response:
        return default
    
    return response in ['y', 'yes', '1', 'true']

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🚀 {description}...")
    print(f"   Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print("✅ Success!")
        if result.stdout.strip():
            print("Output:")
            for line in result.stdout.strip().split('\n'):
                print(f"   {line}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed with exit code {e.returncode}")
        if e.stdout:
            print("STDOUT:")
            for line in e.stdout.strip().split('\n'):
                print(f"   {line}")
        if e.stderr:
            print("STDERR:")
            for line in e.stderr.strip().split('\n'):
                print(f"   {line}")
        return False

def get_env_value(key, default=None):
    """Get environment variable from .env file."""
    env_file = Path(".env")
    if not env_file.exists():
        return os.getenv(key, default)
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split('=', 1)[1].strip().strip('"\'')
    
    return os.getenv(key, default)



def clean_database():
    """Delete entire database file."""
    print_banner("🗑️  DATABASE CLEANUP", "=")
    
    print("⚠️  WARNING: This will delete the entire database!")
    print("This includes:")
    print("  • All harvested products and sellers")
    print("  • All filtered products and variants")
    print("  • All downloaded images and videos")
    print("  • All duplicate detection results")
    print("  • All session tokens")
    print("  • ALL OTHER DATA")
    
    if not confirm_action("\n🚨 Are you absolutely sure you want to DELETE ALL DATA?", False):
        print("❌ Operation cancelled by user")
        sys.exit(0)
    
    try:
        use_sqlite = get_env_value("USE_SQLITE", "true").lower() == "true"
        
        if use_sqlite:
            db_path = "test.db"
            if os.path.exists(db_path):
                print(f"🗑️  Deleting entire SQLite database: {db_path}")
                os.remove(db_path)
                print("✅ Database file deleted completely")
            else:
                print("ℹ️  Database file doesn't exist, will be created fresh")
        else:
            print("⚠️  PostgreSQL detected - you may need to manually drop tables")
            print("   The pipeline will recreate tables automatically")
        
        print("✅ Database cleanup completed")
        
    except Exception as e:
        print(f"❌ Error cleaning database: {e}")
        sys.exit(1)

def setup_api_session():
    """Setup fresh API session with user interaction."""
    print_banner("🔑 API SESSION SETUP", "=")
    
    print("Creating a fresh API session for this demo...")
    print()
    
    # Get the authorization URL by running the get_code_link command
    print("🔄 Getting authorization URL...")
    try:
        result = subprocess.run("python main.py get_code_link", shell=True, capture_output=True, text=True, check=True)
        # Extract the URL from the output
        lines = result.stdout.strip().split('\n')
        auth_url = None
        for line in lines:
            if line.startswith('https://'):
                auth_url = line.strip()
                break
        
        if not auth_url:
            print("❌ Error: Could not get authorization URL")
            sys.exit(1)
            
    except subprocess.CalledProcessError as e:
        print("❌ Error getting authorization URL:")
        print(f"   {e.stderr}")
        sys.exit(1)
    
    print(f"🔗 Please visit this URL to get your authorization code:")
    print(f"   {auth_url}")
    print()
    print("📋 Steps:")
    print("   1. Click the link above (or copy-paste into browser)")
    print("   2. Log in to your AliExpress account")
    print("   3. Authorize the application")
    print("   4. Copy the authorization code from the callback URL")
    print("   5. Paste the code below")
    print()
    
    # Get authorization code from user
    while True:
        auth_code = input("🔑 Enter your authorization code: ").strip()
        if auth_code:
            break
        print("❌ Please enter a valid authorization code")
    
    # Create session
    cmd = f"python main.py create_session --code {auth_code}"
    if not run_command(cmd, "Creating API session"):
        print("❌ Failed to create API session")
        sys.exit(1)

def run_harvest():
    """Run the harvest process."""
    print_banner("🌾 MERCHANT HARVEST", "=")
    
    cmd = "python main.py harvest:init --limit 10"
    if not run_command(cmd, "Running merchant harvest for 10 products"):
        print("❌ Harvest failed")
        sys.exit(1)

def assign_seller_statuses():
    """Randomly assign seller approval statuses."""
    print_banner("👥 SELLER APPROVAL", "=")
    
    try:
        use_sqlite = get_env_value("USE_SQLITE", "true").lower() == "true"
        
        if use_sqlite:
            db_path = "test.db"
            conn = sqlite3.connect(db_path)
        else:
            print("❌ PostgreSQL support not implemented in this script")
            print("Please assign seller statuses manually or use the main demo_pipeline.py script")
            return
        
        cursor = conn.cursor()
        
        # Get all sellers
        cursor.execute("SELECT shop_id FROM sellers")
        sellers = cursor.fetchall()
        seller_ids = [row[0] for row in sellers]
        
        if not seller_ids:
            print("❌ No sellers found in database")
            conn.close()
            return
        
        print(f"📊 Found {len(seller_ids)} sellers")
        
        # Calculate counts
        total = len(seller_ids)
        whitelist_count = int(total * 0.8)  # 80%
        blacklist_count = int(total * 0.1)  # 10%
        # Remaining stay as PENDING (10%)
        
        # Shuffle sellers for random assignment
        random.shuffle(seller_ids)
        
        # Assign statuses
        whitelist_sellers = seller_ids[:whitelist_count]
        blacklist_sellers = seller_ids[whitelist_count:whitelist_count + blacklist_count]
        pending_sellers = seller_ids[whitelist_count + blacklist_count:]
        
        print(f"📈 Assigning statuses:")
        print(f"   • WHITELIST: {len(whitelist_sellers)} sellers (80%)")
        print(f"   • BLACKLIST: {len(blacklist_sellers)} sellers (10%)")
        print(f"   • PENDING: {len(pending_sellers)} sellers (10%)")
        
        # Update database
        for seller_id in whitelist_sellers:
            cursor.execute("UPDATE sellers SET approval_status = 'WHITELIST' WHERE shop_id = ?", (seller_id,))
        
        for seller_id in blacklist_sellers:
            cursor.execute("UPDATE sellers SET approval_status = 'BLACKLIST' WHERE shop_id = ?", (seller_id,))
        
        # PENDING sellers keep their current status (should already be PENDING)
        
        conn.commit()
        conn.close()
        print("✅ Seller statuses assigned successfully")
        
    except Exception as e:
        print(f"❌ Error assigning seller statuses: {e}")
        sys.exit(1)

def run_product_filter():
    """Run product filtering."""
    print_banner("🔍 PRODUCT FILTERING", "=")
    
    cmd = "python main.py filter:products"
    if not run_command(cmd, "Running product filtering"):
        print("❌ Product filtering failed")
        sys.exit(1)

def run_duplicate_detection():
    """Run duplicate detection."""
    print_banner("🔍 DUPLICATE DETECTION", "=")
    
    cmd = "python main.py detect:duplicates"
    if not run_command(cmd, "Running duplicate detection"):
        print("❌ Duplicate detection failed")
        sys.exit(1)

def setup_airtable():
    """Setup Airtable base and sync data."""
    print_banner("📊 AIRTABLE INTEGRATION", "=")
    
    # Check if base exists
    base_id = get_env_value("AIRTABLE_BASE_ID")
    
    if not base_id or not base_id.startswith("app"):
        print("🏗️  No Airtable base found, creating new base...")
        cmd = 'python main.py airtable:create-base --name "Product Pipeline Demo"'
        if not run_command(cmd, "Creating Airtable base"):
            print("❌ Base creation failed")
            print("Please check your AIRTABLE_PERSONAL_ACCESS_TOKEN in .env file")
            return
    else:
        print(f"📊 Using existing Airtable base: {base_id}")
    
    # Sync data
    cmd = "python main.py airtable:sync"
    if not run_command(cmd, "Syncing data to Airtable"):
        print("❌ Airtable sync failed")

def show_final_stats():
    """Show final pipeline statistics."""
    print_banner("📈 PIPELINE RESULTS", "=")
    
    try:
        use_sqlite = get_env_value("USE_SQLITE", "true").lower() == "true"
        
        if use_sqlite:
            db_path = "test.db"
            if not os.path.exists(db_path):
                print("❌ Database file not found")
                return
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
        else:
            print("ℹ️  PostgreSQL stats not implemented in this script")
            return
        
        # Get seller counts
        cursor.execute("""
            SELECT approval_status, COUNT(*) 
            FROM sellers 
            GROUP BY approval_status
        """)
        seller_stats = cursor.fetchall()
        
        print("👥 Sellers:")
        for status, count in seller_stats:
            print(f"   • {status}: {count}")
        
        # Get product counts
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        
        try:
            cursor.execute("SELECT COUNT(*) FROM filtered_products")
            filtered_count = cursor.fetchone()[0]
        except:
            filtered_count = 0
        
        print(f"\n📦 Products:")
        print(f"   • Total harvested: {product_count}")
        print(f"   • Passed filters: {filtered_count}")
        
        # Get duplicate detection stats
        try:
            cursor.execute("""
                SELECT status, COUNT(*) 
                FROM product_status 
                GROUP BY status
            """)
            status_stats = cursor.fetchall()
            
            if status_stats:
                print(f"\n🔍 Duplicate Detection:")
                for status, count in status_stats:
                    print(f"   • {status}: {count}")
        except:
            print(f"\n🔍 Duplicate Detection: Not run yet")
        
        # Get image counts
        try:
            cursor.execute("SELECT COUNT(*) FROM product_images WHERE s3_url IS NOT NULL")
            image_count = cursor.fetchone()[0]
            print(f"\n🖼️  Images processed: {image_count}")
        except:
            print(f"\n🖼️  Images processed: 0")
        
        conn.close()
        
        print(f"\n🎉 Pipeline demo completed successfully!")
        print(f"📊 Check your Airtable base for the synchronized data")
        
    except Exception as e:
        print(f"❌ Error getting final stats: {e}")

def main():
    """Main function to run the complete pipeline demo."""
    parser = argparse.ArgumentParser(description="Automated Pipeline Demo Script (Command-Based)")
    parser.add_argument("--skip-confirmation", action="store_true", 
                       help="Skip safety confirmation prompts (use with caution)")
    args = parser.parse_args()
    
    print_banner("🚀 PRODUCT DATA PIPELINE DEMO", "🌟")
    print("This script will demonstrate the complete pipeline workflow:")
    print("1. 🗑️  Delete entire database")
    print("2. 🔑 Create fresh API session")
    print("3. 🌾 Harvest merchants (10 products)")
    print("4. 👥 Assign seller statuses (80% whitelist, 10% blacklist, 10% pending)")
    print("5. 🔍 Filter products")
    print("6. 🔍 Detect duplicates")
    print("7. 📊 Create Airtable base and sync")
    print()
    
    if not args.skip_confirmation:
        if not confirm_action("🚀 Do you want to proceed with the demo?", True):
            print("❌ Demo cancelled by user")
            sys.exit(0)
    
    try:
        # Step 1: Clean database
        clean_database()
        
        # Step 2: Setup fresh API session
        setup_api_session()
        
        # Step 3: Run harvest
        run_harvest()
        
        # Step 4: Assign seller statuses
        assign_seller_statuses()
        
        # Step 5: Filter products
        run_product_filter()
        
        # Step 6: Detect duplicates
        run_duplicate_detection()
        
        # Step 7: Setup Airtable
        setup_airtable()
        
        # Show final results
        show_final_stats()
        
    except KeyboardInterrupt:
        print(f"\n❌ Demo interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()