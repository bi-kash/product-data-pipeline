#!/usr/bin/env python3
"""
Debug script to test AliExpress IOP API directly
"""

import os
import sys
sys.path.append('iop')
import iop
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_api_call():
    """Test the IOP API directly with debugging"""
    
    # Get configuration
    url = os.getenv("IOP_URL", "https://api-sg.aliexpress.com/rest")
    appkey = os.getenv("IOP_APPKEY")
    app_secret = os.getenv("IOP_APPSECRET")
    code = os.getenv("CODE", "3_519268_sz9JYM1A0a7mDiHDO5xm81rI5264")
    
    print(f"Testing with:")
    print(f"URL: {url}")
    print(f"App Key: {appkey}")
    print(f"App Secret: {app_secret[:10]}...")
    print(f"Code: {code}")
    print()
    
    try:
        # Create client and request
        client = iop.IopClient(url, appkey, app_secret)
        request = iop.IopRequest('/auth/token/create')
        request.add_api_param('code', code)
        
        print("Making API request...")
        
        # Execute request and catch raw response
        response = client.execute(request)
        
        print(f"Response type: {type(response)}")
        print(f"Response.type: {response.type}")
        print(f"Response.code: {response.code}")
        print(f"Response.message: {response.message}")
        print(f"Response.request_id: {response.request_id}")
        print(f"Response.body: {response.body}")
        
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_api_call()