"""
Session management for AliExpress API using the iop module.

This module handles:
- Creating sessions using authorization codes
- Refreshing expired tokens
- Automatically validating and refreshing tokens before use
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone

# Add the iop module to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'iop'))
import iop

from src.common.database import (
    create_tables_if_not_exist,
    create_session_code,
    get_active_session_by_code,
    deactivate_session
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_session(code):
    """
    Create a new session using the authorization code.
    
    Args:
        code: Authorization code from AliExpress
    
    Returns:
        dict: Result with success status, message, and session data
    """
    try:
        # Ensure tables exist
        create_tables_if_not_exist()
        
        # Check if session already exists
        existing_session = get_active_session_by_code(code)
        if existing_session:
            logger.warning(f"Active session already exists for code '{code}'")
            return {
                'success': False,
                'message': f"Active session already exists for code '{code}'",
                'code': code,
                'token': existing_session.access_token
            }
        
        # Get IOP configuration for create_session
        url = "https://api-sg.aliexpress.com/rest"  # Always use /rest for authentication
        appkey = os.getenv("IOP_APPKEY")
        app_secret = os.getenv("IOP_APPSECRET")
        
        if not (appkey and app_secret):
            return {
                'success': False,
                'message': 'IOP_APPKEY and IOP_APPSECRET must be set in .env file',
                'code': code
            }
        
        # Create IOP client and request
        client = iop.IopClient(url, appkey, app_secret)
        request = iop.IopRequest('/auth/token/create')
        request.add_api_param('code', code)
        
        # Execute request
        try:
            response = client.execute(request)
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error executing IOP request: {e}")
            return {
                'success': False,
                'message': f'Network error: {e}. Please check your internet connection.',
                'code': code
            }
        except Exception as e:
            error_str = str(e)
            logger.error(f"Error executing IOP request: {error_str}")
            
            # Handle specific JSON parsing errors
            if 'Expecting value' in error_str or 'JSON' in error_str:
                return {
                    'success': False,
                    'message': 'The authorization code appears to be invalid or expired. Please get a new authorization code from AliExpress.',
                    'code': code
                }
            elif 'Connection' in error_str or 'timeout' in error_str.lower():
                return {
                    'success': False,
                    'message': f'Network error: {error_str}. Please check your internet connection.',
                    'code': code
                }
            else:
                return {
                    'success': False,
                    'message': f'API request failed: {error_str}',
                    'code': code
                }
        
        # Debug: Log the raw response
        logger.info(f"API Response type: {response.type}")
        logger.info(f"API Response code: {response.code}")
        logger.info(f"API Response message: {response.message}")
        
        # Check if we got a valid response structure
        if not hasattr(response, 'body') or response.body is None:
            logger.error("API returned invalid response structure")
            return {
                'success': False,
                'message': 'API returned invalid response. Please check your credentials and try again.',
                'code': code
            }
        
        # Check for API errors
        if response.code != "0":
            error_msg = response.message or 'Unknown error'
            
            # Provide specific error messages for common issues
            if 'invalid' in error_msg.lower() or 'expired' in error_msg.lower():
                error_msg = f"Authorization code is invalid or expired. Please get a new code. Original error: {error_msg}"
            elif 'unauthorized' in error_msg.lower():
                error_msg = f"Invalid API credentials. Please check IOP_APPKEY and IOP_APPSECRET. Original error: {error_msg}"
            
            logger.error(f"Failed to create session: {error_msg}")
            return {
                'success': False,
                'message': error_msg,
                'code': code
            }
        
        # Save session to database
        session_obj = create_session_code(code, response.body, token_type='original')
        
        logger.info(f"Created new session for code '{code}' with token '{response.body.get('access_token', '')[:16]}...'")
        
        return {
            'success': True,
            'message': 'Session created successfully',
            'code': code,
            'token': response.body.get('access_token'),
            'session_id': session_obj.id,
            'response': response.body
        }
        
    except Exception as e:
        logger.error(f"Error creating session: {str(e)}")
        return {
            'success': False,
            'message': f"Error creating session: {str(e)}",
            'code': code
        }


def auto_refresh_session(access_token=None, refresh_token=None):
    """
    Auto-refresh session token.
    - If both tokens provided, uses them directly
    - If only refresh_token provided, tries to find access_token from database
    - If no tokens provided, tries to find both from database
    
    Args:
        access_token: Current access token (session parameter)
        refresh_token: Refresh token to use
    
    Returns:
        dict: Result with success status, message, and session data
    """
    try:
        from src.common.database import get_db_session, SessionCode
        
        # Ensure tables exist
        create_tables_if_not_exist()
        
        # If both tokens provided, use them directly
        if access_token and refresh_token:
            logger.info("Using provided access token and refresh token")
            return refresh_with_tokens(access_token, refresh_token, None)
        
        # Try to get the most recent active session from database
        db = get_db_session()
        try:
            recent_session = db.query(SessionCode).filter(
                SessionCode.is_active == True
            ).order_by(SessionCode.updated_at.desc()).first()
            
            if recent_session and recent_session.access_token and recent_session.refresh_token:
                # Found session with both tokens in database
                if refresh_token:
                    # Use provided refresh_token with database access_token
                    logger.info(f"Using access token from database session '{recent_session.code}' with provided refresh token")
                    return refresh_with_tokens(recent_session.access_token, refresh_token, recent_session.code)
                else:
                    # Use both tokens from database
                    logger.info(f"Using both tokens from database session '{recent_session.code}'")
                    return refresh_with_tokens(recent_session.access_token, recent_session.refresh_token, recent_session.code)
            
            elif recent_session and access_token:
                # Have access_token parameter and session in database
                db_refresh_token = refresh_token or recent_session.refresh_token
                if db_refresh_token:
                    logger.info(f"Using provided access token with refresh token from session '{recent_session.code}'")
                    return refresh_with_tokens(access_token, db_refresh_token, recent_session.code)
            
            # Need both tokens
            missing = []
            if not access_token and (not recent_session or not recent_session.access_token):
                missing.append("access token (--token)")
            if not refresh_token and (not recent_session or not recent_session.refresh_token):
                missing.append("refresh token (--refresh-token)")
            
            if missing:
                return {
                    'success': False,
                    'message': f"Missing required parameters: {', '.join(missing)}. Both access token and refresh token are required for the API.",
                    'needs_tokens': True
                }
            
            return {
                'success': False,
                'message': "Unable to find required tokens. Please provide both --token and --refresh-token parameters.",
                'needs_tokens': True
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in auto refresh session: {str(e)}")
        return {
            'success': False,
            'message': f"Error refreshing session: {str(e)}",
            'needs_tokens': False
        }


def refresh_with_tokens(access_token, refresh_token, existing_code=None):
    """
    Refresh tokens using the correct API structure.
    Both access_token and refresh_token are required.
    
    Args:
        access_token: Current access token (session parameter) - REQUIRED
        refresh_token: Refresh token to use - REQUIRED
        existing_code: Existing session code (can be None)
    
    Returns:
        dict: Result with success status, message, and session data
    """
    try:
        if not access_token or not refresh_token:
            return {
                'success': False,
                'message': 'Both access_token and refresh_token are required for the API call'
            }
        
        # Get IOP configuration for refresh_with_tokens
        url = "https://api-sg.aliexpress.com/rest"  # Always use /rest for authentication
        appkey = os.getenv("IOP_APPKEY")
        app_secret = os.getenv("IOP_APPSECRET")
        
        if not (appkey and app_secret):
            return {
                'success': False,
                'message': 'IOP_APPKEY and IOP_APPSECRET must be set in .env file'
            }
        
        # Create IOP client and request - exactly as you specified
        client = iop.IopClient(url, appkey, app_secret)
        request = iop.IopRequest('/auth/token/refresh')
        
        # Add BOTH required parameters as you specified
        request.add_api_param('session', access_token)
        request.add_api_param('refresh_token', refresh_token)
        
        logger.info(f"Calling API with both session token and refresh token")
        
        # Execute request
        try:
            response = client.execute(request)
            print(f"Response type: {response.type}")
            print(f"Response body: {response.body}")
        except Exception as e:
            error_str = str(e)
            logger.error(f"Error executing IOP refresh request: {error_str}")
            
            if existing_code:
                deactivate_session(existing_code)
            
            if 'Expecting value' in error_str or 'JSON' in error_str:
                return {
                    'success': False,
                    'message': 'The refresh token appears to be invalid or expired. Please get a new authorization code.',
                }
            elif 'Connection' in error_str or 'timeout' in error_str.lower():
                return {
                    'success': False,
                    'message': f'Network error: {error_str}. Please check your internet connection.',
                }
            else:
                return {
                    'success': False,
                    'message': f'API request failed: {error_str}',
                }
        
        # Check response
        if response.code != "0":
            error_msg = response.message or 'Unknown error'
            logger.error(f"Failed to refresh token: {error_msg}")
            
            if existing_code:
                deactivate_session(existing_code)
            
            return {
                'success': False,
                'message': f"Failed to refresh token: {error_msg}",
            }
        
        # Success - create or update session
        if existing_code:
            # Update existing session
            session_obj = create_session_code(existing_code, response.body, token_type='refreshed')
            code = existing_code
        else:
            # Create new session with temporary code
            import time
            code = f"refreshed_{int(time.time())}"
            session_obj = create_session_code(code, response.body, token_type='refreshed')
        
        logger.info(f"Successfully refreshed token for session '{code}'")
        
        return {
            'success': True,
            'message': 'Token refreshed successfully',
            'code': code,
            'token': response.body.get('access_token'),
            'session_id': session_obj.id,
            'response': response.body
        }
        
    except Exception as e:
        logger.error(f"Error refreshing token: {str(e)}")
        return {
            'success': False,
            'message': f"Error refreshing token: {str(e)}",
        }


def refresh_session_token(code):
    """
    Refresh the token for an existing session.
    
    Args:
        code: Session code to refresh
    
    Returns:
        dict: Result with success status, message, and updated session data
    """
    try:
        # Get existing session
        session = get_active_session_by_code(code)
        if not session:
            return {
                'success': False,
                'message': f"No active session found for code '{code}'",
                'code': code
            }
        
        # Get refresh token
        refresh_token = session.refresh_token
        if not refresh_token:
            return {
                'success': False,
                'message': 'No refresh token available for this session',
                'code': code
            }
        
        # Get IOP configuration for refresh_session_token 
        url = "https://api-sg.aliexpress.com/rest"  # Always use /rest for authentication
        appkey = os.getenv("IOP_APPKEY")
        app_secret = os.getenv("IOP_APPSECRET")
        
        if not (appkey and app_secret):
            return {
                'success': False,
                'message': 'IOP_APPKEY and IOP_APPSECRET must be set in .env file',
                'code': code
            }
        
        # Create IOP client and request
        client = iop.IopClient(url, appkey, app_secret)
        request = iop.IopRequest('/auth/token/refresh')
        request.add_api_param('refresh_token', refresh_token)
        
        # Execute request
        try:
            response = client.execute(request)
        except Exception as e:
            error_str = str(e)
            logger.error(f"Error executing IOP refresh request: {error_str}")
            
            # Mark session as inactive if refresh fails
            deactivate_session(code)
            
            # Handle specific JSON parsing errors
            if 'Expecting value' in error_str or 'JSON' in error_str:
                return {
                    'success': False,
                    'message': 'The refresh token appears to be invalid or expired. Please create a new session.',
                    'code': code
                }
            elif 'Connection' in error_str or 'timeout' in error_str.lower():
                return {
                    'success': False,
                    'message': f'Network error: {error_str}. Please check your internet connection.',
                    'code': code
                }
            else:
                return {
                    'success': False,
                    'message': f'API request failed: {error_str}',
                    'code': code
                }
        
        # Check response
        if response.code != "0":
            error_msg = response.message or 'Unknown error'
            logger.error(f"Failed to refresh token: {error_msg}")
            
            # Mark session as inactive if refresh fails
            deactivate_session(code)
            
            return {
                'success': False,
                'message': f"Failed to refresh token: {error_msg}",
                'code': code
            }
        
        # Update session with new token data
        session_obj = create_session_code(code, response.body, token_type='refreshed')
        
        logger.info(f"Refreshed token for session '{code}'")
        
        return {
            'success': True,
            'message': 'Token refreshed successfully',
            'code': code,
            'token': response.body.get('access_token'),
            'session_id': session_obj.id,
            'response': response.body
        }
        
    except Exception as e:
        logger.error(f"Error refreshing token: {str(e)}")
        return {
            'success': False,
            'message': f"Error refreshing token: {str(e)}",
            'code': code
        }


def get_valid_token_for_code(code, refresh_margin_seconds=300):
    """
    Get a valid access token for the given session code.
    Automatically refreshes the token if it's expired or about to expire.
    
    Args:
        code: Session code
        refresh_margin_seconds: Refresh token this many seconds before expiry (default: 5 minutes)
    
    Returns:
        dict: Result with success status, token, and metadata
    """
    try:
        # Get the latest session
        session = get_active_session_by_code(code)
        if not session or not session.access_token:
            return {
                'success': False,
                'message': 'No active session or token found',
                'token': None,
                'refreshed': False
            }
        
        # Check if token is expired or about to expire
        current_time = int(time.time() * 1000)  # Convert to milliseconds
        expire_time = int(session.expire_time) if session.expire_time else None
        
        if expire_time and current_time >= (expire_time - (refresh_margin_seconds * 1000)):
            # Token is expired or about to expire, refresh it
            logger.info(f"Token for session '{code}' is expired or about to expire, refreshing...")
            refresh_result = refresh_session_token(code)
            
            if refresh_result.get('success'):
                return {
                    'success': True,
                    'token': refresh_result.get('token'),
                    'refreshed': True,
                    'message': 'Token was refreshed',
                    'response': refresh_result.get('response')
                }
            else:
                return {
                    'success': False,
                    'message': refresh_result.get('message', 'Failed to refresh token'),
                    'token': None,
                    'refreshed': False
                }
        
        # Token is still valid
        return {
            'success': True,
            'token': session.access_token,
            'refreshed': False,
            'message': 'Token is valid',
            'response': session.response_json
        }
        
    except Exception as e:
        logger.error(f"Error getting valid token: {str(e)}")
        return {
            'success': False,
            'message': f"Error getting valid token: {str(e)}",
            'token': None,
            'refreshed': False
        }


def list_sessions():
    """
    List all sessions in the database.
    
    Returns:
        list: List of session information
    """
    from src.common.database import get_db_session, SessionCode
    
    db = get_db_session()
    try:
        sessions = db.query(SessionCode).order_by(SessionCode.updated_at.desc()).all()
        
        result = []
        for session in sessions:
            result.append({
                'id': session.id,
                'code': session.code,
                'user_nick': session.user_nick,
                'account': session.account,
                'token_type': session.token_type,
                'is_active': session.is_active,
                'created_at': session.created_at.isoformat() if session.created_at else None,
                'updated_at': session.updated_at.isoformat() if session.updated_at else None,
                'expire_time': session.expire_time
            })
        
        return result
    finally:
        db.close()
