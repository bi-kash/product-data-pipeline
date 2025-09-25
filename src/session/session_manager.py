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
from src.common.config import get_env

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cleanup_database_connections():
    """
    Clean up any lingering database connections that might cause locks.
    This is a safety measure to prevent database lock issues.
    """
    try:
        from src.common.database import engine
        import gc
        
        # Force garbage collection to clean up any lingering objects
        gc.collect()
        
        # Dispose of all connections in the pool
        if hasattr(engine, 'dispose'):
            engine.dispose()
            logger.info("Database connection pool disposed")
            
        # Additional cleanup - close any remaining connections
        if hasattr(engine.pool, 'dispose'):
            engine.pool.dispose()
            
        # Wait a moment for cleanup to complete
        import time
        time.sleep(0.1)
        
        logger.info("Database connections cleaned up successfully")
    except Exception as e:
        logger.warning(f"Could not clean up database connections: {e}")
        # This is not critical, so we continue


def check_database_lock():
    """
    Check if database is currently locked and attempt to resolve it.
    
    Returns:
        bool: True if database is accessible, False if locked
    """
    try:
        from src.common.database import get_db_session
        db = None
        try:
            db = get_db_session()
            # Try a simple query to test if database is accessible
            from sqlalchemy import text
            db.execute(text("SELECT 1")).fetchone()
            return True
        except Exception as e:
            if 'locked' in str(e).lower():
                logger.warning("Database is locked, attempting cleanup...")
                return False
            else:
                # Other database error, re-raise
                raise e
        finally:
            if db:
                try:
                    db.close()
                except:
                    pass
    except Exception as e:
        logger.error(f"Error checking database lock: {e}")
        return False


def force_unlock_database():
    """
    Aggressively attempt to unlock the database by cleaning up all connections.
    This is a last resort function for persistent lock issues.
    """
    logger.info("🔓 Attempting to force unlock database...")
    
    try:
        import gc
        import time
        from src.common.database import engine
        
        # Multiple cleanup attempts
        for attempt in range(3):
            logger.info(f"Cleanup attempt {attempt + 1}/3...")
            
            # Force garbage collection
            gc.collect()
            
            # Dispose of engine and all connections
            if hasattr(engine, 'dispose'):
                engine.dispose()
            
            # Additional pool cleanup
            if hasattr(engine, 'pool') and hasattr(engine.pool, 'dispose'):
                engine.pool.dispose()
            
            # Clear any connection pool
            if hasattr(engine, '_pool') and engine._pool:
                engine._pool.dispose()
            
            time.sleep(0.5)
        
        # Test if unlock was successful
        if check_database_lock():
            logger.info("✅ Database successfully unlocked!")
            return True
        else:
            logger.warning("⚠️ Database may still be locked")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error during force unlock: {e}")
        return False


def get_oauth_authorization_url():
    """
    Generate OAuth authorization URL for AliExpress using app credentials from .env
    
    Returns:
        str: Complete OAuth authorization URL
    """
    try:
        app_key = get_env('IOP_APPKEY')
        app_secret = get_env('IOP_APPSECRET')
        
        if not app_key or not app_secret:
            return "https://auth.aliexpress.com/oauth/authorize?response_type=code&client_id=YOUR_APP_KEY&client_secret=YOUR_APP_SECRET"
        
        return f"https://auth.aliexpress.com/oauth/authorize?response_type=code&client_id={app_key}&client_secret={app_secret}"
        
    except Exception as e:
        logger.error(f"Error generating OAuth URL: {e}")
        return "https://auth.aliexpress.com/oauth/authorize?response_type=code&client_id=YOUR_APP_KEY&client_secret=YOUR_APP_SECRET"


def create_session(code):
    """
    Create a new session using the authorization code.
    
    Args:
        code: Authorization code from AliExpress
    
    Returns:
        dict: Result with success status, message, and session data
    """
    try:
        # Clean up any lingering database connections first
        cleanup_database_connections()
        
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
            oauth_url = get_oauth_authorization_url()
            
            if 'Expecting value' in error_str or 'JSON' in error_str:
                return {
                    'success': False,
                    'message': f'The authorization code appears to be invalid or expired. Get a new authorization code from:\n{oauth_url}',
                    'code': code,
                    'oauth_url': oauth_url
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
            oauth_url = get_oauth_authorization_url()
            
            # Provide specific error messages for common issues
            if 'invalid' in error_msg.lower() or 'expired' in error_msg.lower():
                error_msg = f"Authorization code is invalid or expired. Get a new code from:\n{oauth_url}\n\nOriginal error: {error_msg}"
            elif 'unauthorized' in error_msg.lower():
                error_msg = f"Invalid API credentials. Please check IOP_APPKEY and IOP_APPSECRET. Original error: {error_msg}"
            
            logger.error(f"Failed to create session: {error_msg}")
            return {
                'success': False,
                'message': error_msg,
                'code': code,
                'oauth_url': oauth_url if 'invalid' in error_msg.lower() or 'expired' in error_msg.lower() else None
            }
        
        # Save session to database with retry mechanism for lock issues
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Check if database is accessible before attempting to save
                if not check_database_lock():
                    logger.info(f"Database locked, attempt {retry_count + 1}/{max_retries} - cleaning up connections...")
                    cleanup_database_connections()
                    import time
                    time.sleep(0.5 * (retry_count + 1))  # Progressive delay
                    retry_count += 1
                    continue
                
                # Attempt to save session
                session_obj = create_session_code(code, response.body, token_type='original')
                break  # Success - exit retry loop
                
            except Exception as db_error:
                if 'locked' in str(db_error).lower() and retry_count < max_retries - 1:
                    logger.warning(f"Database locked on attempt {retry_count + 1}/{max_retries}, retrying after cleanup...")
                    cleanup_database_connections()
                    import time
                    time.sleep(1.0 * (retry_count + 1))  # Progressive delay
                    retry_count += 1
                    continue
                else:
                    # Final attempt failed or non-lock error
                    logger.error(f"Database error while saving session: {db_error}")
                    cleanup_database_connections()
                    return {
                        'success': False,
                        'message': f"Failed to save session to database after {retry_count + 1} attempts: {db_error}",
                        'code': code
                    }
        else:
            # All retries exhausted
            cleanup_database_connections()
            return {
                'success': False,
                'message': f"Database remains locked after {max_retries} attempts. Please check if another process is using the database.",
                'code': code
            }
        
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
        # Clean up database connections in case of any error
        cleanup_database_connections()
        return {
            'success': False,
            'message': f"Error creating session: {str(e)}",
            'code': code
        }


def auto_refresh_session(access_token=None, refresh_token=None):
    """
    Auto-refresh session token with simplified logic:
    1. If session exists in database, use it automatically (tokens are optional)
    2. If no session exists, require tokens or suggest using create_session
    
    Args:
        access_token: Current access token (optional if session exists in DB)
        refresh_token: Refresh token to use (optional if session exists in DB)
    
    Returns:
        dict: Result with success status, message, and session data
    """
    try:
        from src.common.database import get_db_session, SessionCode
        
        # Ensure tables exist
        create_tables_if_not_exist()
        
        # Try to get the most recent session from database (active or inactive)
        db = get_db_session()
        try:
            recent_session = db.query(SessionCode).order_by(SessionCode.updated_at.desc()).first()
            
            if recent_session and recent_session.access_token and recent_session.refresh_token:
                # Found session with tokens in database - use them
                db_access_token = access_token or recent_session.access_token
                db_refresh_token = refresh_token or recent_session.refresh_token
                
                logger.info(f"Using session from database: '{recent_session.code}' (using {'provided' if access_token else 'database'} access_token, {'provided' if refresh_token else 'database'} refresh_token)")
                return refresh_with_tokens(db_access_token, db_refresh_token, recent_session.code)
            
            else:
                # No valid session in database - require tokens
                oauth_url = get_oauth_authorization_url()
                
                if not access_token or not refresh_token:
                    missing = []
                    if not access_token:
                        missing.append("access token (--token)")
                    if not refresh_token:
                        missing.append("refresh token (--refresh-token)")
                    
                    return {
                        'success': False,
                        'message': f"No session found in database. Please provide: {', '.join(missing)}.\n\nIf you don't have tokens, get authorization code from:\n{oauth_url}\n\nThen use: python main.py create_session --code YOUR_CODE",
                        'needs_tokens': True,
                        'suggest_create': True,
                        'oauth_url': oauth_url
                    }
                
                # Use provided tokens
                logger.info("No database session found, using provided tokens")
                return refresh_with_tokens(access_token, refresh_token, None)
            
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
            
            oauth_url = get_oauth_authorization_url()
            
            if 'Expecting value' in error_str or 'JSON' in error_str:
                return {
                    'success': False,
                    'message': f'The refresh token appears to be invalid or expired. Please get a new authorization code from:\n{oauth_url}\n\nThen use: python main.py create_session --code YOUR_CODE',
                    'oauth_url': oauth_url,
                    'suggest_create': True
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
            
            oauth_url = get_oauth_authorization_url()
            
            # Check for specific error types that indicate need for new authorization
            if 'IllegalRefreshToken' in error_msg or 'invalid' in error_msg.lower() or 'expired' in error_msg.lower():
                return {
                    'success': False,
                    'message': f"Refresh token is invalid or expired: {error_msg}\n\nGet a new authorization code from:\n{oauth_url}\n\nThen use: python main.py create_session --code YOUR_CODE",
                    'oauth_url': oauth_url,
                    'suggest_create': True
                }
            
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


def get_valid_token_for_code(code, refresh_margin_seconds=300, refresh_interval_hours=6):
    """
    Get a valid access token for the given session code.
    Automatically refreshes the token if it's expired, about to expire, or if 6 hours have passed since last refresh.
    
    Args:
        code: Session code
        refresh_margin_seconds: Refresh token this many seconds before expiry (default: 5 minutes)
        refresh_interval_hours: Refresh token after this many hours since last update (default: 6 hours)
    
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
        
        # Check both expiry time and time since last refresh
        current_time = int(time.time() * 1000)  # Convert to milliseconds
        expire_time = int(session.expire_time) if session.expire_time else None
        
        # Check if token is expired or about to expire (existing logic)
        token_needs_expiry_refresh = expire_time and current_time >= (expire_time - (refresh_margin_seconds * 1000))
        
        # Check if 6 hours have passed since last refresh (new logic)
        time_needs_refresh = False
        if session.updated_at:
            # Convert updated_at to timestamp in milliseconds
            updated_at_timestamp = int(session.updated_at.timestamp() * 1000)
            refresh_interval_ms = refresh_interval_hours * 3600 * 1000  # Convert hours to milliseconds
            time_needs_refresh = current_time >= (updated_at_timestamp + refresh_interval_ms)
        
        # Refresh if either condition is met
        if token_needs_expiry_refresh or time_needs_refresh:
            if token_needs_expiry_refresh:
                logger.info(f"Token for session '{code}' is expired or about to expire, refreshing...")
            elif time_needs_refresh:
                hours_since_update = (current_time - updated_at_timestamp) / (1000 * 3600)
                logger.info(f"Token for session '{code}' was last refreshed {hours_since_update:.1f} hours ago (>= {refresh_interval_hours}h), refreshing...")
            
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
        
        # Token is still valid and not due for refresh
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


def get_latest_valid_tokens():
    """
    Get the latest valid tokens from the database regardless of session code.
    This is simpler than using session codes.
    
    Returns:
        dict: Result with success status, message, and token data
    """
    from src.common.database import get_db_session, SessionCode
    import time
    
    db = get_db_session()
    try:
        # Get the most recently updated active session
        session = db.query(SessionCode).filter(
            SessionCode.is_active == True
        ).order_by(SessionCode.updated_at.desc()).first()
        
        if not session:
            return {
                'success': False,
                'message': 'No active sessions found',
                'token': None
            }
        
        # Check if token is still valid
        current_time_ms = int(time.time() * 1000)
        expire_time = int(session.expire_time) if isinstance(session.expire_time, str) else session.expire_time
        
        # If token expires within 1 hour, refresh it
        if expire_time - current_time_ms < 3600000:  # 1 hour in ms
            logger.info(f"Token expires soon, refreshing...")
            return refresh_latest_session_token()
        
        return {
            'success': True,
            'message': 'Valid token found',
            'token': session.access_token,
            'refreshed': False
        }
        
    finally:
        db.close()


def refresh_latest_session_token():
    """
    Refresh the token for the latest session in the database.
    
    Returns:
        dict: Result with success status, message, and token data
    """
    from src.common.database import get_db_session, SessionCode
    
    db = get_db_session()
    try:
        # Get the most recently updated active session
        session = db.query(SessionCode).filter(
            SessionCode.is_active == True
        ).order_by(SessionCode.updated_at.desc()).first()
        
        if not session:
            return {
                'success': False,
                'message': 'No active sessions found to refresh',
                'token': None
            }
        
        # Use the existing refresh logic but with the latest session
        return refresh_session_token(session.code)
        
    finally:
        db.close()
