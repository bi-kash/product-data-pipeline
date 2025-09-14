"""
Token validation utility for AliExpress API calls.

This module provides automatic token validation and refresh functionality
that can be used by any AliExpress API client.
"""

import os
import logging
from src.session.session_manager import get_valid_token_for_code

logger = logging.getLogger(__name__)


class TokenValidator:
    """Utility class for validating and refreshing AliExpress API tokens."""
    
    def __init__(self, session_code=None):
        """
        Initialize token validator.
        
        Args:
            session_code: Default session code to use. Can be overridden in method calls.
        """
        self.default_session_code = session_code or os.getenv('ALIEXPRESS_SESSION_CODE')
        
    def get_valid_token(self, session_code=None, refresh_margin_seconds=300):
        """
        Get a valid access token, refreshing if necessary.
        
        Args:
            session_code: Session code to use (overrides default)
            refresh_margin_seconds: Refresh margin in seconds
            
        Returns:
            dict: Token validation result
        """
        code = session_code or self.default_session_code
        
        if not code:
            return {
                'success': False,
                'message': 'No session code provided. Set ALIEXPRESS_SESSION_CODE in .env or pass session_code parameter.',
                'token': None
            }
        
        return get_valid_token_for_code(code, refresh_margin_seconds)
    
    def validate_token_for_api_call(self, session_code=None):
        """
        Validate token before making an API call.
        
        Args:
            session_code: Session code to use
            
        Returns:
            tuple: (success: bool, token: str or None, message: str)
        """
        result = self.get_valid_token(session_code)
        
        if result['success']:
            logger.info("✅ Token validated successfully" + (" (refreshed)" if result.get('refreshed') else ""))
            return True, result['token'], result['message']
        else:
            logger.error(f"❌ Token validation failed: {result['message']}")
            return False, None, result['message']


# Global token validator instance
_global_validator = None


def get_token_validator(session_code=None):
    """
    Get a global token validator instance.
    
    Args:
        session_code: Session code to use for the validator
        
    Returns:
        TokenValidator: The validator instance
    """
    global _global_validator
    
    if _global_validator is None:
        _global_validator = TokenValidator(session_code)
    elif session_code and session_code != _global_validator.default_session_code:
        _global_validator.default_session_code = session_code
        
    return _global_validator


def validate_token_before_api_call(session_code=None):
    """
    Convenience function to validate token before making API calls.
    
    Args:
        session_code: Session code to use
        
    Returns:
        tuple: (success: bool, token: str or None, message: str)
    """
    validator = get_token_validator(session_code)
    return validator.validate_token_for_api_call(session_code)