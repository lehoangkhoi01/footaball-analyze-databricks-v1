import threading
import requests
import json
import logging
from typing import Dict, Any, Optional, Union
from urllib.parse import urljoin
import time

class APIRequestHandler:
    """
    A robust API request handler for making HTTP requests to third-party APIs
    """

    _instances = {}  # Dictionary to store instances by key
    _lock = threading.Lock()  # Class-level lock for thread-safe singleton creation

    def __new__(cls, key="default", **kwargs):
        if key not in cls._instances:
            with cls._lock:
                if key not in cls._instances:
                    cls._instances[key] = super().__new__(cls)
        return cls._instances[key]

    def __init__(self, key="default", base_url: str = None, timeout: int = 30, max_retries: int = 3):
        """
        Initialize the API request handler
        
        Args:
            key: Unique identifier for this instance (for singleton pattern)
            base_url: The base URL for the API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        # Use key as part of initialization check to avoid reinitializing
        init_attr = f'initialized_{key}'
        if hasattr(self, init_attr):
            # Already initialized, warn if different parameters provided
            if (base_url is not None and hasattr(self, 'base_url') and base_url.rstrip('/') != self.base_url) or \
               (timeout != self.timeout) or \
               (max_retries != self.max_retries):
                logging.warning(f"APIRequestHandler[{key}] already initialized. New parameters ignored.")
            return
        
        # Mark as initialized for this key
        setattr(self, init_attr, True)
        
        # Validate required parameters
        if base_url is None:
            raise ValueError("base_url is required for initialization")
        
        self.key = key
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.last_request_time = 0
        self.request_lock = threading.Lock()
        
        # Set up logging
        self.logger = logging.getLogger(f"{__name__}.{key}")
        logging.basicConfig(level=logging.INFO)
        
        # Default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Python-API-Client/1.0'
        })
        
        self.logger.info(f"APIRequestHandler[{key}] initialized with base_url={base_url}, timeout={timeout}, max_retries={max_retries}")

    @classmethod
    def get_instance(cls, key="default") -> 'APIRequestHandler':
        """
        Get existing instance by key. Raises error if not initialized.
        
        Args:
            key: Instance key
            
        Returns:
            APIRequestHandler instance
        """
        if key not in cls._instances:
            raise ValueError(f"APIRequestHandler[{key}] not initialized. Call constructor first.")
        return cls._instances[key]

    @classmethod
    def list_instances(cls) -> list:
        """List all initialized instance keys"""
        return list(cls._instances.keys())

    def set_auth(self, auth_type: str, **kwargs):
        """
        Set authentication for requests
        
        Args:
            auth_type: Type of auth ('bearer', 'basic', 'api_key')
            **kwargs: Authentication parameters
        """
        if auth_type.lower() == 'bearer':
            token = kwargs.get('token')
            self.session.headers['Authorization'] = f'Bearer {token}'
            
        elif auth_type.lower() == 'basic':
            username = kwargs.get('username')
            password = kwargs.get('password')
            self.session.auth = (username, password)
            
        elif auth_type.lower() == 'api_key':
            key = kwargs.get('key')
            header_name = kwargs.get('header', 'X-API-Key')
            self.session.headers[header_name] = key
            
        else:
            raise ValueError(f"Unsupported auth type: {auth_type}")

    def set_headers(self, headers: Dict[str, str]):
        """Update session headers"""
        self.session.headers.update(headers)

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint"""
        return urljoin(self.base_url + '/', endpoint.lstrip('/'))

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Make HTTP request with retry logic
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request parameters
            
        Returns:
            requests.Response object
        """
        url = self._build_url(endpoint)
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.info(f"Making {method} request to {url} (attempt {attempt + 1})")
                
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.timeout,
                    **kwargs
                )
                
                # Log response details
                self.logger.info(f"Response: {response.status_code} - {response.reason}")
                
                return response
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}): {str(e)}")
                
                if attempt == self.max_retries:
                    self.logger.error(f"Max retries exceeded for {method} {url}")
                    raise
                
                # Exponential backoff
                time.sleep(2 ** attempt)

    def _handle_response(self, response: requests.Response, 
                        return_raw: bool = False) -> Union[Dict[str, Any], str, requests.Response]:
        """
        Handle API response
        
        Args:
            response: requests.Response object
            return_raw: Return raw response object if True
            
        Returns:
            Parsed response data or raw response
        """
        if return_raw:
            return response
            
        # Check if request was successful
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP Error: {e}")
            # Try to get error details from response
            try:
                error_data = response.json()
                raise APIError(f"API Error: {error_data}", response.status_code, error_data)
            except (ValueError, json.JSONDecodeError):
                raise APIError(f"HTTP {response.status_code}: {response.text}", response.status_code)

        # Try to parse JSON response
        try:
            return response.json()
        except (ValueError, json.JSONDecodeError):
            # Return text if not JSON
            return response.text

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, 
           return_raw: bool = False, **kwargs) -> Union[Dict[str, Any], str, requests.Response]:
        """
        Make GET request
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            return_raw: Return raw response if True
            **kwargs: Additional request parameters
            
        Returns:
            Response data
        """
        response = self._make_request('GET', endpoint, params=params, **kwargs)
        return self._handle_response(response, return_raw)

    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None,
            json_data: Optional[Dict[str, Any]] = None, return_raw: bool = False, 
            **kwargs) -> Union[Dict[str, Any], str, requests.Response]:
        """
        Make POST request
        
        Args:
            endpoint: API endpoint
            data: Form data
            json_data: JSON data
            return_raw: Return raw response if True
            **kwargs: Additional request parameters
            
        Returns:
            Response data
        """
        request_kwargs = {}
        if json_data is not None:
            request_kwargs['json'] = json_data
        if data is not None:
            request_kwargs['data'] = data
            
        request_kwargs.update(kwargs)
        
        response = self._make_request('POST', endpoint, **request_kwargs)
        return self._handle_response(response, return_raw)

    def put(self, endpoint: str, data: Optional[Dict[str, Any]] = None,
           json_data: Optional[Dict[str, Any]] = None, return_raw: bool = False,
           **kwargs) -> Union[Dict[str, Any], str, requests.Response]:
        """Make PUT request"""
        request_kwargs = {}
        if json_data is not None:
            request_kwargs['json'] = json_data
        if data is not None:
            request_kwargs['data'] = data
            
        request_kwargs.update(kwargs)
        
        response = self._make_request('PUT', endpoint, **request_kwargs)
        return self._handle_response(response, return_raw)

    def delete(self, endpoint: str, return_raw: bool = False, 
              **kwargs) -> Union[Dict[str, Any], str, requests.Response]:
        """Make DELETE request"""
        response = self._make_request('DELETE', endpoint, **kwargs)
        return self._handle_response(response, return_raw)

    def patch(self, endpoint: str, data: Optional[Dict[str, Any]] = None,
             json_data: Optional[Dict[str, Any]] = None, return_raw: bool = False,
             **kwargs) -> Union[Dict[str, Any], str, requests.Response]:
        """Make PATCH request"""
        request_kwargs = {}
        if json_data is not None:
            request_kwargs['json'] = json_data
        if data is not None:
            request_kwargs['data'] = data
            
        request_kwargs.update(kwargs)
        
        response = self._make_request('PATCH', endpoint, **request_kwargs)
        return self._handle_response(response, return_raw)

    def close(self):
        """Close the session"""
        self.session.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def fetch_with_rate_limit(self, delay_time: float, 
                              headers: Dict[str, str], 
                              endpoint: str, 
                              params: Optional[Dict[str, Any]]) -> Union[Dict[str, Any], str, requests.Response]: 
        """
        Fetch data with rate limiting
        
        Args:
            delay_time: Minimum delay between requests in seconds
            headers: Additional headers for this request
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            Response data
        """
        with self.request_lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            
            self.logger.info(f'Last request time: {self.last_request_time}')
            self.logger.info(f'Elapsed: {elapsed}')
            self.logger.info(f'Delay time: {delay_time}')
            
            if elapsed < delay_time:
                sleep_time = delay_time - elapsed
                self.logger.info(f'Sleeping for {sleep_time:.2f} seconds to respect rate limit')
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
        
        # Set headers for this request
        self.set_headers(headers)
        response = self.get(endpoint, params)
        return response


class APIError(Exception):
    """Custom exception for API errors"""
    
    def __init__(self, message: str, status_code: int = None, response_data: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


# Example usage
# if __name__ == "__main__":
#     # Initialize different API handlers for different services
    
#     # GitHub API handler
#     github_api = APIRequestHandler(
#         key="github",
#         base_url="https://api.github.com",
#         timeout=30,
#         max_retries=3
#     )
    
#     # JSONPlaceholder API handler (for testing)
#     test_api = APIRequestHandler(
#         key="test",
#         base_url="https://jsonplaceholder.typicode.com",
#         timeout=10,
#         max_retries=2
#     )
    
#     # Get the same instances later
#     github_api_2 = APIRequestHandler.get_instance("github")
#     test_api_2 = APIRequestHandler.get_instance("test")
    
#     print(f"GitHub API instances are same: {github_api is github_api_2}")
#     print(f"Test API instances are same: {test_api is test_api_2}")
#     print(f"All instances: {APIRequestHandler.list_instances()}")
    
#     # Try to reinitialize with different parameters (will show warning)
#     github_api_3 = APIRequestHandler(
#         key="github",
#         base_url="https://different-url.com",  # Different URL
#         timeout=60  # Different timeout
#     )
    
#     print(f"GitHub API base_url unchanged: {github_api.base_url}")  # Still original URL