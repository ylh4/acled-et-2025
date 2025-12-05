"""
ACLED API Client

Provides authenticated access to the ACLED API using OAuth2 token-based authentication.
Handles token retrieval, refresh, pagination, and error handling.
"""

import os
import time
import requests
from typing import Dict, Optional, List, Any
from pathlib import Path
from dotenv import load_dotenv


class ACLEDClient:
    """
    Client for interacting with the ACLED API.
    
    Handles OAuth2 authentication, token management, and paginated requests.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ACLED API client.
        
        Parameters
        ----------
        config_path : str, optional
            Path to .env file. If None, looks for config/.env in project root.
        """
        # Load environment variables
        if config_path:
            load_dotenv(config_path)
        else:
            # Try project root config/.env
            project_root = Path(__file__).parent.parent
            env_path = project_root / "config" / ".env"
            if env_path.exists():
                load_dotenv(env_path)
            else:
                # Fallback to current directory
                load_dotenv()
        
        # Load configuration
        self.username = os.getenv("ACLED_USERNAME")
        self.password = os.getenv("ACLED_PASSWORD")
        self.client_id = os.getenv("ACLED_CLIENT_ID", "acled")
        self.base_url = os.getenv("ACLED_BASE_URL", "https://acleddata.com/api/")
        
        # Token management
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        
        # Validate credentials
        if not self.username or not self.password:
            raise ValueError(
                "ACLED_USERNAME and ACLED_PASSWORD must be set in .env file. "
                "See config/.env.example for template."
            )
    
    def _get_token(self) -> Dict[str, Any]:
        """
        Request a new OAuth2 access token from ACLED.
        
        Returns
        -------
        dict
            Token response containing access_token, refresh_token, and expires_in
        """
        token_url = "https://acleddata.com/oauth/token"
        
        payload = {
            "grant_type": "password",
            "client_id": self.client_id,
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(token_url, data=payload, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            
            # Store tokens
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token")
            
            # Calculate expiration time (default to 3600 seconds if not provided)
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = time.time() + expires_in - 60  # 1 minute buffer
            
            return token_data
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                raise ValueError(
                    "Authentication failed. Please check your ACLED_USERNAME and "
                    "ACLED_PASSWORD in config/.env"
                ) from e
            raise requests.exceptions.HTTPError(
                f"Failed to obtain token: {response.status_code} - {response.text}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to connect to ACLED API: {str(e)}") from e
    
    def _refresh_token(self) -> Dict[str, Any]:
        """
        Refresh the access token using the refresh token.
        
        Returns
        -------
        dict
            New token response
        """
        if not self.refresh_token:
            # If no refresh token, get a new one
            return self._get_token()
        
        token_url = "https://acleddata.com/oauth/token"
        
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "refresh_token": self.refresh_token
        }
        
        try:
            response = requests.post(token_url, data=payload, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            
            # Update tokens
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token", self.refresh_token)
            
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = time.time() + expires_in - 60
            
            return token_data
            
        except requests.exceptions.HTTPError:
            # If refresh fails, get a new token
            return self._get_token()
    
    def _ensure_valid_token(self) -> None:
        """
        Ensure we have a valid access token, refreshing if necessary.
        """
        if not self.access_token:
            self._get_token()
        elif self.token_expires_at and time.time() >= self.token_expires_at:
            self._refresh_token()
    
    def _make_request(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        retry: bool = True
    ) -> requests.Response:
        """
        Make an authenticated GET request to the ACLED API.
        
        Parameters
        ----------
        endpoint : str
            API endpoint (e.g., 'acled/read')
        params : dict, optional
            Query parameters
        retry : bool, default True
            Whether to retry on authentication failure
        
        Returns
        -------
        requests.Response
            API response
        """
        self._ensure_valid_token()
        
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            
            # Handle 401 Unauthorized - token may have expired
            if response.status_code == 401 and retry:
                self._get_token()  # Get fresh token
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = requests.get(url, headers=headers, params=params, timeout=60)
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(
                f"API request failed: {response.status_code} - {response.text}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to connect to ACLED API: {str(e)}") from e
    
    def get_data(
        self,
        endpoint: str = "acled/read",
        country: Optional[str] = None,
        iso: Optional[int] = None,
        event_type: Optional[str] = None,
        event_date: Optional[str] = None,
        year: Optional[int] = None,
        year_where: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Retrieve data from the ACLED API with pagination support.
        
        Parameters
        ----------
        endpoint : str, default 'acled/read'
            API endpoint
        country : str, optional
            Country name filter (e.g., 'Ethiopia')
        iso : int, optional
            ISO country code (e.g., 231 for Ethiopia)
        event_type : str, optional
            Filter by event type
        event_date : str, optional
            Filter by event date
        year : int, optional
            Filter by year
        year_where : str, optional
            Year filter operator (e.g., 'BETWEEN' for ranges)
        limit : int, default 1000
            Number of records per page
        offset : int, default 0
            Offset for pagination
        fields : list of str, optional
            Specific fields to retrieve (for efficiency)
        **kwargs
            Additional query parameters
        
        Returns
        -------
        dict
            API response containing 'data' and 'count' fields
        """
        params = {
            "limit": limit,
            "offset": offset,
            **kwargs
        }
        
        # Add filters
        if country:
            params["country"] = country
        if iso:
            params["iso"] = iso
        if event_type:
            params["event_type"] = event_type
        if event_date:
            params["event_date"] = event_date
        if year:
            params["year"] = year
        if year_where:
            params["year_where"] = year_where
        if fields:
            params["fields"] = ",".join(fields)
        
        response = self._make_request(endpoint, params=params)
        return response.json()
    
    def get_all_pages(
        self,
        endpoint: str = "acled/read",
        limit: int = 1000,
        max_pages: Optional[int] = None,
        progress: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all pages of data from the ACLED API.
        
        Parameters
        ----------
        endpoint : str, default 'acled/read'
            API endpoint
        limit : int, default 1000
            Records per page
        max_pages : int, optional
            Maximum number of pages to retrieve (None for all)
        progress : bool, default True
            Print progress messages
        **kwargs
            Query parameters (same as get_data)
        
        Returns
        -------
        list of dict
            All records from all pages
        """
        all_data = []
        offset = 0
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            if progress:
                print(f"Fetching page {page} (offset {offset})...", end=" ")
            
            try:
                response = self.get_data(
                    endpoint=endpoint,
                    limit=limit,
                    offset=offset,
                    **kwargs
                )
                
                data = response.get("data", [])
                count = response.get("count", 0)
                
                if not data:
                    if progress:
                        print("No more data.")
                    break
                
                all_data.extend(data)
                
                if progress:
                    print(f"Retrieved {len(data)} records (total: {len(all_data)})")
                
                # Check if we've retrieved all records
                if len(data) < limit or len(all_data) >= count:
                    if progress:
                        print(f"Retrieved all {len(all_data)} records.")
                    break
                
                offset += limit
                page += 1
                
                # Rate limiting: small delay between requests
                time.sleep(0.5)
                
            except Exception as e:
                if progress:
                    print(f"Error: {str(e)}")
                raise
        
        return all_data
    
    def test_connection(self) -> bool:
        """
        Test the API connection and authentication.
        
        Returns
        -------
        bool
            True if connection is successful
        """
        try:
            # Make a minimal request to test connection
            response = self.get_data(endpoint="acled/read", limit=1)
            return "data" in response
        except Exception as e:
            print(f"Connection test failed: {str(e)}")
            return False


if __name__ == "__main__":
    # Example usage
    try:
        client = ACLEDClient()
        
        # Test connection
        print("Testing ACLED API connection...")
        if client.test_connection():
            print("✓ Connection successful!")
        else:
            print("✗ Connection failed!")
        
    except ValueError as e:
        print(f"Configuration error: {str(e)}")
        print("\nPlease create config/.env with your ACLED credentials.")
        print("See config/.env.example for template.")
    except Exception as e:
        print(f"Error: {str(e)}")

