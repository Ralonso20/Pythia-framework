"""
HTTP configuration classes
"""

from typing import Dict, Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class HTTPConfig(BaseSettings):
    """HTTP client configuration"""

    # Base settings
    base_url: str = Field(description="Base URL for HTTP requests")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: float = Field(default=1.0, description="Delay between retries")
    retry_backoff: float = Field(default=2.0, description="Exponential backoff multiplier")

    # Headers
    default_headers: Dict[str, str] = Field(
        default_factory=lambda: {"Content-Type": "application/json"},
        description="Default headers for all requests",
    )

    # Authentication
    auth_type: str = Field(default="none", description="Authentication type")
    api_key: Optional[str] = Field(default=None, description="API key for authentication")
    api_key_header: str = Field(default="X-API-Key", description="API key header name")
    bearer_token: Optional[str] = Field(default=None, description="Bearer token")
    username: Optional[str] = Field(default=None, description="Basic auth username")
    password: Optional[str] = Field(default=None, description="Basic auth password")

    # SSL/TLS settings
    ssl_verify: bool = Field(default=True, description="Verify SSL certificates")
    ssl_cert_file: Optional[str] = Field(default=None, description="SSL certificate file")
    ssl_key_file: Optional[str] = Field(default=None, description="SSL private key file")
    ssl_ca_file: Optional[str] = Field(default=None, description="SSL CA file")

    # Connection settings
    connection_pool_size: int = Field(default=100, description="Connection pool size")
    max_keepalive_connections: int = Field(default=20, description="Maximum keepalive connections")
    keepalive_expiry: int = Field(default=300, description="Keepalive expiry in seconds")

    # Follow redirects
    follow_redirects: bool = Field(default=True, description="Follow HTTP redirects")
    max_redirects: int = Field(default=10, description="Maximum redirects to follow")

    class Config:
        env_prefix = "HTTP_"
        case_sensitive = False
        extra = "ignore"  # Allow extra fields for flexibility

    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        headers = {}

        if self.auth_type == "api_key" and self.api_key:
            headers[self.api_key_header] = self.api_key
        elif self.auth_type == "bearer" and self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        return headers

    def get_basic_auth(self) -> Optional[tuple]:
        """Get basic auth tuple"""
        if self.auth_type == "basic" and self.username and self.password:
            return (self.username, self.password)
        return None

    def get_ssl_context(self):
        """Get SSL context"""
        if self.ssl_verify and not any([self.ssl_cert_file, self.ssl_key_file, self.ssl_ca_file]):
            return None

        import ssl

        context = ssl.create_default_context()

        if not self.ssl_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        if self.ssl_ca_file:
            context.load_verify_locations(self.ssl_ca_file)

        if self.ssl_cert_file and self.ssl_key_file:
            context.load_cert_chain(self.ssl_cert_file, self.ssl_key_file)

        return context


class WebhookConfig(HTTPConfig):
    """Webhook specific configuration"""

    # Webhook endpoints
    endpoints: Dict[str, str] = Field(default_factory=dict, description="Named webhook endpoints")

    # Payload settings
    payload_template: Optional[str] = Field(default=None, description="Payload template")
    include_headers: bool = Field(default=True, description="Include request headers in payload")
    include_metadata: bool = Field(default=True, description="Include metadata in payload")

    # Verification
    verify_signature: bool = Field(default=False, description="Verify webhook signatures")
    signature_header: str = Field(default="X-Signature", description="Signature header name")
    signature_secret: Optional[str] = Field(
        default=None, description="Secret for signature verification"
    )
    signature_algorithm: str = Field(default="sha256", description="Signature algorithm")

    # Health check
    health_check_endpoint: Optional[str] = Field(default=None, description="Health check endpoint")
    health_check_method: str = Field(default="GET", description="Health check HTTP method")
    health_check_interval: int = Field(default=300, description="Health check interval in seconds")


class PollerConfig(HTTPConfig):
    """HTTP poller configuration"""

    # Polling settings
    url: str = Field(description="URL to poll")
    interval: int = Field(default=60, description="Polling interval in seconds")
    method: str = Field(default="GET", description="HTTP method for polling")

    # Timeout settings (specific to poller)
    connect_timeout: float = Field(default=10.0, description="Connection timeout")
    read_timeout: float = Field(default=30.0, description="Read timeout")
    write_timeout: float = Field(default=30.0, description="Write timeout")
    pool_timeout: float = Field(default=5.0, description="Pool timeout")

    # Connection settings
    max_connections: int = Field(default=100, description="Max total connections")
    max_keepalive_connections: int = Field(default=20, description="Max keepalive connections")
    keepalive_expiry: int = Field(default=30, description="Keepalive expiry")

    # SSL and redirect settings
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")
    follow_redirects: bool = Field(default=True, description="Follow redirects")
    max_redirects: int = Field(default=10, description="Max redirects")

    # User agent
    user_agent: str = Field(default="1.0", description="User agent version")

    # Conditional requests
    use_conditional_requests: bool = Field(
        default=True, description="Use If-None-Match and If-Modified-Since headers"
    )

    # Request body for POST/PUT/PATCH
    request_body: Optional[dict] = Field(
        default=None, description="Request body for non-GET methods"
    )

    # Data extraction
    response_format: str = Field(default="json", description="Expected response format")
    data_path: Optional[str] = Field(default=None, description="JSONPath for data extraction")

    # Change detection
    enable_change_detection: bool = Field(default=True, description="Enable change detection")
    change_detection_method: str = Field(
        default="content_hash", description="Change detection method"
    )

    # Error handling
    ignore_http_errors: bool = Field(
        default=False, description="Ignore HTTP errors and continue polling"
    )
    max_consecutive_errors: int = Field(
        default=10, description="Maximum consecutive errors before stopping"
    )

    # Persistence
    state_file: Optional[str] = Field(default=None, description="File to persist polling state")
    checkpoint_interval: int = Field(default=10, description="Checkpoint interval in poll cycles")
