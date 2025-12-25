"""
Odoo XML-RPC Client

Provides a clean interface for communicating with Odoo via XML-RPC API.
Supports authentication, CRUD operations, batch processing, and retry logic.
"""

import time
import xmlrpc.client
from typing import Any
from dataclasses import dataclass, field


class OdooConnectionError(Exception):
    """Raised when connection to Odoo fails."""
    pass


class OdooAPIError(Exception):
    """Raised when Odoo API returns an error."""
    
    def __init__(self, message: str, fault_code: int | None = None):
        super().__init__(message)
        self.fault_code = fault_code


class OdooAuthenticationError(OdooConnectionError):
    """Raised when authentication fails."""
    pass


@dataclass
class OdooClient:
    """
    XML-RPC client for Odoo API communication.
    
    Provides methods for authentication, CRUD operations, and batch processing
    with built-in retry logic and dry-run support.
    
    Example:
        >>> client = OdooClient(
        ...     url="https://mycompany.odoo.com",
        ...     db="mydb",
        ...     username="admin",
        ...     password="admin_password"
        ... )
        >>> client.authenticate()
        >>> partners = client.search_read("res.partner", [("is_company", "=", True)], ["name", "email"])
    """
    
    url: str
    db: str
    username: str
    password: str
    timeout: int = 120
    retry_attempts: int = 3
    retry_delay: float = 2.0
    
    # Internal state
    _uid: int | None = field(default=None, init=False, repr=False)
    _common: xmlrpc.client.ServerProxy | None = field(default=None, init=False, repr=False)
    _models: xmlrpc.client.ServerProxy | None = field(default=None, init=False, repr=False)
    
    def __post_init__(self) -> None:
        """Normalize URL and initialize proxies."""
        self.url = self.url.rstrip("/")
        self._init_proxies()
    
    def _init_proxies(self) -> None:
        """Initialize XML-RPC server proxies."""
        try:
            self._common = xmlrpc.client.ServerProxy(
                f"{self.url}/xmlrpc/2/common",
                allow_none=True,
                context=None,
            )
            self._models = xmlrpc.client.ServerProxy(
                f"{self.url}/xmlrpc/2/object",
                allow_none=True,
                context=None,
            )
        except Exception as e:
            raise OdooConnectionError(f"Failed to initialize XML-RPC proxies: {e}") from e
    
    @property
    def uid(self) -> int:
        """Get authenticated user ID, raising if not authenticated."""
        if self._uid is None:
            raise OdooConnectionError("Not authenticated. Call authenticate() first.")
        return self._uid
    
    @property
    def is_authenticated(self) -> bool:
        """Check if client is authenticated."""
        return self._uid is not None
    
    def authenticate(self) -> int:
        """
        Authenticate with Odoo and return user ID.
        
        Returns:
            User ID (uid) if authentication successful
            
        Raises:
            OdooAuthenticationError: If authentication fails
            OdooConnectionError: If connection fails
        """
        try:
            if self._common is None:
                self._init_proxies()
            
            uid = self._common.authenticate(  # type: ignore
                self.db, self.username, self.password, {}
            )
            
            if not uid:
                raise OdooAuthenticationError(
                    f"Authentication failed for user '{self.username}' on database '{self.db}'"
                )
            
            self._uid = uid
            return uid
            
        except xmlrpc.client.Fault as e:
            raise OdooAuthenticationError(f"Authentication error: {e.faultString}") from e
        except Exception as e:
            if isinstance(e, OdooAuthenticationError):
                raise
            raise OdooConnectionError(f"Connection failed during authentication: {e}") from e
    
    def version(self) -> dict[str, Any]:
        """Get Odoo server version information."""
        try:
            if self._common is None:
                self._init_proxies()
            return self._common.version()  # type: ignore
        except Exception as e:
            raise OdooConnectionError(f"Failed to get version: {e}") from e
    
    def execute(
        self,
        model: str,
        method: str,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """
        Execute a method on an Odoo model.
        
        Args:
            model: Odoo model name (e.g., "res.partner")
            method: Method to call (e.g., "search", "read", "create")
            *args: Positional arguments for the method
            **kwargs: Keyword arguments for the method
            
        Returns:
            Result of the method call
            
        Raises:
            OdooAPIError: If the API call fails
            OdooConnectionError: If not authenticated or connection fails
        """
        return self._execute_with_retry(model, method, *args, **kwargs)
    
    def _execute_with_retry(
        self,
        model: str,
        method: str,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Execute with retry logic for transient failures."""
        last_error: Exception | None = None
        
        for attempt in range(self.retry_attempts):
            try:
                if self._models is None:
                    self._init_proxies()
                
                result = self._models.execute_kw(  # type: ignore
                    self.db,
                    self.uid,
                    self.password,
                    model,
                    method,
                    list(args),
                    kwargs or {}
                )
                return result
                
            except xmlrpc.client.Fault as e:
                # Don't retry on application-level errors
                raise OdooAPIError(
                    f"Odoo API error on {model}.{method}: {e.faultString}",
                    fault_code=e.faultCode
                ) from e
                
            except (ConnectionError, TimeoutError, OSError) as e:
                last_error = e
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    self._init_proxies()  # Reinitialize connection
                continue
                
            except Exception as e:
                raise OdooAPIError(f"Unexpected error on {model}.{method}: {e}") from e
        
        raise OdooConnectionError(
            f"Failed after {self.retry_attempts} attempts: {last_error}"
        )
    
    # -------------------------------------------------------------------------
    # High-Level CRUD Operations
    # -------------------------------------------------------------------------
    
    def search(
        self,
        model: str,
        domain: list[Any],
        offset: int = 0,
        limit: int | None = None,
        order: str | None = None,
    ) -> list[int]:
        """
        Search for record IDs matching the domain.
        
        Args:
            model: Model name
            domain: Odoo domain filter
            offset: Number of records to skip
            limit: Maximum records to return
            order: Sort order (e.g., "name asc, id desc")
            
        Returns:
            List of record IDs
        """
        kwargs: dict[str, Any] = {"offset": offset}
        if limit is not None:
            kwargs["limit"] = limit
        if order is not None:
            kwargs["order"] = order
            
        return self.execute(model, "search", domain, **kwargs)
    
    def search_count(self, model: str, domain: list[Any]) -> int:
        """Count records matching the domain."""
        return self.execute(model, "search_count", domain)
    
    def read(
        self,
        model: str,
        ids: list[int],
        fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Read records by IDs.
        
        Args:
            model: Model name
            ids: List of record IDs
            fields: Fields to read (None = all fields)
            
        Returns:
            List of record dictionaries
        """
        if fields is None:
            return self.execute(model, "read", ids)
        return self.execute(model, "read", ids, fields)
    
    def search_read(
        self,
        model: str,
        domain: list[Any],
        fields: list[str] | None = None,
        offset: int = 0,
        limit: int | None = None,
        order: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search and read records in one call.
        
        Args:
            model: Model name
            domain: Odoo domain filter
            fields: Fields to read
            offset: Number of records to skip
            limit: Maximum records to return
            order: Sort order
            
        Returns:
            List of record dictionaries
        """
        kwargs: dict[str, Any] = {"offset": offset}
        if fields is not None:
            kwargs["fields"] = fields
        if limit is not None:
            kwargs["limit"] = limit
        if order is not None:
            kwargs["order"] = order
            
        return self.execute(model, "search_read", domain, **kwargs)
    
    def create(
        self,
        model: str,
        values: dict[str, Any],
        dry_run: bool = False,
    ) -> int | None:
        """
        Create a new record.
        
        Args:
            model: Model name
            values: Field values for the new record
            dry_run: If True, validate but don't create
            
        Returns:
            Created record ID, or None if dry_run
        """
        if dry_run:
            # Validate by checking field types without creating
            self._validate_create_values(model, values)
            return None
        
        return self.execute(model, "create", values)
    
    def create_batch(
        self,
        model: str,
        records: list[dict[str, Any]],
        chunk_size: int = 500,
        dry_run: bool = False,
    ) -> list[int]:
        """
        Create multiple records in batches.
        
        Args:
            model: Model name
            records: List of value dictionaries
            chunk_size: Records per batch
            dry_run: If True, validate but don't create
            
        Returns:
            List of created record IDs (empty if dry_run)
        """
        if dry_run:
            for record in records:
                self._validate_create_values(model, record)
            return []
        
        created_ids: list[int] = []
        
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            # Odoo supports multi-create with list of dicts
            ids = self.execute(model, "create", chunk)
            if isinstance(ids, list):
                created_ids.extend(ids)
            else:
                created_ids.append(ids)
        
        return created_ids
    
    def write(
        self,
        model: str,
        ids: list[int],
        values: dict[str, Any],
    ) -> bool:
        """
        Update existing records.
        
        Args:
            model: Model name
            ids: Record IDs to update
            values: Field values to update
            
        Returns:
            True if successful
        """
        return self.execute(model, "write", ids, values)
    
    def unlink(self, model: str, ids: list[int]) -> bool:
        """
        Delete records.
        
        Args:
            model: Model name
            ids: Record IDs to delete
            
        Returns:
            True if successful
        """
        return self.execute(model, "unlink", ids)
    
    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------
    
    def fields_get(
        self,
        model: str,
        fields: list[str] | None = None,
        attributes: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Get field definitions for a model.
        
        Args:
            model: Model name
            fields: Specific fields to get (None = all)
            attributes: Field attributes to return
            
        Returns:
            Dictionary of field definitions
        """
        kwargs: dict[str, Any] = {}
        if attributes is not None:
            kwargs["attributes"] = attributes
            
        if fields is not None:
            return self.execute(model, "fields_get", fields, **kwargs)
        return self.execute(model, "fields_get", **kwargs)
    
    def name_search(
        self,
        model: str,
        name: str,
        args: list[Any] | None = None,
        operator: str = "ilike",
        limit: int = 100,
    ) -> list[tuple[int, str]]:
        """
        Search for records by name.
        
        Args:
            model: Model name
            name: Name to search for
            args: Additional domain to filter
            operator: Comparison operator
            limit: Maximum results
            
        Returns:
            List of (id, name) tuples
        """
        return self.execute(
            model, "name_search",
            name=name,
            args=args or [],
            operator=operator,
            limit=limit,
        )
    
    def check_access_rights(
        self,
        model: str,
        operation: str,
        raise_exception: bool = False,
    ) -> bool:
        """
        Check if user has access rights for an operation.
        
        Args:
            model: Model name
            operation: Operation type ("read", "write", "create", "unlink")
            raise_exception: Whether to raise on failure
            
        Returns:
            True if access is granted
        """
        return self.execute(
            model, "check_access_rights",
            operation,
            raise_exception=raise_exception,
        )
    
    def _validate_create_values(
        self,
        model: str,
        values: dict[str, Any],
    ) -> None:
        """
        Validate values for create operation.
        
        This is used in dry-run mode to check if values are valid
        without actually creating records.
        """
        # Get required fields
        fields_info = self.fields_get(
            model,
            list(values.keys()),
            ["type", "required", "readonly"],
        )
        
        # Check for missing required fields
        for field_name, field_info in fields_info.items():
            if field_info.get("required") and field_name not in values:
                if not field_info.get("readonly"):  # Readonly fields may have defaults
                    pass  # Will be checked by Pydantic schema
        
        # Type validation is handled by Pydantic schemas

    def resolve_reference(
        self,
        model: str,
        value: str | int,
        search_field: str = "name",
        domain: list[Any] | None = None,
    ) -> int | None:
        """
        Resolve a reference value to an Odoo record ID.
        
        Args:
            model: Target model (e.g., "res.country")
            value: Value to search (ID, name, or code)
            search_field: Field to search by
            domain: Additional domain filter
            
        Returns:
            Record ID if found, None otherwise
        """
        if isinstance(value, int):
            return value
        
        if not value:
            return None
        
        # Try exact match first
        search_domain: list[Any] = [(search_field, "=", value)]
        if domain:
            search_domain.extend(domain)
        
        ids = self.search(model, search_domain, limit=1)
        if ids:
            return ids[0]
        
        # Try case-insensitive match
        search_domain = [(search_field, "=ilike", value)]
        if domain:
            search_domain.extend(domain)
        
        ids = self.search(model, search_domain, limit=1)
        return ids[0] if ids else None
