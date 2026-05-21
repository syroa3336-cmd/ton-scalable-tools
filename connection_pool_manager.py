"""
Connection Pool Manager - Optimized resource management for TON operations
Provides intelligent pooling, failover handling, and connection health monitoring.
"""

import asyncio
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Set
from datetime import datetime, timedelta
from enum import Enum
import logging

from ton_core import NetworkGlobalID
from tonutils.clients import ToncenterClient


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectionStatus(Enum):
    """Connection health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CLOSED = "closed"


@dataclass
class PoolConnection:
    """Managed pool connection"""
    client: ToncenterClient
    created_at: datetime = field(default_factory=datetime.now)
    last_used: datetime = field(default_factory=datetime.now)
    status: ConnectionStatus = ConnectionStatus.HEALTHY
    request_count: int = 0
    error_count: int = 0
    health_check_count: int = 0
    in_use: bool = False
    
    def is_healthy(self, max_errors: int = 5, error_window_seconds: int = 60) -> bool:
        """Check if connection is healthy"""
        # Check if too many errors in recent window
        if self.error_count >= max_errors:
            return False
        
        # Check connection age
        age = (datetime.now() - self.created_at).total_seconds()
        if age > 3600:  # 1 hour max connection lifetime
            return False
        
        return True


class ConnectionPoolManager:
    """
    Advanced connection pool with:
    - Automatic failover and reconnection
    - Health monitoring and self-healing
    - Intelligent request routing
    - Connection lifecycle management
    - Real-time statistics
    """
    
    def __init__(self,
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 pool_size: int = 10,
                 health_check_interval: float = 30.0,
                 health_check_timeout: float = 5.0):
        """
        Initialize connection pool manager
        
        Args:
            network: TON network
            pool_size: Number of connections in pool
            health_check_interval: Seconds between health checks
            health_check_timeout: Timeout for health check
        """
        self.network = network
        self.pool_size = pool_size
        self.health_check_interval = health_check_interval
        self.health_check_timeout = health_check_timeout
        
        self.connections: List[PoolConnection] = []
        self.available_connections: asyncio.Queue = asyncio.Queue()
        self.lock = asyncio.Lock()
        
        self.is_running = False
        self._health_monitor_task: Optional[asyncio.Task] = None
        
        # Statistics
        self.total_requests = 0
        self.total_errors = 0
        self.pool_hits = 0
        self.pool_misses = 0
    
    async def initialize(self) -> None:
        """Initialize the connection pool"""
        async with self.lock:
            for i in range(self.pool_size):
                client = ToncenterClient(network=self.network)
                connection = PoolConnection(client=client)
                self.connections.append(connection)
                await self.available_connections.put(connection)
            
            logger.info(f"Connection pool initialized with {self.pool_size} connections")
        
        self.is_running = True
        self._health_monitor_task = asyncio.create_task(self._health_monitor_loop())
    
    async def get_connection(self, timeout: float = 10.0) -> Optional[PoolConnection]:
        """
        Get an available connection from pool
        Implements intelligent retrieval with fallback
        """
        try:
            # Try to get available connection
            connection = await asyncio.wait_for(
                self.available_connections.get(),
                timeout=timeout
            )
            
            connection.in_use = True
            connection.last_used = datetime.now()
            connection.request_count += 1
            self.total_requests += 1
            self.pool_hits += 1
            
            logger.debug(f"Connection acquired from pool (available: {self.available_connections.qsize()})")
            
            return connection
        
        except asyncio.TimeoutError:
            logger.warning("No connections available in pool, creating new connection")
            self.pool_misses += 1
            
            # Create temporary connection if pool exhausted
            try:
                client = ToncenterClient(network=self.network)
                connection = PoolConnection(client=client)
                connection.in_use = True
                connection.request_count += 1
                self.total_requests += 1
                return connection
            except Exception as e:
                logger.error(f"Failed to create temporary connection: {e}")
                return None
    
    async def return_connection(self, connection: PoolConnection) -> None:
        """Return connection to pool"""
        if not connection:
            return
        
        connection.in_use = False
        
        # Check if connection is still healthy
        if connection.status == ConnectionStatus.HEALTHY:
            await self.available_connections.put(connection)
            logger.debug("Connection returned to pool")
        else:
            logger.warning(f"Connection returned but status is {connection.status}, will be recreated")
            # Don't return unhealthy connections to pool
            async with self.lock:
                self.connections.remove(connection)
                # Create replacement
                new_client = ToncenterClient(network=self.network)
                new_connection = PoolConnection(client=new_client)
                self.connections.append(new_connection)
                await self.available_connections.put(new_connection)
    
    async def record_error(self, connection: PoolConnection) -> None:
        """Record an error on a connection"""
        connection.error_count += 1
        self.total_errors += 1
        
        if connection.error_count >= 5:
            connection.status = ConnectionStatus.UNHEALTHY
            logger.warning(f"Connection marked as unhealthy after {connection.error_count} errors")
    
    async def _health_monitor_loop(self) -> None:
        """Monitor connection health and perform healing"""
        while self.is_running:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self._perform_health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    
    async def _perform_health_check(self) -> None:
        """Perform health checks on all connections"""
        async with self.lock:
            unhealthy_count = 0
            
            for connection in self.connections:
                if connection.in_use:
                    continue
                
                # Reset error count periodically (healing)
                if (datetime.now() - connection.last_used).total_seconds() > 300:
                    connection.error_count = max(0, connection.error_count - 1)
                
                # Update status based on health
                if connection.is_healthy():
                    if connection.status != ConnectionStatus.HEALTHY:
                        connection.status = ConnectionStatus.HEALTHY
                        logger.info(f"Connection recovered to healthy status")
                else:
                    if connection.status == ConnectionStatus.HEALTHY:
                        connection.status = ConnectionStatus.UNHEALTHY
                        unhealthy_count += 1
                
                connection.health_check_count += 1
            
            if unhealthy_count > 0:
                logger.warning(f"Health check found {unhealthy_count} unhealthy connections")
    
    async def close(self) -> None:
        """Close all connections"""
        self.is_running = False
        
        if self._health_monitor_task:
            self._health_monitor_task.cancel()
            try:
                await self._health_monitor_task
            except asyncio.CancelledError:
                pass
        
        async with self.lock:
            for connection in self.connections:
                try:
                    await connection.client.close()
                except:
                    pass
                connection.status = ConnectionStatus.CLOSED
        
        logger.info("Connection pool closed")
    
    def get_statistics(self) -> Dict:
        """Get pool statistics"""
        total_available = self.available_connections.qsize()
        total_in_use = sum(1 for c in self.connections if c.in_use)
        healthy = sum(1 for c in self.connections if c.status == ConnectionStatus.HEALTHY)
        
        return {
            'total_connections': len(self.connections),
            'available': total_available,
            'in_use': total_in_use,
            'healthy': healthy,
            'total_requests': self.total_requests,
            'total_errors': self.total_errors,
            'pool_hits': self.pool_hits,
            'pool_misses': self.pool_misses,
            'hit_rate': (self.pool_hits / max(self.pool_hits + self.pool_misses, 1)) * 100,
            'error_rate': (self.total_errors / max(self.total_requests, 1)) * 100,
        }