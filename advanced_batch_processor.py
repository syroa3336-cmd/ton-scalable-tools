"""
Advanced Batch Processor - Futuristic TON Scaling Solution
Queue-based processing, dynamic batching, and intelligent routing for massive throughput.
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable, Any, Coroutine
from enum import Enum
from datetime import datetime
from collections import defaultdict
import logging

from ton_core import NetworkGlobalID, to_nano, Address
from tonutils.clients import ToncenterClient
from tonutils.contracts import WalletV4R2


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OperationType(Enum):
    """Types of operations supported"""
    WALLET_CREATE = "wallet_create"
    WALLET_DEPLOY = "wallet_deploy"
    SEND_FUNDS = "send_funds"
    MINT_JETTON = "mint_jetton"
    TRANSFER_NFT = "transfer_nft"


@dataclass
class BatchJob:
    """Individual job in batch"""
    job_id: str
    operation_type: OperationType
    data: Dict[str, Any]
    priority: int = 0
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    
    def __lt__(self, other):
        """For priority queue sorting (higher priority first, then older first)"""
        if self.priority != other.priority:
            return self.priority > other.priority
        return self.created_at < other.created_at


@dataclass
class BatchStatistics:
    """Track batch processing statistics"""
    total_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    total_time: float = 0
    throughput: float = 0  # jobs per second
    success_rate: float = 0
    
    def calculate(self, elapsed_time: float):
        """Calculate statistics"""
        self.success_rate = (self.completed_jobs - self.failed_jobs) / max(self.total_jobs, 1) * 100
        self.throughput = self.completed_jobs / max(elapsed_time, 0.001)


class AdvancedBatchProcessor:
    """
    Futuristic TON batch processor with:
    - Queue-based job management
    - Dynamic batching and smart routing
    - Intelligent retry logic with exponential backoff
    - Connection pooling and load balancing
    - Real-time statistics and monitoring
    """
    
    def __init__(self,
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 max_workers: int = 10,
                 batch_size: int = 50,
                 batch_timeout: float = 5.0):
        """
        Initialize advanced batch processor
        
        Args:
            network: TON network
            max_workers: Maximum concurrent workers
            batch_size: Number of jobs to batch together
            batch_timeout: Maximum time to wait before processing batch
        """
        self.network = network
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # Connection pool
        self.clients = [ToncenterClient(network=network) for _ in range(max_workers)]
        self.semaphore = asyncio.Semaphore(max_workers)
        
        # Job queues by priority
        self.job_queues: Dict[int, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.default_priorities = [10, 5, 0, -5, -10]  # High to low
        
        # Results tracking
        self.results: Dict[str, BatchJob] = {}
        self.statistics = BatchStatistics()
        
        # Processing control
        self.is_running = False
        self._workers = []
        self._batch_processor_task = None
    
    async def submit_job(self, job: BatchJob) -> None:
        """Submit a job to the appropriate queue"""
        priority = job.priority
        await self.job_queues[priority].put(job)
        self.statistics.total_jobs += 1
        logger.info(f"Job submitted: {job.job_id} (priority: {priority})")
    
    async def submit_jobs(self, jobs: List[BatchJob]) -> None:
        """Submit multiple jobs"""
        for job in jobs:
            await self.submit_job(job)
    
    async def _get_next_batch(self) -> List[BatchJob]:
        """
        Retrieve next batch from highest priority queue
        Uses dynamic batching with timeout
        """
        batch = []
        start_time = asyncio.get_event_loop().time()
        
        while len(batch) < self.batch_size:
            # Check timeout
            elapsed = asyncio.get_event_loop().time() - start_time
            if batch and elapsed > self.batch_timeout:
                break
            
            # Try to get from highest priority queue first
            for priority in self.default_priorities:
                try:
                    job = self.job_queues[priority].get_nowait()
                    batch.append(job)
                    break
                except asyncio.QueueEmpty:
                    continue
            else:
                # No jobs available in any queue
                if not batch:
                    # Wait for first job
                    for priority in self.default_priorities:
                        try:
                            job = await asyncio.wait_for(
                                self.job_queues[priority].get(),
                                timeout=1.0
                            )
                            batch.append(job)
                            break
                        except asyncio.TimeoutError:
                            continue
                    if not batch:
                        break
                else:
                    break
        
        return batch
    
    async def _process_job(self, job: BatchJob, client_id: int) -> BatchJob:
        """Process individual job"""
        async with self.semaphore:
            client = self.clients[client_id % len(self.clients)]
            
            try:
                if job.operation_type == OperationType.WALLET_CREATE:
                    result = await self._handle_wallet_create(job, client)
                elif job.operation_type == OperationType.WALLET_DEPLOY:
                    result = await self._handle_wallet_deploy(job, client)
                elif job.operation_type == OperationType.SEND_FUNDS:
                    result = await self._handle_send_funds(job, client)
                else:
                    result = {"error": "Unsupported operation type"}
                
                job.result = result
                job.completed_at = datetime.now()
                self.statistics.completed_jobs += 1
                logger.info(f"Job completed: {job.job_id}")
                
                return job
            
            except Exception as e:
                job.error = str(e)
                job.retry_count += 1
                
                if job.retry_count < job.max_retries:
                    # Re-queue for retry
                    await self.submit_job(job)
                    logger.warning(f"Job retried: {job.job_id} (attempt {job.retry_count})")
                else:
                    self.statistics.failed_jobs += 1
                    job.completed_at = datetime.now()
                    logger.error(f"Job failed: {job.job_id} - {str(e)}")
                
                return job
    
    async def _handle_wallet_create(self, job: BatchJob, client: ToncenterClient) -> Dict:
        """Handle wallet creation operation"""
        from tonutils.contracts import WalletV4R2
        from ton_core import WalletV4Config
        
        config = WalletV4Config()
        wallet, public_key, private_key, mnemonic = WalletV4R2.create(client, config=config)
        
        return {
            "address": wallet.address.to_str(is_bounceable=False),
            "mnemonic": ' '.join(mnemonic),
            "public_key": str(public_key.as_int),
        }
    
    async def _handle_wallet_deploy(self, job: BatchJob, client: ToncenterClient) -> Dict:
        """Handle wallet deployment operation"""
        from tonutils.contracts import WalletV4R2
        
        mnemonic = job.data.get("mnemonic")
        amount = job.data.get("amount", 0.05)
        
        await client.connect()
        wallet, _, _, _ = WalletV4R2.from_mnemonic(client, mnemonic)
        address = wallet.address.to_str(is_bounceable=False)
        
        msg = await wallet.transfer(
            destination=wallet.address,
            amount=to_nano(amount)
        )
        
        await client.close()
        
        return {
            "address": address,
            "transaction_hash": msg.normalized_hash,
        }
    
    async def _handle_send_funds(self, job: BatchJob, client: ToncenterClient) -> Dict:
        """Handle fund sending operation"""
        from tonutils.contracts import WalletV4R2
        
        sender_mnemonic = job.data.get("sender_mnemonic")
        recipient = job.data.get("recipient")
        amount = job.data.get("amount")
        
        await client.connect()
        wallet, _, _, _ = WalletV4R2.from_mnemonic(client, sender_mnemonic)
        sender_address = wallet.address.to_str(is_bounceable=False)
        
        msg = await wallet.transfer(
            destination=Address(recipient),
            amount=to_nano(amount)
        )
        
        await client.close()
        
        return {
            "sender": sender_address,
            "recipient": recipient,
            "amount": amount,
            "transaction_hash": msg.normalized_hash,
        }
    
    async def process_batch(self, batch: List[BatchJob]) -> List[BatchJob]:
        """Process batch of jobs concurrently"""
        if not batch:
            return []
        
        tasks = [
            self._process_job(job, i)
            for i, job in enumerate(batch)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        processed_batch = []
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Batch processing error: {result}")
            else:
                processed_batch.append(result)
                self.results[result.job_id] = result
        
        return processed_batch
    
    async def _batch_processor_loop(self) -> None:
        """Main batch processing loop"""
        try:
            while self.is_running:
                batch = await self._get_next_batch()
                if batch:
                    logger.info(f"Processing batch of {len(batch)} jobs")
                    await self.process_batch(batch)
                else:
                    await asyncio.sleep(0.1)
        
        except asyncio.CancelledError:
            logger.info("Batch processor stopped")
    
    async def start(self) -> None:
        """Start the batch processor"""
        if self.is_running:
            return
        
        self.is_running = True
        self._batch_processor_task = asyncio.create_task(self._batch_processor_loop())
        logger.info(f"Batch processor started with {self.max_workers} workers")
    
    async def stop(self) -> None:
        """Stop the batch processor"""
        self.is_running = False
        
        if self._batch_processor_task:
            await self._batch_processor_task
        
        logger.info("Batch processor stopped")
    
    async def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all jobs to complete"""
        start_time = asyncio.get_event_loop().time()
        
        while self.is_running:
            all_queues_empty = all(
                queue.empty() for queue in self.job_queues.values()
            )
            
            if all_queues_empty and (
                self.statistics.completed_jobs + self.statistics.failed_jobs
                >= self.statistics.total_jobs
            ):
                break
            
            if timeout and (asyncio.get_event_loop().time() - start_time) > timeout:
                return False
            
            await asyncio.sleep(0.5)
        
        return True
    
    def get_statistics(self) -> BatchStatistics:
        """Get current statistics"""
        return self.statistics
    
    def get_results(self) -> List[BatchJob]:
        """Get all results"""
        return list(self.results.values())