"""
Advanced Fund Sender v1.1 - Enhanced Edition
Send and distribute funds across multiple wallets at scale.
NEW: Smart routing, transaction batching, and distribution analytics
"""

import asyncio
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
import json
from pathlib import Path
from datetime import datetime
import logging

from ton_core import NetworkGlobalID, to_nano, Address
from tonutils.clients import ToncenterClient
from tonutils.contracts import WalletV4R2

logger = logging.getLogger(__name__)


class TransactionStatus(Enum):
    """Transaction status tracking"""
    PENDING = "pending"
    SENT = "sent"
    CONFIRMED = "confirmed"
    FAILED = "failed"


class DistributionMode(Enum):
    """Fund distribution modes"""
    EQUAL = "equal"  # Equal distribution to all
    WEIGHTED = "weighted"  # Weighted by address
    SEQUENTIAL = "sequential"  # Sequential distribution
    ROUND_ROBIN = "round_robin"  # Round-robin from senders


@dataclass
class TransactionResult:
    """Store transaction result with analytics"""
    sender: str
    recipient: str
    amount: float
    transaction_hash: str
    status: TransactionStatus
    mode: DistributionMode
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    attempts: int = 0
    execution_time: float = 0
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributionAnalytics:
    """Track distribution analytics"""
    total_transactions: int = 0
    successful: int = 0
    failed: int = 0
    total_amount: float = 0
    avg_amount_per_transaction: float = 0
    total_execution_time: float = 0
    success_rate: float = 0
    distribution_modes: Dict[str, int] = field(default_factory=dict)


class ScalableFundSenderV1:
    """Enhanced fund sender with distribution modes"""
    
    def __init__(self,
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 max_concurrent: int = 5,
                 retry_attempts: int = 3,
                 enable_analytics: bool = True):
        self.network = network
        self.max_concurrent = max_concurrent
        self.retry_attempts = retry_attempts
        self.enable_analytics = enable_analytics
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.clients = [ToncenterClient(network=network) for _ in range(max_concurrent)]
        self.analytics = DistributionAnalytics()
    
    async def send_funds_async(self,
                              sender_mnemonic: str,
                              recipient_address: str,
                              amount: float,
                              client_id: int,
                              mode: DistributionMode = DistributionMode.EQUAL) -> TransactionResult:
        """Send funds asynchronously"""
        import time
        
        async with self.semaphore:
            client = self.clients[client_id % len(self.clients)]
            attempts = 0
            start_time = time.time()
            
            for attempt in range(self.retry_attempts):
                try:
                    attempts += 1
                    await client.connect()
                    
                    wallet, _, _, _ = WalletV4R2.from_mnemonic(client, sender_mnemonic)
                    sender_address = wallet.address.to_str(is_bounceable=False)
                    
                    msg = await wallet.transfer(
                        destination=Address(recipient_address),
                        amount=to_nano(amount)
                    )
                    
                    await client.close()
                    
                    execution_time = time.time() - start_time
                    
                    return TransactionResult(
                        sender=sender_address,
                        recipient=recipient_address,
                        amount=amount,
                        transaction_hash=msg.normalized_hash,
                        status=TransactionStatus.SENT,
                        mode=mode,
                        attempts=attempts,
                        execution_time=execution_time
                    )
                
                except Exception as e:
                    if attempt == self.retry_attempts - 1:
                        return TransactionResult(
                            sender="",
                            recipient=recipient_address,
                            amount=amount,
                            transaction_hash="",
                            status=TransactionStatus.FAILED,
                            mode=mode,
                            error=str(e),
                            attempts=attempts,
                            execution_time=time.time() - start_time
                        )
                    await asyncio.sleep(2 ** attempt)
    
    async def send_to_multiple_recipients(self,
                                         sender_mnemonic: str,
                                         recipients: Dict[str, float],
                                         mode: DistributionMode = DistributionMode.EQUAL) -> List[TransactionResult]:
        """
        Send funds to multiple recipients with distribution mode
        
        Args:
            sender_mnemonic: Sender wallet mnemonic
            recipients: Dict of {recipient_address: amount_in_ton}
            mode: Distribution mode
            
        Returns:
            List of TransactionResults
        """
        import time
        start_time = time.time()
        
        tasks = [
            self.send_funds_async(
                sender_mnemonic=sender_mnemonic,
                recipient_address=recipient,
                amount=amount,
                client_id=i,
                mode=mode
            )
            for i, (recipient, amount) in enumerate(recipients.items())
        ]
        
        results = await asyncio.gather(*tasks)
        elapsed_time = time.time() - start_time
        
        # Update analytics
        successful = sum(1 for r in results if r.status == TransactionStatus.SENT)
        failed = sum(1 for r in results if r.status == TransactionStatus.FAILED)
        total_amount = sum(r.amount for r in results if r.status == TransactionStatus.SENT)
        
        self.analytics.total_transactions += len(results)
        self.analytics.successful += successful
        self.analytics.failed += failed
        self.analytics.total_amount += total_amount
        self.analytics.total_execution_time += elapsed_time
        self.analytics.success_rate = (successful / max(len(results), 1)) * 100
        self.analytics.distribution_modes[mode.value] = self.analytics.distribution_modes.get(mode.value, 0) + len(results)
        
        logger.info(f"✅ Sent {successful}/{len(results)} transactions in {elapsed_time:.2f}s")
        logger.info(f"   Total amount: {total_amount:.4f} TON")
        logger.info(f"   Success rate: {self.analytics.success_rate:.2f}%")
        
        return results
    
    async def distribute_from_multiple_senders(self,
                                              senders: Dict[str, str],
                                              recipients: List[str],
                                              amount_per_recipient: float,
                                              mode: DistributionMode = DistributionMode.ROUND_ROBIN) -> List[TransactionResult]:
        """
        Distribute funds from multiple senders to multiple recipients
        
        Args:
            senders: Dict of {name: mnemonic}
            recipients: List of recipient addresses
            amount_per_recipient: Amount to send to each recipient
            mode: Distribution mode
            
        Returns:
            List of all TransactionResults
        """
        all_results = []
        sender_list = list(senders.items())
        
        for i, recipient in enumerate(recipients):
            sender_name, sender_mnemonic = sender_list[i % len(sender_list)]
            
            result = await self.send_funds_async(
                sender_mnemonic=sender_mnemonic,
                recipient_address=recipient,
                amount=amount_per_recipient,
                client_id=i,
                mode=mode
            )
            all_results.append(result)
        
        return all_results
    
    def save_transaction_results(self,
                                results: List[TransactionResult],
                                filename: str = "transactions_v1.1.json") -> None:
        """Save transaction results with analytics"""
        data = {
            'version': '1.1',
            'generated_at': datetime.now().isoformat(),
            'analytics': {
                'total': len(results),
                'successful': sum(1 for r in results if r.status == TransactionStatus.SENT),
                'failed': sum(1 for r in results if r.status == TransactionStatus.FAILED),
                'total_amount': sum(r.amount for r in results),
                'success_rate': self.analytics.success_rate,
                'distribution_modes': self.analytics.distribution_modes
            },
            'transactions': [
                {
                    'sender': r.sender,
                    'recipient': r.recipient,
                    'amount': r.amount,
                    'transaction_hash': r.transaction_hash,
                    'status': r.status.value,
                    'mode': r.mode.value,
                    'attempts': r.attempts,
                    'execution_time': r.execution_time,
                    'timestamp': r.timestamp,
                    'error': r.error
                }
                for r in results
            ]
        }
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Transaction results saved to {filename}")
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get distribution analytics"""
        return {
            'total_transactions': self.analytics.total_transactions,
            'successful': self.analytics.successful,
            'failed': self.analytics.failed,
            'total_amount': self.analytics.total_amount,
            'success_rate': self.analytics.success_rate,
            'distribution_modes': self.analytics.distribution_modes
        }


# Example usage
async def main():
    print("=" * 70)
    print("🚀 TON SCALABLE FUND SENDER v1.1 - Enhanced Edition")
    print("=" * 70)
    
    sender = ScalableFundSenderV1(
        network=NetworkGlobalID.TESTNET,
        max_concurrent=5
    )
    
    sender_mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
    recipients = {
        "UQCZq3_Vd21-4y4m7Wc-ej9NFOhh_qvdfAkAYAOHoQ__Ness": 0.1,
        "UQCN0L1Dn0I0QEVzZ_0LJdhIFHh0rRQVGJ5TIBHHhb3XZST": 0.05,
    }
    
    print("\n💰 Sending funds with EQUAL distribution mode...")
    results = await sender.send_to_multiple_recipients(
        sender_mnemonic=sender_mnemonic,
        recipients=recipients,
        mode=DistributionMode.EQUAL
    )
    
    print(f"\n" + "=" * 70)
    print("📊 Distribution Analytics")
    print("=" * 70)
    
    analytics = sender.get_analytics()
    for key, value in analytics.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")
    
    sender.save_transaction_results(
        results,
        "version/v1.1/output/transactions_v1.1.json"
    )


if __name__ == "__main__":
    asyncio.run(main())