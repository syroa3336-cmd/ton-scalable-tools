"""
Scalable Fund Sender for TON Network
Supports single and batch fund distribution with advanced routing.
"""

import asyncio
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from enum import Enum
import json
from pathlib import Path
from datetime import datetime

from ton_core import NetworkGlobalID, to_nano, Address
from tonutils.clients import ToncenterClient
from tonutils.contracts import WalletV4R2


class TransactionStatus(Enum):
    """Transaction status tracking"""
    PENDING = "pending"
    SENT = "sent"
    CONFIRMED = "confirmed"
    FAILED = "failed"


@dataclass
class TransactionResult:
    """Store transaction result"""
    sender: str
    recipient: str
    amount: float
    transaction_hash: str
    status: TransactionStatus
    error: Optional[str] = None
    timestamp: str = ""
    attempts: int = 0


class FundSender:
    """Single fund transfer"""
    
    def __init__(self, network: NetworkGlobalID = NetworkGlobalID.MAINNET):
        self.network = network
        self.client = ToncenterClient(network=network)
    
    async def send_funds(self,
                        sender_mnemonic: str,
                        recipient_address: str,
                        amount: float) -> TransactionResult:
        """
        Send funds from one wallet to another
        
        Args:
            sender_mnemonic: Sender wallet mnemonic
            recipient_address: Recipient wallet address
            amount: Amount in TON to send
            
        Returns:
            TransactionResult with status and transaction hash
        """
        try:
            await self.client.connect()
            
            # Create sender wallet
            wallet, _, _, _ = WalletV4R2.from_mnemonic(self.client, sender_mnemonic)
            sender_address = wallet.address.to_str(is_bounceable=False)
            
            # Send transfer
            msg = await wallet.transfer(
                destination=Address(recipient_address),
                amount=to_nano(amount)
            )
            
            await self.client.close()
            
            return TransactionResult(
                sender=sender_address,
                recipient=recipient_address,
                amount=amount,
                transaction_hash=msg.normalized_hash,
                status=TransactionStatus.SENT,
                timestamp=datetime.now().isoformat(),
                attempts=1
            )
        
        except Exception as e:
            return TransactionResult(
                sender="",
                recipient=recipient_address,
                amount=amount,
                transaction_hash="",
                status=TransactionStatus.FAILED,
                error=str(e),
                timestamp=datetime.now().isoformat(),
                attempts=1
            )


class ScalableFundSender:
    """Batch fund distribution with advanced routing and optimization"""
    
    def __init__(self,
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 max_concurrent: int = 5,
                 retry_attempts: int = 3):
        """
        Initialize scalable fund sender
        
        Args:
            network: TON network (MAINNET or TESTNET)
            max_concurrent: Maximum concurrent transfers
            retry_attempts: Number of retry attempts per transfer
        """
        self.network = network
        self.max_concurrent = max_concurrent
        self.retry_attempts = retry_attempts
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.clients = [ToncenterClient(network=network) for _ in range(max_concurrent)]
    
    async def send_funds_async(self,
                              sender_mnemonic: str,
                              recipient_address: str,
                              amount: float,
                              client_id: int) -> TransactionResult:
        """Send funds asynchronously"""
        async with self.semaphore:
            client = self.clients[client_id % len(self.clients)]
            attempts = 0
            
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
                    
                    return TransactionResult(
                        sender=sender_address,
                        recipient=recipient_address,
                        amount=amount,
                        transaction_hash=msg.normalized_hash,
                        status=TransactionStatus.SENT,
                        timestamp=datetime.now().isoformat(),
                        attempts=attempts
                    )
                
                except Exception as e:
                    if attempt == self.retry_attempts - 1:
                        return TransactionResult(
                            sender="",
                            recipient=recipient_address,
                            amount=amount,
                            transaction_hash="",
                            status=TransactionStatus.FAILED,
                            error=str(e),
                            timestamp=datetime.now().isoformat(),
                            attempts=attempts
                        )
                    await asyncio.sleep(2 ** attempt)
    
    async def send_to_multiple_recipients(self,
                                         sender_mnemonic: str,
                                         recipients: Dict[str, float]) -> List[TransactionResult]:
        """
        Send funds to multiple recipients from one sender
        
        Args:
            sender_mnemonic: Sender wallet mnemonic
            recipients: Dict of {recipient_address: amount_in_ton}
            
        Returns:
            List of TransactionResults
        """
        tasks = [
            self.send_funds_async(
                sender_mnemonic=sender_mnemonic,
                recipient_address=recipient,
                amount=amount,
                client_id=i
            )
            for i, (recipient, amount) in enumerate(recipients.items())
        ]
        return await asyncio.gather(*tasks)
    
    async def distribute_from_multiple_senders(self,
                                              senders: Dict[str, str],
                                              recipients: List[str],
                                              amount_per_recipient: float) -> List[TransactionResult]:
        """
        Distribute funds from multiple senders to multiple recipients
        Uses round-robin distribution for load balancing
        
        Args:
            senders: Dict of {name: mnemonic}
            recipients: List of recipient addresses
            amount_per_recipient: Amount to send to each recipient
            
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
                client_id=i
            )
            all_results.append(result)
        
        return all_results
    
    def save_transaction_results(self,
                                results: List[TransactionResult],
                                filename: str = "transactions.json") -> None:
        """Save transaction results to JSON file"""
        data = {
            'transactions': [
                {
                    'sender': r.sender,
                    'recipient': r.recipient,
                    'amount': r.amount,
                    'transaction_hash': r.transaction_hash,
                    'status': r.status.value,
                    'attempts': r.attempts,
                    'timestamp': r.timestamp,
                    'error': r.error
                }
                for r in results
            ],
            'summary': {
                'total': len(results),
                'sent': sum(1 for r in results if r.status in [TransactionStatus.SENT, TransactionStatus.CONFIRMED]),
                'failed': sum(1 for r in results if r.status == TransactionStatus.FAILED),
                'total_amount': sum(r.amount for r in results if r.status != TransactionStatus.FAILED)
            }
        }
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Transaction results saved to {filename}")


# Example usage
async def main():
    print("=" * 60)
    print("TON SCALABLE FUND SENDER")
    print("=" * 60)
    
    # Example data
    sender_mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
    recipients = {
        "UQCZq3_Vd21-4y4m7Wc-ej9NFOhh_qvdfAkAYAOHoQ__Ness": 0.1,
        "UQCN0L1Dn0I0QEVzZ_0LJdhIFHh0rRQVGJ5TIBHHhb3XZST": 0.05,
    }
    
    print("\n💰 Sending funds to multiple recipients...")
    sender = ScalableFundSender(
        network=NetworkGlobalID.TESTNET,
        max_concurrent=3
    )
    
    results = await sender.send_to_multiple_recipients(
        sender_mnemonic=sender_mnemonic,
        recipients=recipients
    )
    
    print(f"\n📊 Transaction Summary:")
    successful = sum(1 for r in results if r.status == TransactionStatus.SENT)
    failed = sum(1 for r in results if r.status == TransactionStatus.FAILED)
    total_sent = sum(r.amount for r in results if r.status == TransactionStatus.SENT)
    
    print(f"  Total Transactions: {len(results)}")
    print(f"  Sent: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total Amount: {total_sent} TON")
    
    print(f"\nDetails:")
    for i, result in enumerate(results):
        print(f"  {i+1}. {result.recipient[:20]}...")
        print(f"     Amount: {result.amount} TON")
        print(f"     Status: {result.status.value}")
        if result.error:
            print(f"     Error: {result.error}")
    
    sender.save_transaction_results(
        results,
        "output/transactions.json"
    )


if __name__ == "__main__":
    asyncio.run(main())