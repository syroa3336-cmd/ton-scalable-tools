"""
Scalable Wallet Deployer for TON Network
Supports single and batch wallet deployment with async processing.
"""

import asyncio
from dataclasses import dataclass
from typing import List, Optional, Dict
from enum import Enum
import json
from pathlib import Path

from ton_core import NetworkGlobalID, to_nano
from tonutils.clients import ToncenterClient
from tonutils.contracts import WalletV4R2


class DeploymentStatus(Enum):
    """Deployment status tracking"""
    PENDING = "pending"
    DEPLOYING = "deploying"
    DEPLOYED = "deployed"
    FAILED = "failed"


@dataclass
class DeploymentResult:
    """Store deployment result"""
    address: str
    transaction_hash: str
    status: DeploymentStatus
    error: Optional[str] = None
    attempts: int = 0


class WalletDeployer:
    """Single wallet deployment"""
    
    def __init__(self, network: NetworkGlobalID = NetworkGlobalID.MAINNET):
        self.network = network
        self.client = ToncenterClient(network=network)
    
    async def deploy_wallet(self, 
                           mnemonic: str,
                           amount: float = 0.05) -> DeploymentResult:
        """
        Deploy a single wallet
        
        Args:
            mnemonic: Wallet mnemonic phrase
            amount: TON amount to send for deployment
            
        Returns:
            DeploymentResult with status and transaction hash
        """
        try:
            await self.client.connect()
            
            # Create wallet from mnemonic
            wallet, _, _, _ = WalletV4R2.from_mnemonic(self.client, mnemonic)
            address = wallet.address.to_str(is_bounceable=False)
            
            # Deploy wallet by sending transaction to self
            msg = await wallet.transfer(
                destination=wallet.address,
                amount=to_nano(amount)
            )
            
            await self.client.close()
            
            return DeploymentResult(
                address=address,
                transaction_hash=msg.normalized_hash,
                status=DeploymentStatus.DEPLOYED,
                attempts=1
            )
        
        except Exception as e:
            return DeploymentResult(
                address="",
                transaction_hash="",
                status=DeploymentStatus.FAILED,
                error=str(e),
                attempts=1
            )


class ScalableWalletDeployer:
    """Batch wallet deployment with async processing and connection pooling"""
    
    def __init__(self,
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 max_concurrent: int = 5,
                 retry_attempts: int = 3):
        """
        Initialize scalable wallet deployer
        
        Args:
            network: TON network (MAINNET or TESTNET)
            max_concurrent: Maximum concurrent deployments
            retry_attempts: Number of retry attempts per deployment
        """
        self.network = network
        self.max_concurrent = max_concurrent
        self.retry_attempts = retry_attempts
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.clients = [ToncenterClient(network=network) for _ in range(max_concurrent)]
    
    async def deploy_wallet_async(self,
                                  mnemonic: str,
                                  client_id: int,
                                  amount: float = 0.05) -> DeploymentResult:
        """Deploy single wallet asynchronously"""
        async with self.semaphore:
            client = self.clients[client_id % len(self.clients)]
            attempts = 0
            
            for attempt in range(self.retry_attempts):
                try:
                    attempts += 1
                    await client.connect()
                    
                    wallet, _, _, _ = WalletV4R2.from_mnemonic(client, mnemonic)
                    address = wallet.address.to_str(is_bounceable=False)
                    
                    msg = await wallet.transfer(
                        destination=wallet.address,
                        amount=to_nano(amount)
                    )
                    
                    await client.close()
                    
                    return DeploymentResult(
                        address=address,
                        transaction_hash=msg.normalized_hash,
                        status=DeploymentStatus.DEPLOYED,
                        attempts=attempts
                    )
                
                except Exception as e:
                    if attempt == self.retry_attempts - 1:
                        return DeploymentResult(
                            address="",
                            transaction_hash="",
                            status=DeploymentStatus.FAILED,
                            error=str(e),
                            attempts=attempts
                        )
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    async def deploy_multiple_wallets(self,
                                     mnemonics: List[str],
                                     amount: float = 0.05) -> List[DeploymentResult]:
        """
        Deploy multiple wallets concurrently
        
        Args:
            mnemonics: List of wallet mnemonics
            amount: TON amount per deployment
            
        Returns:
            List of DeploymentResults
        """
        tasks = [
            self.deploy_wallet_async(
                mnemonic=mnemonic,
                client_id=i,
                amount=amount
            )
            for i, mnemonic in enumerate(mnemonics)
        ]
        return await asyncio.gather(*tasks)
    
    def save_deployment_results(self,
                               results: List[DeploymentResult],
                               filename: str = "deployments.json") -> None:
        """Save deployment results to JSON file"""
        data = {
            'deployments': [
                {
                    'address': r.address,
                    'transaction_hash': r.transaction_hash,
                    'status': r.status.value,
                    'attempts': r.attempts,
                    'error': r.error
                }
                for r in results
            ],
            'summary': {
                'total': len(results),
                'deployed': sum(1 for r in results if r.status == DeploymentStatus.DEPLOYED),
                'failed': sum(1 for r in results if r.status == DeploymentStatus.FAILED)
            }
        }
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Deployment results saved to {filename}")


# Example usage
async def main():
    print("=" * 60)
    print("TON SCALABLE WALLET DEPLOYER")
    print("=" * 60)
    
    # Example mnemonics (replace with real ones)
    test_mnemonics = [
        "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
        # Add more mnemonics here
    ]
    
    print("\n🚀 Deploying wallets...")
    deployer = ScalableWalletDeployer(
        network=NetworkGlobalID.TESTNET,
        max_concurrent=3,
        retry_attempts=3
    )
    
    results = await deployer.deploy_multiple_wallets(test_mnemonics, amount=0.05)
    
    print(f"\n📊 Deployment Summary:")
    successful = sum(1 for r in results if r.status == DeploymentStatus.DEPLOYED)
    failed = sum(1 for r in results if r.status == DeploymentStatus.FAILED)
    
    print(f"  Total: {len(results)}")
    print(f"  Deployed: {successful}")
    print(f"  Failed: {failed}")
    
    print(f"\nDetails:")
    for i, result in enumerate(results[:3]):
        print(f"  {i+1}. Status: {result.status.value}")
        if result.address:
            print(f"     Address: {result.address}")
        if result.error:
            print(f"     Error: {result.error}")
    
    deployer.save_deployment_results(
        results,
        "output/deployments.json"
    )


if __name__ == "__main__":
    asyncio.run(main())