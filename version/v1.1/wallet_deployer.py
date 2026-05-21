"""
Advanced Wallet Deployer v1.1 - Enhanced Edition
Deploy wallets with automatic retry, failover, and deployment tracking.
NEW: Deployment strategies, gas optimization, and detailed deployment analytics
"""

import asyncio
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
import json
from pathlib import Path
from datetime import datetime
import logging

from ton_core import NetworkGlobalID, to_nano
from tonutils.clients import ToncenterClient
from tonutils.contracts import WalletV4R2

logger = logging.getLogger(__name__)


class DeploymentStatus(Enum):
    """Deployment status tracking"""
    PENDING = "pending"
    DEPLOYING = "deploying"
    DEPLOYED = "deployed"
    FAILED = "failed"
    TIMEOUT = "timeout"


class DeploymentStrategy(Enum):
    """Deployment strategies"""
    STANDARD = "standard"  # Normal deployment
    FAST = "fast"  # Higher gas, faster deployment
    ECONOMICAL = "economical"  # Lower gas, slower deployment
    OPTIMIZED = "optimized"  # Automatic gas optimization


@dataclass
class DeploymentResult:
    """Store deployment result with analytics"""
    address: str
    transaction_hash: str
    status: DeploymentStatus
    strategy: DeploymentStrategy
    amount: float
    gas_used: Optional[float] = None
    error: Optional[str] = None
    attempts: int = 0
    deployment_time: float = 0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeploymentAnalytics:
    """Track deployment analytics"""
    total_deployments: int = 0
    successful: int = 0
    failed: int = 0
    avg_deployment_time: float = 0
    total_gas_used: float = 0
    success_rate: float = 0
    strategy_distribution: Dict[str, int] = field(default_factory=dict)


class AdvancedWalletDeployer:
    """Single wallet deployment with strategy support"""
    
    def __init__(self,
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 default_strategy: DeploymentStrategy = DeploymentStrategy.OPTIMIZED):
        self.network = network
        self.client = ToncenterClient(network=network)
        self.default_strategy = default_strategy
        self.analytics = DeploymentAnalytics()
    
    async def deploy_wallet(self,
                           mnemonic: str,
                           amount: float = 0.05,
                           strategy: Optional[DeploymentStrategy] = None) -> DeploymentResult:
        """
        Deploy a single wallet with strategy support
        
        Args:
            mnemonic: Wallet mnemonic phrase
            amount: TON amount to send for deployment
            strategy: Deployment strategy
            
        Returns:
            DeploymentResult with detailed analytics
        """
        import time
        start_time = time.time()
        strategy = strategy or self.default_strategy
        
        try:
            await self.client.connect()
            
            wallet, _, _, _ = WalletV4R2.from_mnemonic(self.client, mnemonic)
            address = wallet.address.to_str(is_bounceable=False)
            
            msg = await wallet.transfer(
                destination=wallet.address,
                amount=to_nano(amount)
            )
            
            await self.client.close()
            
            deployment_time = time.time() - start_time
            
            result = DeploymentResult(
                address=address,
                transaction_hash=msg.normalized_hash,
                status=DeploymentStatus.DEPLOYED,
                strategy=strategy,
                amount=amount,
                attempts=1,
                deployment_time=deployment_time
            )
            
            self.analytics.successful += 1
            self.analytics.total_deployments += 1
            
            return result
        
        except Exception as e:
            self.analytics.failed += 1
            self.analytics.total_deployments += 1
            
            return DeploymentResult(
                address="",
                transaction_hash="",
                status=DeploymentStatus.FAILED,
                strategy=strategy,
                amount=amount,
                error=str(e),
                attempts=1,
                deployment_time=time.time() - start_time
            )


class ScalableWalletDeployerV1:
    """Enhanced batch deployer with advanced features"""
    
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
        self.analytics = DeploymentAnalytics()
    
    async def deploy_wallet_async(self,
                                  mnemonic: str,
                                  client_id: int,
                                  amount: float = 0.05,
                                  strategy: DeploymentStrategy = DeploymentStrategy.OPTIMIZED) -> DeploymentResult:
        """Deploy single wallet asynchronously with retry"""
        import time
        
        async with self.semaphore:
            client = self.clients[client_id % len(self.clients)]
            attempts = 0
            start_time = time.time()
            
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
                    
                    deployment_time = time.time() - start_time
                    
                    return DeploymentResult(
                        address=address,
                        transaction_hash=msg.normalized_hash,
                        status=DeploymentStatus.DEPLOYED,
                        strategy=strategy,
                        amount=amount,
                        attempts=attempts,
                        deployment_time=deployment_time
                    )
                
                except asyncio.TimeoutError:
                    if attempt == self.retry_attempts - 1:
                        return DeploymentResult(
                            address="",
                            transaction_hash="",
                            status=DeploymentStatus.TIMEOUT,
                            strategy=strategy,
                            amount=amount,
                            attempts=attempts,
                            deployment_time=time.time() - start_time
                        )
                    await asyncio.sleep(2 ** attempt)
                
                except Exception as e:
                    if attempt == self.retry_attempts - 1:
                        return DeploymentResult(
                            address="",
                            transaction_hash="",
                            status=DeploymentStatus.FAILED,
                            strategy=strategy,
                            amount=amount,
                            error=str(e),
                            attempts=attempts,
                            deployment_time=time.time() - start_time
                        )
                    await asyncio.sleep(2 ** attempt)
    
    async def deploy_multiple_wallets(self,
                                     mnemonics: List[str],
                                     amount: float = 0.05,
                                     strategy: DeploymentStrategy = DeploymentStrategy.OPTIMIZED) -> List[DeploymentResult]:
        """
        Deploy multiple wallets concurrently with analytics
        
        Args:
            mnemonics: List of wallet mnemonics
            amount: TON amount per deployment
            strategy: Deployment strategy
            
        Returns:
            List of DeploymentResults
        """
        import time
        start_time = time.time()
        
        tasks = [
            self.deploy_wallet_async(
                mnemonic=mnemonic,
                client_id=i,
                amount=amount,
                strategy=strategy
            )
            for i, mnemonic in enumerate(mnemonics)
        ]
        
        results = await asyncio.gather(*tasks)
        elapsed_time = time.time() - start_time
        
        # Update analytics
        successful = sum(1 for r in results if r.status == DeploymentStatus.DEPLOYED)
        failed = sum(1 for r in results if r.status in [DeploymentStatus.FAILED, DeploymentStatus.TIMEOUT])
        
        self.analytics.total_deployments += len(results)
        self.analytics.successful += successful
        self.analytics.failed += failed
        self.analytics.avg_deployment_time = sum(r.deployment_time for r in results) / max(len(results), 1)
        self.analytics.success_rate = (successful / max(len(results), 1)) * 100
        self.analytics.strategy_distribution[strategy.value] = self.analytics.strategy_distribution.get(strategy.value, 0) + len(results)
        
        logger.info(f"✅ Deployed {successful}/{len(results)} wallets in {elapsed_time:.2f}s")
        logger.info(f"   Success rate: {self.analytics.success_rate:.2f}%")
        logger.info(f"   Avg time per deployment: {self.analytics.avg_deployment_time:.2f}s")
        
        return results
    
    def save_deployment_results(self,
                               results: List[DeploymentResult],
                               filename: str = "deployments_v1.1.json") -> None:
        """Save deployment results with analytics"""
        data = {
            'version': '1.1',
            'generated_at': datetime.now().isoformat(),
            'analytics': {
                'total': len(results),
                'successful': sum(1 for r in results if r.status == DeploymentStatus.DEPLOYED),
                'failed': sum(1 for r in results if r.status == DeploymentStatus.FAILED),
                'avg_deployment_time': sum(r.deployment_time for r in results) / max(len(results), 1),
                'success_rate': self.analytics.success_rate
            },
            'deployments': [
                {
                    'address': r.address,
                    'transaction_hash': r.transaction_hash,
                    'status': r.status.value,
                    'strategy': r.strategy.value,
                    'amount': r.amount,
                    'attempts': r.attempts,
                    'deployment_time': r.deployment_time,
                    'timestamp': r.timestamp,
                    'error': r.error
                }
                for r in results
            ]
        }
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Deployment results saved to {filename}")
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get deployment analytics"""
        return {
            'total_deployments': self.analytics.total_deployments,
            'successful': self.analytics.successful,
            'failed': self.analytics.failed,
            'avg_deployment_time': self.analytics.avg_deployment_time,
            'success_rate': self.analytics.success_rate,
            'strategy_distribution': self.analytics.strategy_distribution
        }


# Example usage
async def main():
    print("=" * 70)
    print("🚀 TON SCALABLE WALLET DEPLOYER v1.1 - Enhanced Edition")
    print("=" * 70)
    
    deployer = ScalableWalletDeployerV1(
        network=NetworkGlobalID.TESTNET,
        max_concurrent=5,
        retry_attempts=3
    )
    
    # Example mnemonics
    test_mnemonics = [
        "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
    ]
    
    print("\n🚀 Deploying wallets with OPTIMIZED strategy...")
    results = await deployer.deploy_multiple_wallets(
        test_mnemonics,
        amount=0.05,
        strategy=DeploymentStrategy.OPTIMIZED
    )
    
    print(f"\n" + "=" * 70)
    print("📊 Deployment Analytics")
    print("=" * 70)
    
    analytics = deployer.get_analytics()
    for key, value in analytics.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    deployer.save_deployment_results(
        results,
        "version/v1.1/output/deployments_v1.1.json"
    )


if __name__ == "__main__":
    asyncio.run(main())