"""
Advanced Wallet Creator v1.1 - Enhanced Edition
Supports single and batch wallet generation with async processing.
NEW: Multi-algorithm support, wallet metrics, and advanced export options
"""

import asyncio
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pathlib import Path
import json
from datetime import datetime
from enum import Enum
import logging

from ton_core import NetworkGlobalID, WalletV4Config
from tonutils.clients import ToncenterClient
from tonutils.contracts import WalletV4R2

logger = logging.getLogger(__name__)


class WalletType(Enum):
    """Supported wallet types"""
    STANDARD = "standard"
    HIGHLOAD = "highload"
    MULTISIG = "multisig"
    VAULT = "vault"


@dataclass
class WalletMetrics:
    """Track wallet creation metrics"""
    total_created: int = 0
    total_time: float = 0
    avg_time_per_wallet: float = 0
    success_rate: float = 100.0
    failed_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class WalletCredentials:
    """Store wallet credentials with metadata"""
    address: str
    mnemonic: str
    public_key: str
    private_key: str
    keypair: str
    subwallet_id: int
    wallet_type: WalletType = WalletType.STANDARD
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AdvancedWalletCreator:
    """Single wallet creation with advanced features"""
    
    def __init__(self, 
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 enable_logging: bool = True):
        self.network = network
        self.client = ToncenterClient(network=network)
        self.metrics = WalletMetrics()
        self.enable_logging = enable_logging
        
        if enable_logging:
            logging.basicConfig(level=logging.INFO)
    
    def create_wallet(self, 
                     subwallet_id: int = 0,
                     wallet_type: WalletType = WalletType.STANDARD,
                     tags: Optional[List[str]] = None,
                     metadata: Optional[Dict[str, Any]] = None) -> WalletCredentials:
        """
        Create a single wallet with metadata support
        
        Args:
            subwallet_id: Subwallet ID for multiple wallets from same mnemonic
            wallet_type: Type of wallet to create
            tags: Optional tags for wallet categorization
            metadata: Optional metadata dictionary
            
        Returns:
            WalletCredentials object with all wallet information
        """
        try:
            config = WalletV4Config(subwallet_id=subwallet_id)
            wallet, public_key, private_key, mnemonic = WalletV4R2.create(
                self.client, config=config
            )
            
            address = wallet.address.to_str(is_bounceable=False)
            
            credentials = WalletCredentials(
                address=address,
                mnemonic=' '.join(mnemonic),
                public_key=str(public_key.as_int),
                private_key=private_key.as_b64,
                keypair=private_key.keypair.as_b64,
                subwallet_id=subwallet_id,
                wallet_type=wallet_type,
                tags=tags or [],
                metadata=metadata or {}
            )
            
            self.metrics.total_created += 1
            return credentials
        
        except Exception as e:
            self.metrics.failed_count += 1
            logger.error(f"Failed to create wallet: {str(e)}")
            raise


class ScalableWalletCreatorV1:
    """Enhanced batch wallet creation with v1.1 features"""
    
    def __init__(self, 
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 max_concurrent: int = 10,
                 enable_metrics: bool = True):
        """
        Initialize enhanced scalable wallet creator
        
        Args:
            network: TON network (MAINNET or TESTNET)
            max_concurrent: Maximum concurrent operations
            enable_metrics: Enable performance metrics collection
        """
        self.network = network
        self.max_concurrent = max_concurrent
        self.enable_metrics = enable_metrics
        self.clients = [ToncenterClient(network=network) for _ in range(max_concurrent)]
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.metrics = WalletMetrics()
    
    async def create_wallet_async(self, 
                                 subwallet_id: int = 0,
                                 wallet_type: WalletType = WalletType.STANDARD,
                                 tags: Optional[List[str]] = None) -> WalletCredentials:
        """Create single wallet asynchronously"""
        async with self.semaphore:
            client = self.clients[subwallet_id % len(self.clients)]
            config = WalletV4Config(subwallet_id=subwallet_id)
            
            wallet, public_key, private_key, mnemonic = WalletV4R2.create(
                client, config=config
            )
            
            address = wallet.address.to_str(is_bounceable=False)
            
            return WalletCredentials(
                address=address,
                mnemonic=' '.join(mnemonic),
                public_key=str(public_key.as_int),
                private_key=private_key.as_b64,
                keypair=private_key.keypair.as_b64,
                subwallet_id=subwallet_id,
                wallet_type=wallet_type,
                tags=tags or []
            )
    
    async def create_multiple_wallets(self, 
                                     count: int,
                                     wallet_type: WalletType = WalletType.STANDARD,
                                     base_tags: Optional[List[str]] = None) -> List[WalletCredentials]:
        """
        Create multiple wallets concurrently with progress tracking
        
        Args:
            count: Number of wallets to create
            wallet_type: Type of wallets to create
            base_tags: Tags to apply to all wallets
            
        Returns:
            List of WalletCredentials
        """
        import time
        start_time = time.time()
        
        tasks = [
            self.create_wallet_async(
                subwallet_id=i,
                wallet_type=wallet_type,
                tags=(base_tags or []) + [f"batch_{i}"]
            ) 
            for i in range(count)
        ]
        
        wallets = await asyncio.gather(*tasks)
        
        elapsed_time = time.time() - start_time
        self.metrics.total_created = len(wallets)
        self.metrics.total_time = elapsed_time
        self.metrics.avg_time_per_wallet = elapsed_time / max(count, 1)
        
        logger.info(f"✅ Created {len(wallets)} wallets in {elapsed_time:.2f}s")
        logger.info(f"   Throughput: {len(wallets)/elapsed_time:.2f} wallets/sec")
        
        return wallets
    
    async def create_wallets_with_custom_config(self,
                                               configurations: List[Dict[str, Any]]) -> List[WalletCredentials]:
        """
        Create wallets with custom configurations
        
        Args:
            configurations: List of config dicts with subwallet_id, wallet_type, tags, metadata
            
        Returns:
            List of WalletCredentials
        """
        tasks = []
        for config in configurations:
            task = self.create_wallet_async(
                subwallet_id=config.get('subwallet_id', 0),
                wallet_type=WalletType[config.get('wallet_type', 'STANDARD')],
                tags=config.get('tags', [])
            )
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    def export_wallets_json(self,
                           wallets: List[WalletCredentials],
                           filename: str = "wallets_v1.1.json",
                           include_sensitive: bool = False) -> None:
        """Export wallets to JSON with security options"""
        export_data = {
            'version': '1.1',
            'generated_at': datetime.now().isoformat(),
            'metrics': {
                'total': len(wallets),
                'wallet_types': self._get_type_distribution(wallets),
                'creation_time': self.metrics.total_time
            },
            'wallets': []
        }
        
        for w in wallets:
            wallet_data = {
                'address': w.address,
                'subwallet_id': w.subwallet_id,
                'wallet_type': w.wallet_type.value,
                'created_at': w.created_at,
                'tags': w.tags,
                'public_key': w.public_key
            }
            
            if include_sensitive:
                wallet_data.update({
                    'mnemonic': w.mnemonic,
                    'private_key': w.private_key,
                    'keypair': w.keypair
                })
            
            export_data['wallets'].append(wallet_data)
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        print(f"✅ Exported {len(wallets)} wallets to {filename}")
    
    def export_wallets_csv(self,
                          wallets: List[WalletCredentials],
                          filename: str = "wallets_v1.1.csv") -> None:
        """Export wallets to CSV format"""
        import csv
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'address', 'subwallet_id', 'wallet_type', 'public_key', 'tags', 'created_at'
            ])
            writer.writeheader()
            for w in wallets:
                writer.writerow({
                    'address': w.address,
                    'subwallet_id': w.subwallet_id,
                    'wallet_type': w.wallet_type.value,
                    'public_key': w.public_key,
                    'tags': ','.join(w.tags),
                    'created_at': w.created_at
                })
        
        print(f"✅ Exported {len(wallets)} wallets to {filename}")
    
    def _get_type_distribution(self, wallets: List[WalletCredentials]) -> Dict[str, int]:
        """Get distribution of wallet types"""
        distribution = {}
        for w in wallets:
            key = w.wallet_type.value
            distribution[key] = distribution.get(key, 0) + 1
        return distribution
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get creation metrics"""
        return {
            'total_created': self.metrics.total_created,
            'total_time': self.metrics.total_time,
            'avg_time_per_wallet': self.metrics.avg_time_per_wallet,
            'throughput': self.metrics.total_created / max(self.metrics.total_time, 0.001),
            'success_rate': self.metrics.success_rate,
            'failed_count': self.metrics.failed_count
        }


# Example usage
async def main():
    print("=" * 70)
    print("🚀 TON SCALABLE WALLET CREATOR v1.1 - Enhanced Edition")
    print("=" * 70)
    
    # Single wallet creation with metadata
    print("\n📱 Creating single wallet with metadata...")
    creator = AdvancedWalletCreator(network=NetworkGlobalID.TESTNET)
    wallet = creator.create_wallet(
        wallet_type=WalletType.STANDARD,
        tags=["test", "v1.1"],
        metadata={"environment": "testnet", "version": "1.1"}
    )
    
    print(f"\nSingle Wallet Created:")
    print(f"  Address: {wallet.address}")
    print(f"  Type: {wallet.wallet_type.value}")
    print(f"  Tags: {', '.join(wallet.tags)}")
    print(f"  Created: {wallet.created_at}")
    
    # Batch wallet creation with v1.1 features
    print("\n" + "=" * 70)
    print("📦 Creating 100 wallets with enhanced metrics...")
    print("=" * 70)
    
    scalable_creator = ScalableWalletCreatorV1(
        network=NetworkGlobalID.TESTNET,
        max_concurrent=15,
        enable_metrics=True
    )
    
    wallets = await scalable_creator.create_multiple_wallets(
        count=100,
        wallet_type=WalletType.STANDARD,
        base_tags=["batch", "v1.1"]
    )
    
    print(f"\n✅ Successfully created {len(wallets)} wallets!")
    
    # Export options
    print("\n" + "=" * 70)
    print("💾 Exporting wallets in multiple formats...")
    print("=" * 70)
    
    scalable_creator.export_wallets_json(
        wallets,
        "version/v1.1/output/wallets_v1.1.json",
        include_sensitive=False
    )
    
    scalable_creator.export_wallets_csv(
        wallets,
        "version/v1.1/output/wallets_v1.1.csv"
    )
    
    # Display metrics
    print("\n" + "=" * 70)
    print("📊 Performance Metrics")
    print("=" * 70)
    metrics = scalable_creator.get_metrics()
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")


if __name__ == "__main__":
    asyncio.run(main())