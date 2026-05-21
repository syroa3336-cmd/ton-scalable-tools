"""
Scalable Wallet Creator for TON Network
Supports single and batch wallet generation with async processing.
"""

import asyncio
from dataclasses import dataclass
from typing import List, Tuple, Optional
from pathlib import Path
import json

from ton_core import NetworkGlobalID, WalletV4Config
from tonutils.clients import ToncenterClient
from tonutils.contracts import WalletV4R2


@dataclass
class WalletCredentials:
    """Store wallet credentials"""
    address: str
    mnemonic: str
    public_key: str
    private_key: str
    keypair: str
    subwallet_id: int


class WalletCreator:
    """Single wallet creation"""
    
    def __init__(self, network: NetworkGlobalID = NetworkGlobalID.MAINNET):
        self.network = network
        self.client = ToncenterClient(network=network)
    
    def create_wallet(self, subwallet_id: int = 0) -> WalletCredentials:
        """
        Create a single wallet synchronously
        
        Args:
            subwallet_id: Subwallet ID for multiple wallets from same mnemonic
            
        Returns:
            WalletCredentials object with all wallet information
        """
        config = WalletV4Config(subwallet_id=subwallet_id)
        wallet, public_key, private_key, mnemonic = WalletV4R2.create(
            self.client, config=config
        )
        
        address = wallet.address.to_str(is_bounceable=False)
        
        return WalletCredentials(
            address=address,
            mnemonic=' '.join(mnemonic),
            public_key=str(public_key.as_int),
            private_key=private_key.as_b64,
            keypair=private_key.keypair.as_b64,
            subwallet_id=subwallet_id
        )


class ScalableWalletCreator:
    """Batch wallet creation with async processing for high throughput"""
    
    def __init__(self, 
                 network: NetworkGlobalID = NetworkGlobalID.MAINNET,
                 max_concurrent: int = 10):
        """
        Initialize scalable wallet creator
        
        Args:
            network: TON network (MAINNET or TESTNET)
            max_concurrent: Maximum concurrent operations
        """
        self.network = network
        self.max_concurrent = max_concurrent
        self.clients = [ToncenterClient(network=network) for _ in range(max_concurrent)]
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    async def create_wallet_async(self, subwallet_id: int = 0) -> WalletCredentials:
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
                subwallet_id=subwallet_id
            )
    
    async def create_multiple_wallets(self, count: int) -> List[WalletCredentials]:
        """
        Create multiple wallets concurrently
        
        Args:
            count: Number of wallets to create
            
        Returns:
            List of WalletCredentials
        """
        tasks = [
            self.create_wallet_async(subwallet_id=i) 
            for i in range(count)
        ]
        return await asyncio.gather(*tasks)
    
    def save_wallets_to_file(self, 
                            wallets: List[WalletCredentials], 
                            filename: str = "wallets.json") -> None:
        """Save wallet credentials to JSON file"""
        data = {
            'wallets': [
                {
                    'address': w.address,
                    'mnemonic': w.mnemonic,
                    'public_key': w.public_key,
                    'subwallet_id': w.subwallet_id
                }
                for w in wallets
            ],
            'count': len(wallets)
        }
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Saved {len(wallets)} wallets to {filename}")


# Example usage
async def main():
    print("=" * 60)
    print("TON SCALABLE WALLET CREATOR")
    print("=" * 60)
    
    # Single wallet creation
    print("\n📱 Creating single wallet...")
    creator = WalletCreator(network=NetworkGlobalID.TESTNET)
    wallet = creator.create_wallet()
    
    print(f"\nSingle Wallet Created:")
    print(f"  Address: {wallet.address}")
    print(f"  Mnemonic: {wallet.mnemonic}")
    print(f"  Subwallet ID: {wallet.subwallet_id}")
    
    # Batch wallet creation
    print("\n" + "=" * 60)
    print("📦 Creating 50 wallets in parallel...")
    print("=" * 60)
    
    scalable_creator = ScalableWalletCreator(
        network=NetworkGlobalID.TESTNET,
        max_concurrent=10
    )
    
    wallets = await scalable_creator.create_multiple_wallets(50)
    
    print(f"\n✅ Successfully created {len(wallets)} wallets!")
    print(f"\nFirst 3 wallets:")
    for i, w in enumerate(wallets[:3]):
        print(f"  {i+1}. {w.address}")
    
    # Save to file
    scalable_creator.save_wallets_to_file(
        wallets, 
        "output/created_wallets.json"
    )


if __name__ == "__main__":
    asyncio.run(main())