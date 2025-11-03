from volume_flex_card.price_oracle import get_price_at_timestamp
import time

# Test ETH price
eth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
timestamp = int(time.time()) - 86400  # Yesterday

print("Testing price oracle...")
price = get_price_at_timestamp(eth_address, timestamp)
print(f"ETH price 24h ago: ${price:.2f}")

# Test cache (should be instant on second call)
start = time.time()
price2 = get_price_at_timestamp(eth_address, timestamp)
elapsed = time.time() - start
print(f"Cached lookup took: {elapsed*1000:.2f}ms")
