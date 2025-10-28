"""
Utility script to verify Polymarket connectivity without placing trades.

Usage:
    python diagnostics/test_data_connection.py

Environment variables:
    TEST_CONDITION_ID    Optional specific market condition_id to inspect
    TEST_MARKET_INDEX    Fallback index (0-based) into the sheet if no condition_id provided
"""

import asyncio
import json
import os
from typing import Optional, Tuple

from dotenv import load_dotenv
import websockets

import poly_data.global_state as global_state
from poly_data.polymarket_client import PolymarketClient
from poly_data.data_utils import update_markets


def summarise_order_book(bids, asks, depth: int = 5) -> Tuple[Optional[float], Optional[float]]:
    """Print a short summary of the order book and return top of book prices."""
    best_bid = None
    best_ask = None

    if not bids.empty:
        best_bid = float(bids.iloc[0]['price'])
        print("\nTop bids:")
        print(bids[['price', 'size']].head(depth).to_string(index=False))
    else:
        print("\nNo bids returned.")

    if not asks.empty:
        best_ask = float(asks.iloc[0]['price'])
        print("\nTop asks:")
        print(asks[['price', 'size']].head(depth).to_string(index=False))
    else:
        print("\nNo asks returned.")

    return best_bid, best_ask


async def test_market_websocket(tokens):
    """Attempt to connect to the Polymarket market websocket and receive a sample message."""
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    payload = {"assets_ids": tokens}

    try:
        async with websockets.connect(uri, ping_interval=5, ping_timeout=20) as websocket:
            print(f"Connected to market websocket. Subscribing with payload: {payload}")
            await websocket.send(json.dumps(payload))

            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=5)
                print(f"Received market websocket message (truncated): {message[:200]}")
            except asyncio.TimeoutError:
                print("Connected to market websocket but no data received within 5 seconds.")
    except Exception as exc:
        print(f"Market websocket connection failed: {exc}")


def select_market(client: PolymarketClient) -> Optional[str]:
    """Pick a market token to inspect based on env vars or available data."""
    token_env = os.getenv("TEST_TOKEN_ID")
    if token_env:
        print(f"Using TEST_TOKEN_ID from environment: {token_env}")
        return token_env

    condition_id_env = os.getenv("TEST_CONDITION_ID")
    if condition_id_env:
        print(f"Using TEST_CONDITION_ID from environment (interpreted as token id): {condition_id_env}")
        return condition_id_env

    df = global_state.df
    if df is not None and not df.empty:
        row_idx = 0
        try:
            row_idx = int(os.getenv("TEST_MARKET_INDEX", "0"))
        except ValueError:
            row_idx = 0
        row_idx = max(0, min(row_idx, len(df) - 1))

        row = df.iloc[row_idx]
        question = row.get('question', 'N/A')
        condition_id_sheet = str(row.get('condition_id', 'N/A'))

        token_columns = [col for col in ['token1', 'token2', 'asset', 'token'] if col in row.index]
        for token_col in token_columns:
            token_value = row[token_col]
            if token_value:
                token_value = str(token_value)
                print(f"Selected market from sheet row {row_idx}: '{question}' "
                      f"(condition_id={condition_id_sheet}, token={token_value}, column={token_col})")
                return token_value

        print(f"Selected sheet row {row_idx} lacks token columns. Available cols: {row.index.tolist()}")
        print(f"Row data: {row.to_dict()}")

    try:
        positions_df = client.get_all_positions()
    except Exception as exc:
        print(f"Warning: could not fetch positions for market selection: {exc}")
        positions_df = None

    if positions_df is not None and not positions_df.empty:
        cols_lower = [c.lower() for c in positions_df.columns]
        row = positions_df.iloc[0]

        def pick_value(key_variants):
            for key in key_variants:
                if key in cols_lower:
                    col = positions_df.columns[cols_lower.index(key)]
                    return str(row[col])
            return None

        market = pick_value(["asset", "market", "market_id", "condition_id", "conditionid"])
        if market:
            print(f"Selected market '{market}' from positions (columns: {positions_df.columns.tolist()})")
            return market

        print("Positions response does not contain market identifiers.")
        print(f"Available columns: {positions_df.columns.tolist()}")
        print(f"First row sample: {row.to_dict()}")
        return None

    print("No TEST_CONDITION_ID provided and no positions found; specify a market via env.")
    return None


def main():
    load_dotenv()

    print("Initialising Polymarket client...")
    client = PolymarketClient()
    global_state.client = client
    tokens_for_ws = []

    try:
        usdc_balance = client.get_usdc_balance()
        pos_balance, raw_value_payload = client.get_pos_balance(return_details=True)
        total_balance = usdc_balance + pos_balance

        print(f"USDC balance: {usdc_balance:.4f} USDC")
        print(f"Positions value: {pos_balance:.4f} USDC")
        print(f"Wallet total balance (USDC + positions): {total_balance:.4f} USDC")
        print(f"Polymarket value API response: {raw_value_payload}")
    except Exception as exc:
        print(f"Warning: could not fetch wallet balance: {exc}")

    try:
        print("\nFetching selected markets from Google Sheet...")
        update_markets()

        df = global_state.df
        if df is not None and not df.empty:
            display_columns = [
                col for col in [
                    "question",
                    "answer1",
                    "answer2",
                    "condition_id",
                    "token1",
                    "token2",
                    "param_type",
                    "trade_size",
                    "max_spread"
                ] if col in df.columns
            ]

            if display_columns:
                print(f"Loaded {len(df)} markets from Selected Markets sheet:")
                print(df[display_columns].to_string(index=False))
            else:
                print(f"Loaded {len(df)} markets but no displayable columns found. Columns: {df.columns.tolist()}")

            for col in ["token1", "token2"]:
                if col in df.columns:
                    tokens_for_ws.extend([str(t) for t in df[col].head(10).dropna()])
        else:
            print("Selected Markets sheet returned no markets.")
    except Exception as exc:
        print(f"Warning: could not fetch Google Sheet markets: {exc}")

    tokens_for_ws = [t for t in tokens_for_ws if t]
    if not tokens_for_ws and getattr(global_state, "all_tokens", None):
        tokens_for_ws = [str(t) for t in global_state.all_tokens[:20]]
    else:
        # Deduplicate while preserving order and limit to 20 entries
        seen = set()
        filtered = []
        for tok in tokens_for_ws:
            if tok not in seen:
                seen.add(tok)
                filtered.append(tok)
        tokens_for_ws = filtered[:20]

    print("\nTesting market websocket connectivity...")
    try:
        asyncio.run(test_market_websocket(tokens_for_ws))
    except RuntimeError as exc:
        # In case an event loop is already running (e.g., within Jupyter)
        loop = asyncio.get_event_loop()
        if loop.is_running():
            print("Event loop already running; scheduling websocket test.")
            loop.create_task(test_market_websocket(tokens_for_ws))
        else:
            print(f"Unexpected runtime error when testing websocket: {exc}")
    except Exception as exc:
        print(f"Market websocket test encountered an error: {exc}")

    condition_id = select_market(client)
    if not condition_id:
        return

    print(f"\nRequesting order book for market {condition_id}...")
    try:
        bids, asks = client.get_order_book(condition_id)
    except Exception as exc:
        print(f"Error fetching order book: {exc}")
        return

    best_bid, best_ask = summarise_order_book(bids, asks)
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        print(f"\nBest bid: {best_bid:.4f}, Best ask: {best_ask:.4f}, Spread: {spread:.4f}, Mid: {mid:.4f}")
    else:
        print("\nInsufficient order book data to compute spread/mid.")


if __name__ == "__main__":
    main()
