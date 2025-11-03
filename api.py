from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from volume_flex_card.aggregate_volume import (
    process_evm_wallet,
    process_solana_wallet,
)
from volume_flex_card.database_setup import init_db


class WalletInput(BaseModel):
    address: str = Field(..., description="Wallet address")
    chain: str = Field(..., description="Chain identifier: 'EVM' or 'Solana'")


class VolumeRequest(BaseModel):
    wallets: List[WalletInput]


app = FastAPI(title="Fogo Volume Flex Card API")

# Enable CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    try:
        init_db()
    except Exception:
        # DB may already be initialized; proceed
        pass


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"success": True, "status": "ok"}


@app.post("/api/volume")
def get_volume(req: VolumeRequest) -> Dict[str, Any]:
    if not req.wallets:
        raise HTTPException(status_code=400, detail="wallets list must not be empty")

    wallet_summaries: List[Dict[str, Any]] = []

    try:
        for w in req.wallets:
            chain = (w.chain or "").strip().upper()
            if chain == "EVM":
                summary = process_evm_wallet(w.address)
            elif chain == "SOLANA":
                summary = process_solana_wallet(w.address)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported chain: {w.chain}")
            wallet_summaries.append(summary)

        # Aggregate totals and breakdown across all wallets
        total_volume = 0.0
        total_trades = 0
        breakdown_by_exchange: Dict[str, Dict[str, Any]] = {}

        for ws in wallet_summaries:
            exchanges = ws.get("exchanges", {})
            for ex_name, ex_data in exchanges.items():
                vol = float(ex_data.get("volume", 0.0))
                ins = int(ex_data.get("inserted", 0))
                total_volume += vol
                total_trades += ins
                agg = breakdown_by_exchange.setdefault(
                    ex_name, {"volume": 0.0, "inserted": 0, "fetched": 0}
                )
                agg["volume"] = float(agg["volume"]) + vol
                agg["inserted"] = int(agg["inserted"]) + ins
                agg["fetched"] = int(agg["fetched"]) + int(ex_data.get("fetched", 0))

        return {
            "success": True,
            "data": {
                "total_volume": float(total_volume),
                "total_trades": int(total_trades),
                "breakdown_by_exchange": breakdown_by_exchange,
                "wallets": wallet_summaries,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)