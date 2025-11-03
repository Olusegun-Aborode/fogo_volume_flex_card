"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Loader2, Plus, Trash2, TrendingUp, Activity } from "lucide-react"
import { cn } from "@/lib/utils"

type Chain = "EVM" | "Solana"

interface Wallet {
  id: string
  address: string
  chain: Chain
}

interface VolumeData {
  total_volume: number
  total_trades: number
  breakdown_by_exchange: Record<string, { volume: number; inserted: number; fetched: number }>
  wallets: Array<{
    address: string
    chain: Chain
    exchanges: Record<string, { volume: number; inserted: number; fetched: number }>
    cached?: boolean
    cached_timestamp?: number
    cached_total_volume?: number
  }>
}

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

export function VolumeFlexCard() {
  const [wallets, setWallets] = useState<Wallet[]>([{ id: "1", address: "", chain: "EVM" }])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [volumeData, setVolumeData] = useState<VolumeData | null>(null)

  const addWallet = () => {
    if (wallets.length < 10) {
      setWallets([...wallets, { id: Date.now().toString(), address: "", chain: "EVM" }])
    }
  }

  const removeWallet = (id: string) => {
    if (wallets.length > 1) {
      setWallets(wallets.filter((w) => w.id !== id))
    }
  }

  const updateWallet = (id: string, field: "address" | "chain", value: string) => {
    setWallets(wallets.map((w) => (w.id === id ? { ...w, [field]: value } : w)))
  }

  const validateAddress = (address: string, chain: Chain): boolean => {
    if (!address) return false
    if (chain === "EVM") {
      return /^0x[a-fA-F0-9]{40}$/.test(address)
    } else {
      return address.length >= 32 && address.length <= 44
    }
  }

  const calculateVolume = async () => {
    setError(null)

    // Validate all addresses
    const invalidWallets = wallets.filter((w) => !validateAddress(w.address, w.chain))
    if (invalidWallets.length > 0) {
      setError("Please enter valid wallet addresses for all fields")
      return
    }

    setIsLoading(true)

    try {
      const response = await fetch(`${API_URL}/api/volume`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          wallets: wallets.map((w) => ({
            address: w.address,
            chain: w.chain,
          })),
        }),
      })

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}))
        throw new Error(errorData.message || `Server error: ${response.status}`)
      }

      const result = await response.json()
      if (result.success && result.data) {
        setVolumeData(result.data)
      } else {
        throw new Error(result.message || "Failed to fetch volume data")
      }
    } catch (err) {
      if (err instanceof TypeError && err.message.includes("fetch")) {
        setError(`Unable to connect to backend. Please ensure the server is running at ${API_URL}`)
      } else if (err instanceof Error) {
        setError(err.message)
      } else {
        setError("An unexpected error occurred while fetching volume data")
      }
      console.error("[v0] Volume fetch error:", err)
    } finally {
      setIsLoading(false)
    }
  }

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value)
  }

  const formatNumber = (value: number) => {
    return new Intl.NumberFormat("en-US").format(value)
  }

  return (
    <div className="w-full max-w-4xl">
      <Card className="glass-card border-slate-800/50 bg-slate-900/40 backdrop-blur-xl shadow-2xl">
        <CardHeader className="space-y-1 pb-6">
          <CardTitle className="text-3xl font-bold flex items-center gap-2 text-balance">
            <span className="text-2xl">🔥</span>
            <span className="bg-gradient-to-r from-emerald-400 to-cyan-400 bg-clip-text text-transparent">
              Volume Flex Card
            </span>
          </CardTitle>
          <CardDescription className="text-slate-400 text-base">
            Track trading volume across multiple wallets and chains
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          {/* Wallet Inputs */}
          <div className="space-y-4">
            {wallets.map((wallet, index) => (
              <div key={wallet.id} className="flex gap-3 items-start">
                <div className="flex-1 space-y-2">
                  <Input
                    placeholder="0x... or Solana address"
                    value={wallet.address}
                    onChange={(e) => updateWallet(wallet.id, "address", e.target.value)}
                    className="bg-slate-950/50 border-slate-700/50 text-slate-100 placeholder:text-slate-500 focus-visible:ring-emerald-500/50"
                  />
                </div>
                <Select
                  value={wallet.chain}
                  onValueChange={(value) => updateWallet(wallet.id, "chain", value as Chain)}
                >
                  <SelectTrigger className="w-32 bg-slate-950/50 border-slate-700/50 text-slate-100">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="EVM">EVM</SelectItem>
                    <SelectItem value="Solana">Solana</SelectItem>
                  </SelectContent>
                </Select>
                {wallets.length > 1 && (
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => removeWallet(wallet.id)}
                    className="text-slate-400 hover:text-red-400 hover:bg-red-950/20"
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                )}
              </div>
            ))}
          </div>

          {/* Add Wallet Button */}
          {wallets.length < 10 && (
            <Button
              variant="outline"
              onClick={addWallet}
              className="w-full border-slate-700/50 bg-slate-950/30 text-slate-300 hover:bg-slate-800/50 hover:text-slate-100"
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Wallet ({wallets.length}/10)
            </Button>
          )}

          {/* Calculate Button */}
          <Button
            onClick={calculateVolume}
            disabled={isLoading}
            className="w-full h-12 text-base font-semibold bg-gradient-to-r from-emerald-500 to-cyan-500 hover:from-emerald-600 hover:to-cyan-600 text-slate-950"
          >
            {isLoading ? (
              <>
                <Loader2 className="h-5 w-5 mr-2 animate-spin" />
                Fetching volume data...
              </>
            ) : (
              <>
                <Activity className="h-5 w-5 mr-2" />
                Calculate Volume
              </>
            )}
          </Button>

          {/* Error Message */}
          {error && (
            <Alert variant="destructive" className="bg-red-950/20 border-red-900/50 text-red-400">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {/* Results */}
          {volumeData && !isLoading && (
            <div className="space-y-6 pt-4 animate-in fade-in slide-in-from-bottom-4 duration-500">
              {/* Total Volume Card */}
              <Card className="border-slate-700/50 bg-gradient-to-br from-emerald-950/30 to-cyan-950/30 backdrop-blur">
                <CardContent className="pt-6">
                  <div className="text-center space-y-2">
                    <p className="text-sm text-slate-400 font-medium">Total Volume</p>
                    <p className="text-5xl font-bold bg-gradient-to-r from-emerald-400 to-cyan-400 bg-clip-text text-transparent">
                      {formatCurrency(volumeData.total_volume)}
                    </p>
                    <div className="flex items-center justify-center gap-2 text-emerald-400">
                      <TrendingUp className="h-4 w-4" />
                      <span className="text-sm font-medium">{formatNumber(volumeData.total_trades)} trades</span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Exchange Breakdown */}
              <div className="space-y-3">
                <h3 className="text-lg font-semibold text-slate-200">Exchange Breakdown</h3>
                <div className="space-y-2">
                  {Object.entries(volumeData.breakdown_by_exchange).map(([exchangeName, exData], index) => {
                    const vol = Number(exData?.volume ?? 0)
                    const percentage = volumeData.total_volume ? (vol / volumeData.total_volume) * 100 : 0
                    return (
                      <div key={index} className="space-y-1">
                        <div className="flex justify-between text-sm">
                          <span className="text-slate-300 font-medium">{exchangeName}</span>
                          <span className="text-slate-400">{formatCurrency(vol)}</span>
                        </div>
                        <div className="h-2 bg-slate-800/50 rounded-full overflow-hidden">
                          <div
                            className={cn(
                              "h-full rounded-full transition-all duration-500",
                              index === 0 && "bg-gradient-to-r from-emerald-500 to-emerald-400",
                              index === 1 && "bg-gradient-to-r from-cyan-500 to-cyan-400",
                              index === 2 && "bg-gradient-to-r from-blue-500 to-blue-400",
                              index === 3 && "bg-gradient-to-r from-teal-500 to-teal-400",
                            )}
                            style={{ width: `${percentage}%` }}
                          />
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>

              {/* Wallets Processed */}
              <div className="space-y-3">
                <h3 className="text-lg font-semibold text-slate-200">Wallets Processed</h3>
                <div className="space-y-2">
                  {volumeData.wallets.map((wallet, index) => (
                    <div key={index} className="p-3 bg-slate-950/30 border border-slate-800/50 rounded-lg">
                      <p className="text-sm text-slate-400 font-mono break-all">{wallet.address}</p>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
