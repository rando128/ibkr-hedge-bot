# config.yaml
# Minimal config for the hedging bot (paper trading defaults)

symbol: NVDA
exchange: SMART
currency: USD

# Strategy
notional_per_leg_usd: 100
sl_pct: 0.03
trailing_pct: 0.15
bar_size: "1 min"
cooldown_bars: 1

# IBKR connection (paper defaults shown)
ibkr:
  host: 127.0.0.1
  port: 7497
  client_id: 1

# Operations
entering_timeout_sec: 30
reconcile_retries: 3
reconcile_retry_delay_sec: 1.0

# Logging: DEBUG / INFO / WARNING / ERROR
log_level: DEBUG
