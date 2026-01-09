from __future__ import annotations

import uuid

from django.db import models
from django.utils import timezone


class Bot(models.Model):
    """
    Bot configuration + desired runtime status.

    The TWS agent is responsible for taking actions to converge the real world
    (TWS orders/positions) to the desired state.
    """

    STATUS_CHOICES = [
        ("RUNNING", "Running"),
        ("STOPPING", "Stopping (finish current cycle)"),
        ("STOPPED", "Stopped"),
        ("ERROR", "Error"),
    ]

    ENVIRONMENT_CHOICES = [
        ("PAPER", "Paper Trading"),
        ("LIVE", "Live Trading"),
    ]

    name = models.CharField(max_length=100, blank=True, help_text="Optional bot name")

    symbol = models.CharField(max_length=20, db_index=True)
    qty = models.DecimalField(max_digits=12, decimal_places=2)

    # Convention: 0.02 == 2%
    stop_pct = models.DecimalField(max_digits=8, decimal_places=6, help_text="Stop loss ratio (e.g., 0.02 for 2%)")
    trailing_pct = models.DecimalField(
        max_digits=8,
        decimal_places=6,
        help_text="Trailing stop ratio (e.g., 0.01 for 1%)",
    )

    primary_exchange = models.CharField(
        max_length=20,
        blank=True,
        help_text="Primary exchange (e.g., 'NYSE', 'NASDAQ', 'SBF'). Leave blank if not needed.",
    )
    exchange = models.CharField(max_length=20, default="SMART", help_text="Routing exchange (usually 'SMART')")
    currency = models.CharField(max_length=3, default="USD", help_text="Currency (e.g., 'USD', 'EUR')")

    long_account = models.CharField(max_length=50)
    short_account = models.CharField(max_length=50)

    environment = models.CharField(max_length=10, choices=ENVIRONMENT_CHOICES, default="PAPER")
    use_algo = models.BooleanField(default=False, help_text="Use IBKR Adaptive algo (optional)")

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="STOPPED", db_index=True)

    panic_requested = models.BooleanField(default=False, help_text="Operator emergency exit request")
    last_error = models.TextField(blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    started_at = models.DateTimeField(null=True, blank=True)
    stopped_at = models.DateTimeField(null=True, blank=True)

    @property
    def port(self) -> int:
        return 7497 if self.environment == "PAPER" else 7496

    class Meta:
        db_table = "trading_bots"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["symbol", "status"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self) -> str:
        return f"Bot {self.id}: {self.symbol} ({self.status})"


class Cycle(models.Model):
    STATE_CHOICES = [
        ("INITIALIZING", "Initializing"),
        ("ENTERING", "Entering (entries + SL protection)"),
        ("ACTIVE", "Active (both SL protections in place)"),
        ("TRANSITIONING", "Transitioning (flip SL to trailing)"),
        ("TRAILING", "Trailing active"),
        ("RECOVERING", "Recovering"),
        ("PANIC", "Panic (flatten/cancel)"),
        ("PNL_CALCULATION", "P&L calculation"),
        ("COMPLETED", "Completed"),
        ("ABORTED", "Aborted"),
        ("ERROR", "Error"),
    ]

    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name="cycles")
    cycle_number = models.IntegerField(help_text="Sequential cycle number for this bot")
    cycle_key = models.UUIDField(default=uuid.uuid4, unique=True, db_index=True)

    symbol = models.CharField(max_length=20)
    contract_id = models.IntegerField(null=True, blank=True, help_text="IBKR conId")
    min_tick = models.DecimalField(max_digits=12, decimal_places=6, default=0.01)

    state = models.CharField(max_length=20, choices=STATE_CHOICES, default="INITIALIZING", db_index=True)

    started_at = models.DateTimeField(default=timezone.now)
    last_activity_at = models.DateTimeField(default=timezone.now, db_index=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    total_buys = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    total_sells = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    total_commission = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    net_pnl = models.DecimalField(max_digits=18, decimal_places=6, default=0)

    class Meta:
        db_table = "trading_cycles"
        ordering = ["-started_at"]
        indexes = [
            models.Index(fields=["bot", "cycle_number"]),
            models.Index(fields=["bot", "state"]),
            models.Index(fields=["cycle_key"]),
        ]

    def __str__(self) -> str:
        return f"Cycle {self.cycle_number} ({self.symbol}) [{self.state}]"


class Event(models.Model):
    LEVEL_CHOICES = [
        ("DEBUG", "Debug"),
        ("INFO", "Info"),
        ("WARNING", "Warning"),
        ("ERROR", "Error"),
        ("CRITICAL", "Critical"),
    ]

    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name="events", null=True, blank=True)
    cycle = models.ForeignKey(Cycle, on_delete=models.CASCADE, related_name="events", null=True, blank=True)

    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default="INFO", db_index=True)
    event_type = models.CharField(max_length=50, db_index=True)
    message = models.TextField()
    data = models.JSONField(null=True, blank=True)

    from_state = models.CharField(max_length=20, blank=True, default="")
    to_state = models.CharField(max_length=20, blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "trading_events"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["bot", "created_at"]),
            models.Index(fields=["cycle", "created_at"]),
            models.Index(fields=["event_type", "created_at"]),
            models.Index(fields=["level", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"[{self.level}] {self.event_type}: {self.message[:50]}"

