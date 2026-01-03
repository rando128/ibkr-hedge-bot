from django.db import models
from django.utils import timezone


class Bot(models.Model):
    """
    A bot instance representing a hedge strategy with specific parameters.
    """
    STATUS_CHOICES = [
        ('IDLE', 'Idle'),
        ('RUNNING', 'Running'),
        ('STOPPED', 'Stopped'),
        ('ERROR', 'Error'),
    ]

    # Bot identification
    name = models.CharField(max_length=100, blank=True, help_text="Optional bot name")

    # Trading parameters
    symbol = models.CharField(max_length=20, db_index=True)
    qty = models.DecimalField(max_digits=10, decimal_places=2)
    stop_pct = models.DecimalField(max_digits=5, decimal_places=2, help_text="Stop loss percentage")
    trailing_pct = models.DecimalField(max_digits=5, decimal_places=2, help_text="Trailing stop percentage")

    # Account configuration
    long_account = models.CharField(max_length=50)
    short_account = models.CharField(max_length=50)

    # Connection settings
    port = models.IntegerField(default=7497)
    use_algo = models.BooleanField(default=False, help_text="Use IBKR Adaptive algo")

    # Status
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='IDLE', db_index=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    started_at = models.DateTimeField(null=True, blank=True)
    stopped_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'trading_bots'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['symbol', 'status']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"Bot {self.id}: {self.symbol} ({self.status})"


class Cycle(models.Model):
    """
    A single hedge cycle from entry to complete exit.
    """
    STATUS_CHOICES = [
        ('INITIALIZING', 'Initializing'),
        ('ENTERING', 'Entering Positions'),
        ('ACTIVE', 'Active with Protection'),
        ('TRANSITIONING', 'Transitioning to Trailing'),
        ('EXITING', 'Exiting Positions'),
        ('COMPLETED', 'Completed'),
        ('ABORTED', 'Aborted'),
        ('ERROR', 'Error'),
    ]

    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name='cycles')
    cycle_number = models.IntegerField(help_text="Sequential cycle number for this bot")

    # Contract details
    symbol = models.CharField(max_length=20)
    contract_id = models.IntegerField(null=True, blank=True, help_text="IBKR conId")
    min_tick = models.DecimalField(max_digits=10, decimal_places=6, default=0.01)

    # Status
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='INITIALIZING', db_index=True)

    # P&L tracking
    total_buys = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_sells = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_commission = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    net_pnl = models.DecimalField(max_digits=15, decimal_places=2, default=0, help_text="Computed: sells - buys - commission")

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'trading_cycles'
        ordering = ['-created_at']
        unique_together = [['bot', 'cycle_number']]
        indexes = [
            models.Index(fields=['bot', 'cycle_number']),
            models.Index(fields=['status']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"Cycle {self.cycle_number} - {self.symbol} ({self.status})"

    def update_pnl(self):
        """Recalculate P&L from executions"""
        execs = self.executions.all()
        self.total_buys = sum(e.price * e.shares for e in execs if e.side == 'BUY')
        self.total_sells = sum(e.price * e.shares for e in execs if e.side == 'SELL')
        self.total_commission = sum(e.commission for e in execs)
        self.net_pnl = self.total_sells - self.total_buys - self.total_commission
        self.save(update_fields=['total_buys', 'total_sells', 'total_commission', 'net_pnl'])


class Order(models.Model):
    """
    An order placed with IBKR, tracking its lifecycle.
    """
    ROLE_CHOICES = [
        ('LONG_ENTRY', 'Long Entry'),
        ('SHORT_ENTRY', 'Short Entry'),
        ('LONG_SL', 'Long Stop Loss'),
        ('SHORT_SL', 'Short Stop Loss'),
        ('LONG_TRAIL', 'Long Trailing Stop'),
        ('SHORT_TRAIL', 'Short Trailing Stop'),
        ('PANIC_FLATTEN', 'Panic Flatten'),
        ('PROT_FAIL_FLATTEN', 'Protection Failure Flatten'),
    ]

    STATUS_CHOICES = [
        ('PendingSubmit', 'Pending Submit'),
        ('PreSubmitted', 'Pre-Submitted'),
        ('Submitted', 'Submitted'),
        ('Filled', 'Filled'),
        ('PartiallyFilled', 'Partially Filled'),
        ('Cancelled', 'Cancelled'),
        ('ApiCancelled', 'API Cancelled'),
        ('Inactive', 'Inactive'),
        ('Rejected', 'Rejected'),
    ]

    cycle = models.ForeignKey(Cycle, on_delete=models.CASCADE, related_name='orders')

    # IBKR identifiers
    order_id = models.IntegerField(db_index=True, help_text="IBKR orderId")
    order_ref = models.CharField(max_length=100, blank=True, help_text="Custom order reference")
    perm_id = models.IntegerField(null=True, blank=True, help_text="IBKR permId")

    # Order details
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, db_index=True)
    account = models.CharField(max_length=50)
    action = models.CharField(max_length=4, choices=[('BUY', 'Buy'), ('SELL', 'Sell')])
    order_type = models.CharField(max_length=20, help_text="Market, Stop, Trail, etc.")
    total_quantity = models.DecimalField(max_digits=10, decimal_places=2)

    # Order parameters
    limit_price = models.DecimalField(max_digits=15, decimal_places=4, null=True, blank=True)
    stop_price = models.DecimalField(max_digits=15, decimal_places=4, null=True, blank=True)
    trailing_percent = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    # Status tracking
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PendingSubmit', db_index=True)
    filled_quantity = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    avg_fill_price = models.DecimalField(max_digits=15, decimal_places=4, null=True, blank=True)

    # Timestamps
    submitted_at = models.DateTimeField(default=timezone.now)
    last_update_at = models.DateTimeField(auto_now=True)
    filled_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'trading_orders'
        ordering = ['-submitted_at']
        indexes = [
            models.Index(fields=['cycle', 'role']),
            models.Index(fields=['order_id']),
            models.Index(fields=['status']),
            models.Index(fields=['submitted_at']),
        ]

    def __str__(self):
        return f"Order {self.order_id}: {self.role} {self.action} {self.total_quantity} ({self.status})"


class Execution(models.Model):
    """
    A trade execution (fill) with commission details.
    """
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='executions')
    cycle = models.ForeignKey(Cycle, on_delete=models.CASCADE, related_name='executions')

    # IBKR identifiers
    exec_id = models.CharField(max_length=50, unique=True, db_index=True, help_text="IBKR execution ID")

    # Execution details
    side = models.CharField(max_length=4, choices=[('BUY', 'Buy'), ('SELL', 'Sell')])
    shares = models.DecimalField(max_digits=10, decimal_places=2)
    price = models.DecimalField(max_digits=15, decimal_places=4)
    account = models.CharField(max_length=50)

    # Commission details
    commission = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    commission_currency = models.CharField(max_length=3, default='USD')
    realized_pnl = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True, help_text="IBKR reported realized P&L")

    # Timestamp from exchange
    executed_at = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'trading_executions'
        ordering = ['-executed_at']
        indexes = [
            models.Index(fields=['cycle', 'executed_at']),
            models.Index(fields=['order', 'executed_at']),
        ]

    def __str__(self):
        return f"Exec {self.exec_id}: {self.side} {self.shares}@{self.price}"


class Event(models.Model):
    """
    System events and logs for audit trail and debugging.
    """
    LEVEL_CHOICES = [
        ('DEBUG', 'Debug'),
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('CRITICAL', 'Critical'),
    ]

    EVENT_TYPE_CHOICES = [
        ('BOT_START', 'Bot Started'),
        ('BOT_STOP', 'Bot Stopped'),
        ('CYCLE_START', 'Cycle Started'),
        ('CYCLE_COMPLETE', 'Cycle Completed'),
        ('CYCLE_ABORT', 'Cycle Aborted'),
        ('ORDER_SUBMITTED', 'Order Submitted'),
        ('ORDER_FILLED', 'Order Filled'),
        ('ORDER_CANCELLED', 'Order Cancelled'),
        ('ORDER_REJECTED', 'Order Rejected'),
        ('STOP_LOSS_HIT', 'Stop Loss Triggered'),
        ('TRANSITION_START', 'Transition Started'),
        ('TRANSITION_COMPLETE', 'Transition Completed'),
        ('TRAILING_UPDATE', 'Trailing Stop Updated'),
        ('POSITION_FLAT', 'Position Flattened'),
        ('PANIC_FLATTEN', 'Panic Flatten'),
        ('IBKR_ERROR', 'IBKR Error'),
        ('SYSTEM_ERROR', 'System Error'),
        ('PNL_REPORT', 'P&L Report'),
    ]

    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name='events', null=True, blank=True)
    cycle = models.ForeignKey(Cycle, on_delete=models.CASCADE, related_name='events', null=True, blank=True)
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='events', null=True, blank=True)

    # Event details
    event_type = models.CharField(max_length=30, choices=EVENT_TYPE_CHOICES, db_index=True)
    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default='INFO', db_index=True)
    message = models.TextField()

    # Optional structured data
    data = models.JSONField(null=True, blank=True, help_text="Additional structured event data")

    # Timestamp
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = 'trading_events'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['bot', 'created_at']),
            models.Index(fields=['cycle', 'created_at']),
            models.Index(fields=['event_type', 'created_at']),
            models.Index(fields=['level', 'created_at']),
        ]

    def __str__(self):
        return f"[{self.level}] {self.event_type}: {self.message[:50]}"
