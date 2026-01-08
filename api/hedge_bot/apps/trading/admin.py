from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from .models import (
    Bot,
    Cycle,
    Order,
    Execution,
    Event,
    ProcrastinateJob,
    ProcrastinateWorker,
    ProcrastinateEvent,
)
from .tasks import start_bot, stop_bot


@admin.register(Bot)
class BotAdmin(admin.ModelAdmin):
    list_display = ['id', 'name', 'symbol', 'qty', 'environment_badge', 'status_badge', 'cycles_count', 'last_pnl', 'action_buttons', 'log_button']
    list_filter = ['status', 'environment', 'symbol', 'created_at']
    search_fields = ['name', 'symbol', 'long_account', 'short_account']
    readonly_fields = ['created_at', 'updated_at', 'started_at', 'stopped_at']
    actions = ['action_start_bots', 'action_stop_bots']

    fieldsets = (
        ('Identification', {
            'fields': ('name', 'status')
        }),
        ('Contract Configuration', {
            'fields': ('symbol', 'primary_exchange', 'exchange', 'currency'),
            'description': 'IBKR contract parameters. For most US stocks, use SMART/USD. For European stocks like AIR, use SBF/EUR.'
        }),
        ('Trading Parameters', {
            'fields': ('qty', 'stop_pct', 'trailing_pct')
        }),
        ('Accounts', {
            'fields': ('long_account', 'short_account')
        }),
        ('Connection', {
            'fields': ('environment', 'use_algo')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at', 'started_at', 'stopped_at'),
            'classes': ('collapse',)
        }),
    )

    def environment_badge(self, obj):
        """Display environment as colored badge"""
        colors = {
            'PAPER': '#2196F3',  # Blue
            'LIVE': '#FF5722',   # Red/Orange
        }
        labels = {
            'PAPER': 'Paper',
            'LIVE': 'Live',
        }
        color = colors.get(obj.environment, 'gray')
        label = labels.get(obj.environment, obj.environment)
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px; font-weight: bold;">{}</span>',
            color, label
        )
    environment_badge.short_description = 'Environment'

    def status_badge(self, obj):
        """Display status as colored badge"""
        colors = {
            'IDLE': 'gray',
            'RUNNING': 'green',
            'STOPPED': 'orange',
            'ERROR': 'red'
        }
        color = colors.get(obj.status, 'gray')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px; font-weight: bold;">{}</span>',
            color, obj.status
        )
    status_badge.short_description = 'Status'

    def cycles_count(self, obj):
        """Display number of cycles"""
        count = obj.cycles.count()
        url = reverse('admin:trading_cycle_changelist') + f'?bot__id__exact={obj.id}'
        return format_html('<a href="{}">{} cycles</a>', url, count)
    cycles_count.short_description = 'Cycles'

    def last_pnl(self, obj):
        """Display P&L from last completed cycle"""
        last_cycle = obj.cycles.filter(status='COMPLETED').order_by('-completed_at').first()
        if last_cycle:
            color = 'green' if last_cycle.net_pnl >= 0 else 'red'
            pnl_value = float(last_cycle.net_pnl)
            return format_html(
                '<span style="color: {}; font-weight: bold;">${}</span>',
                color, f'{pnl_value:.2f}'
            )
        return '-'
    last_pnl.short_description = 'Last P&L'

    def action_buttons(self, obj):
        """Display start/stop/panic buttons"""
        buttons = []

        # Start/Stop button
        if obj.status == 'RUNNING':
            buttons.append(format_html(
                '<a class="button" href="{}">Stop</a>',
                reverse('admin:trading_bot_stop', args=[obj.pk])
            ))
        elif obj.status in ('IDLE', 'STOPPED', 'ERROR'):
            buttons.append(format_html(
                '<a class="button" href="{}">Start</a>',
                reverse('admin:trading_bot_start', args=[obj.pk])
            ))

        # Always show panic button (independent of status)
        buttons.append(format_html(
            '<a class="button" href="{}" style="background-color: #dc3545; margin-left: 5px;" onclick="return confirm(\'PANIC: This will cancel all orders and close all positions. Continue?\')">🚨</a>',
            reverse('admin:trading_bot_panic', args=[obj.pk])
        ))

        return format_html(' '.join(buttons)) if buttons else '-'
    action_buttons.short_description = 'Actions'

    def log_button(self, obj):
        """Display link to bot logs"""
        url = reverse('admin:trading_event_changelist') + f'?bot__id__exact={obj.id}'
        return format_html('<a class="button" href="{}">Logs</a>', url)
    log_button.short_description = 'Logs'

    def action_start_bots(self, request, queryset):
        """Admin action to start selected bots"""
        count = 0
        for bot in queryset:
            if bot.status in ('IDLE', 'STOPPED'):
                start_bot.defer(bot_id=bot.id)
                count += 1
        self.message_user(request, f'{count} bot(s) scheduled to start')
    action_start_bots.short_description = 'Start selected bots'

    def action_stop_bots(self, request, queryset):
        """Admin action to stop selected bots"""
        count = 0
        for bot in queryset:
            if bot.status == 'RUNNING':
                stop_bot.defer(bot_id=bot.id)
                count += 1
        self.message_user(request, f'{count} bot(s) scheduled to stop')
    action_stop_bots.short_description = 'Stop selected bots'

    def get_urls(self):
        """Add custom URLs for start/stop actions"""
        from django.urls import path
        urls = super().get_urls()
        custom_urls = [
            path('<int:bot_id>/start/', self.admin_site.admin_view(self.start_bot_view), name='trading_bot_start'),
            path('<int:bot_id>/stop/', self.admin_site.admin_view(self.stop_bot_view), name='trading_bot_stop'),
            path('<int:bot_id>/panic/', self.admin_site.admin_view(self.panic_bot_view), name='trading_bot_panic'),
        ]
        return custom_urls + urls

    def start_bot_view(self, request, bot_id):
        """View to start a bot"""
        from django.shortcuts import redirect
        from django.contrib import messages
        from django.utils import timezone
        from .tasks import run_bot_worker
        import logging

        logger = logging.getLogger(__name__)

        try:
            bot = Bot.objects.get(pk=bot_id)
            if bot.status in ('IDLE', 'STOPPED', 'ERROR'):
                # NOTE: Preflight check removed - bot runner now handles recovery with smart reconciliation
                # The bot worker will:
                # 1. Backfill executions (catch fills during downtime)
                # 2. Reconcile order statuses
                # 3. Validate TWS-DB consistency
                # 4. Resume if consistent, or block with detailed error if not
                logger.info(f"[START] Starting bot {bot_id} - smart reconciliation will handle state validation")

                # Start the bot (monitor will launch the worker)
                bot.status = 'RUNNING'
                bot.started_at = timezone.now()
                bot.stopped_at = None
                bot.worker_task_id = None
                bot.worker_started_at = None
                bot.worker_last_heartbeat = None
                bot.save()

                Event.objects.create(
                    bot=bot,
                    event_type='BOT_START',
                    level='INFO',
                    message=f"Bot start requested from admin"
                )

                # Do not launch worker here; monitor_bots will claim and start it to avoid races
                # run_bot_worker.defer(bot_id=bot.id)

                messages.success(request, f'Bot "{bot.name or bot.symbol}" started. Worker will validate state and resume if consistent.')
            else:
                messages.warning(request, f'Bot is already {bot.status}')
        except Bot.DoesNotExist:
            messages.error(request, 'Bot not found')

        return redirect('admin:trading_bot_changelist')

    def stop_bot_view(self, request, bot_id):
        """View to stop a bot"""
        from django.shortcuts import redirect
        from django.contrib import messages
        from django.utils import timezone
        import logging

        logger = logging.getLogger(__name__)
        logger.info(f"[ADMIN] Stop button clicked for bot {bot_id}")

        try:
            bot = Bot.objects.get(pk=bot_id)
            logger.info(f"[ADMIN] Bot {bot_id} current status: {bot.status}")

            if bot.status == 'RUNNING':
                # Update status directly instead of deferring
                bot.status = 'STOPPED'
                bot.stopped_at = timezone.now()
                bot.save()

                Event.objects.create(
                    bot=bot,
                    event_type='BOT_STOP',
                    level='INFO',
                    message="Bot stop requested from admin"
                )

                logger.info(f"[ADMIN] Bot {bot_id} status updated to STOPPED")
                messages.success(request, f'Bot "{bot.name or bot.symbol}" stopped')
            else:
                messages.warning(request, f'Bot is not running (status: {bot.status})')
        except Bot.DoesNotExist:
            messages.error(request, 'Bot not found')

        return redirect('admin:trading_bot_changelist')

    def panic_bot_view(self, request, bot_id):
        """
        PANIC BUTTON: Emergency stop that:
        1. Cancels all pending orders for this bot's accounts
        2. Closes all open positions for this bot's accounts
        3. Stops the bot
        """
        from django.shortcuts import redirect
        from django.contrib import messages
        from django.utils import timezone
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(f"[PANIC] Panic button pressed for bot {bot_id}")

        try:
            bot = Bot.objects.get(pk=bot_id)

            # Run panic in sync context with new event loop
            def execute_panic():
                """Execute panic operations synchronously"""
                import asyncio
                import nest_asyncio

                # Create a new event loop for this thread FIRST
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                # Allow nested event loops (ib_insync compatibility)
                nest_asyncio.apply(loop)

                # NOW import ib_insync after loop is set
                from ib_insync import IB, Stock, MarketOrder

                ib = IB()

                try:
                    # Connect to IBKR synchronously (ib_insync will use our loop)
                    port = 7497 if bot.environment == 'PAPER' else 7496
                    # Use Master Client ID (0) to cancel orders from any client
                    panic_client_id = 0

                    logger.info(f"[PANIC] Connecting to TWS on port {port} with Master clientId={panic_client_id}")

                    # Run connect in the loop we created
                    loop.run_until_complete(
                        ib.connectAsync('127.0.0.1', port, clientId=panic_client_id, timeout=10)
                    )

                    # Set up contract
                    contract = Stock(
                        bot.symbol.upper(),
                        bot.exchange,
                        bot.currency
                    )
                    if bot.primary_exchange:
                        contract.primaryExchange = bot.primary_exchange

                    loop.run_until_complete(ib.qualifyContractsAsync(contract))

                    # Request all open orders (including from other clients)
                    loop.run_until_complete(ib.reqAllOpenOrdersAsync())
                    ib.sleep(1)

                    cancelled_orders = 0
                    closed_positions = 0

                    # 1. Cancel all open orders for this bot's accounts
                    logger.info("[PANIC] Checking for open orders...")
                    for trade in ib.openTrades():
                        if (trade.contract.conId == contract.conId and
                            trade.order.account in [bot.long_account, bot.short_account]):
                            logger.info(
                                "[PANIC] Cancelling order %s (%s %s %s) on %s client %s",
                                trade.order.orderId,
                                trade.order.orderType,
                                trade.order.action,
                                trade.order.totalQuantity,
                                trade.order.account,
                                trade.order.clientId,
                            )
                            ib.cancelOrder(trade.order)
                            cancelled_orders += 1

                    if cancelled_orders > 0:
                        # Nuclear option to ensure everything is cancelled
                        ib.reqGlobalCancel()
                        logger.warning(f"[PANIC] reqGlobalCancel invoked for {cancelled_orders} orders")
                        ib.sleep(3)

                    # 2. Close all positions for this bot's accounts
                    for position in ib.positions():
                        if (position.contract.conId == contract.conId and
                            position.account in [bot.long_account, bot.short_account] and
                            position.position != 0):

                            qty = abs(position.position)
                            action = 'SELL' if position.position > 0 else 'BUY'

                            logger.warning(f"[PANIC] Closing position on {position.account}: {action} {qty} shares")

                            # Place market order to close
                            order = MarketOrder(action, qty, account=position.account)
                            ib.placeOrder(contract, order)
                            closed_positions += 1

                    # Wait for fills
                    if closed_positions > 0:
                        ib.sleep(3)

                    return cancelled_orders, closed_positions

                finally:
                    if ib.isConnected():
                        ib.disconnect()
                    try:
                        loop.close()
                    except Exception:
                        pass

            # Execute panic
            cancelled_orders, closed_positions = execute_panic()

            # 3. Stop the bot
            bot.status = 'STOPPED'
            bot.stopped_at = timezone.now()
            bot.save()

            Event.objects.create(
                bot=bot,
                event_type='PANIC_STOP',
                level='CRITICAL',
                message=f"PANIC: Cancelled {cancelled_orders} orders, closed {closed_positions} positions"
            )

            messages.warning(
                request,
                f'PANIC executed for "{bot.name or bot.symbol}": '
                f'Cancelled {cancelled_orders} orders, closed {closed_positions} positions, bot stopped'
            )

        except Bot.DoesNotExist:
            messages.error(request, 'Bot not found')
        except Exception as e:
            logger.error(f"[PANIC] Failed to execute panic: {e}", exc_info=True)
            messages.error(request, f'PANIC failed: {str(e)}')

        return redirect('admin:trading_bot_changelist')


@admin.register(ProcrastinateWorker)
class ProcrastinateWorkerAdmin(admin.ModelAdmin):
    list_display = ['id', 'last_heartbeat', 'doing_jobs']
    readonly_fields = ['last_heartbeat']
    ordering = ['-last_heartbeat']
    search_fields = ['id']

    def doing_jobs(self, obj):
        return obj.jobs.filter(status='doing').count()
    doing_jobs.short_description = 'Active jobs'


@admin.register(ProcrastinateJob)
class ProcrastinateJobAdmin(admin.ModelAdmin):
    list_display = ['id', 'task_name', 'queue_name', 'status', 'attempts', 'scheduled_at', 'worker_link', 'abort_requested']
    list_filter = ['status', 'queue_name', 'task_name', 'abort_requested']
    search_fields = ['id', 'task_name', 'queue_name', 'lock', 'queueing_lock']
    readonly_fields = ['id', 'task_name', 'queue_name', 'priority', 'lock', 'queueing_lock', 'args', 'status',
                      'scheduled_at', 'attempts', 'abort_requested', 'worker']
    ordering = ['-id']

    def worker_link(self, obj):
        if obj.worker_id:
            url = reverse('admin:trading_procrastinateworker_change', args=[obj.worker_id])
            return format_html('<a href="{}">Worker {}</a>', url, obj.worker_id)
        return '-'
    worker_link.short_description = 'Worker'


@admin.register(ProcrastinateEvent)
class ProcrastinateEventAdmin(admin.ModelAdmin):
    list_display = ['id', 'job', 'type', 'at']
    list_filter = ['type']
    search_fields = ['id', 'job__id', 'type']
    readonly_fields = ['job', 'type', 'at']
    ordering = ['-at', '-id']


@admin.register(Cycle)
class CycleAdmin(admin.ModelAdmin):
    list_display = ['id', 'bot', 'cycle_number', 'symbol', 'status', 'net_pnl', 'created_at', 'orders_button', 'events_button']
    list_filter = ['status', 'symbol']
    search_fields = ['symbol']
    readonly_fields = ['created_at', 'updated_at', 'completed_at', 'total_buys', 'total_sells', 'total_commission', 'net_pnl']
    raw_id_fields = ['bot']

    def orders_button(self, obj):
        """Display button to view orders for this cycle"""
        url = reverse('admin:trading_order_changelist') + f'?cycle_id__exact={obj.id}'
        return format_html('<a class="button" href="{}">Orders</a>', url)
    orders_button.short_description = 'Orders'

    def events_button(self, obj):
        """Display button to view events for this cycle"""
        url = reverse('admin:trading_event_changelist') + f'?cycle_id__exact={obj.id}'
        return format_html('<a class="button" href="{}">Events</a>', url)
    events_button.short_description = 'Logs'


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ['order_id', 'cycle', 'role', 'action', 'order_type', 'total_quantity', 'status', 'submitted_at']
    list_filter = ['status', 'role', 'action', 'order_type']
    search_fields = ['order_id', 'order_ref', 'account']
    readonly_fields = ['submitted_at', 'last_update_at', 'filled_at']
    raw_id_fields = ['cycle']


@admin.register(Execution)
class ExecutionAdmin(admin.ModelAdmin):
    list_display = ['exec_id', 'order', 'side', 'shares', 'price', 'commission', 'created_at']
    list_filter = ['side']
    search_fields = ['exec_id', 'account']
    readonly_fields = ['created_at', 'executed_at']
    raw_id_fields = ['order', 'cycle']


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ['id', 'event_type', 'level', 'bot', 'cycle', 'created_at', 'message_preview']
    list_filter = ['event_type', 'level', 'bot', 'created_at']
    search_fields = ['message']
    readonly_fields = ['created_at']
    raw_id_fields = ['bot', 'cycle', 'order']

    def message_preview(self, obj):
        return obj.message[:50]
    message_preview.short_description = 'Message'


# Custom admin view for chart
from django.urls import path
from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required

@staff_member_required
def chart_view(request):
    return render(request, 'admin/chart.html', {
        'title': 'Trading Chart',
        'site_header': admin.site.site_header,
        'site_title': admin.site.site_title,
        'has_permission': True,
    })

# Add custom URL to admin site
_original_get_urls = admin.site.get_urls

def custom_get_urls():
    custom_urls = [
        path('chart/', chart_view, name='trading_chart'),
    ]
    return custom_urls + _original_get_urls()

admin.site.get_urls = custom_get_urls
