from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from .models import Bot, Cycle, Order, Execution, Event
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
                # PREFLIGHT CHECK: Verify no existing positions or orders
                def check_ibkr_state():
                    """Check for existing positions and orders"""
                    import asyncio
                    import nest_asyncio

                    # Create event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    nest_asyncio.apply(loop)

                    # Import ib_insync after loop is set
                    from ib_insync import IB, Stock

                    ib = IB()

                    try:
                        # Connect to IBKR
                        port = 7497 if bot.environment == 'PAPER' else 7496
                        check_client_id = 998000 + bot.id

                        logger.info(f"[PREFLIGHT] Connecting to TWS for preflight check...")
                        loop.run_until_complete(
                            ib.connectAsync('127.0.0.1', port, clientId=check_client_id, timeout=10)
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
                        # Give TWS time to send all orders
                        ib.sleep(1)

                        # Check for positions
                        positions = [p for p in ib.positions()
                                   if p.contract.conId == contract.conId
                                   and p.account in [bot.long_account, bot.short_account]
                                   and p.position != 0]

                        # Check for open orders
                        open_orders = [t for t in ib.openTrades()
                                     if t.contract.conId == contract.conId
                                     and t.order.account in [bot.long_account, bot.short_account]]

                        return positions, open_orders, contract.conId

                    finally:
                        if ib.isConnected():
                            ib.disconnect()
                        try:
                            loop.close()
                        except:
                            pass

                # Run preflight check
                try:
                    positions, open_orders, contract_id = check_ibkr_state()

                    # If there are positions or orders, block the start
                    if positions or open_orders:
                        error_msgs = []

                        if positions:
                            error_msgs.append(f"Found {len(positions)} open position(s):")
                            for p in positions:
                                error_msgs.append(f"  • {p.account}: {p.position} shares @ ${p.avgCost:.2f}")

                        if open_orders:
                            error_msgs.append(f"Found {len(open_orders)} pending order(s):")
                            for t in open_orders:
                                error_msgs.append(f"  • Order {t.order.orderId} on {t.order.account}: "
                                                f"{t.order.action} {t.order.totalQuantity} {t.order.orderType} "
                                                f"(Status: {t.orderStatus.status})")

                        error_msgs.append("Please use the PANIC button to clean up before starting the bot.")

                        messages.error(request, format_html('<br>'.join(error_msgs)))

                        logger.warning(f"[PREFLIGHT] Bot {bot_id} start blocked due to existing positions/orders")

                        Event.objects.create(
                            bot=bot,
                            event_type='BOT_START_BLOCKED',
                            level='WARNING',
                            message=f"Bot start blocked: {len(positions)} positions, {len(open_orders)} orders"
                        )

                        return redirect('admin:trading_bot_changelist')

                    logger.info(f"[PREFLIGHT] Check passed - no positions or orders found (Contract ID: {contract_id})")

                except Exception as e:
                    logger.error(f"[PREFLIGHT] Check failed: {e}", exc_info=True)
                    messages.error(request, f'Preflight check failed: {str(e)}. Cannot start bot.')
                    return redirect('admin:trading_bot_changelist')

                # Preflight passed - start the bot
                bot.status = 'RUNNING'
                bot.started_at = timezone.now()
                bot.stopped_at = None
                bot.save()

                Event.objects.create(
                    bot=bot,
                    event_type='BOT_START',
                    level='INFO',
                    message=f"Bot start requested from admin (preflight check passed)"
                )

                # Launch worker immediately
                run_bot_worker.defer(bot_id=bot.id)

                messages.success(request, f'Bot "{bot.name or bot.symbol}" started (preflight check passed)')
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
        print(f"[ADMIN] Stop button clicked for bot {bot_id}")

        try:
            bot = Bot.objects.get(pk=bot_id)
            logger.info(f"[ADMIN] Bot {bot_id} current status: {bot.status}")
            print(f"[ADMIN] Bot {bot_id} current status: {bot.status}")

            if bot.status == 'RUNNING':
                # Update status directly instead of deferring
                logger.info(f"[ADMIN] Setting bot {bot_id} status to STOPPED directly")
                print(f"[ADMIN] Setting bot {bot_id} status to STOPPED directly")

                bot.status = 'STOPPED'
                bot.stopped_at = timezone.now()
                bot.save()

                Event.objects.create(
                    bot=bot,
                    event_type='BOT_STOP',
                    level='INFO',
                    message=f"Bot stop requested from admin"
                )

                logger.info(f"[ADMIN] Bot {bot_id} status updated to STOPPED")
                print(f"[ADMIN] Bot {bot_id} status updated to STOPPED")
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

                # Don't call util.patchAsyncio() - we're managing the loop ourselves

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
                    # Give TWS time to send all orders
                    ib.sleep(1)

                    cancelled_orders = 0
                    closed_positions = 0

                    # 1. Cancel all open orders for this bot's accounts
                    print(f"[PANIC] Checking for open orders...")
                    logger.info(f"[PANIC] Checking for open orders...")
                    print(f"[PANIC] Found {len(ib.openTrades())} total open trades")
                    logger.info(f"[PANIC] Found {len(ib.openTrades())} total open trades")

                    # Count orders that need cancellation
                    orders_for_this_bot = []
                    for trade in ib.openTrades():
                        if (trade.contract.conId == contract.conId and
                            trade.order.account in [bot.long_account, bot.short_account]):
                            print(f"[PANIC] Found order {trade.order.orderId} "
                                  f"({trade.order.orderType} {trade.order.action} {trade.order.totalQuantity}) "
                                  f"on {trade.order.account} - placed by client {trade.order.clientId}")
                            orders_for_this_bot.append(trade)
                            cancelled_orders += 1

                    print(f"[PANIC] Total orders to cancel: {cancelled_orders}")

                    # Use Master Client ID (0) to cancel orders placed by any client
                    if cancelled_orders > 0:
                        print(f"[PANIC] Using reqGlobalCancel to cancel ALL orders...")
                        logger.warning(f"[PANIC] Using reqGlobalCancel to cancel ALL orders")

                        # reqGlobalCancel() cancels ALL orders for ALL accounts on this API connection
                        # This is the nuclear option but it works across all clients
                        ib.reqGlobalCancel()

                        print(f"[PANIC] Waiting for {cancelled_orders} order cancellations...")
                        logger.info(f"[PANIC] Waiting for {cancelled_orders} order cancellations...")
                        ib.sleep(3)

                        # Re-fetch all orders to check status
                        loop.run_until_complete(ib.reqAllOpenOrdersAsync())
                        ib.sleep(1)

                        # Verify cancellations
                        still_open_count = 0
                        for trade in ib.openTrades():
                            if (trade.contract.conId == contract.conId and
                                trade.order.account in [bot.long_account, bot.short_account]):
                                print(f"[PANIC] ⚠ Order {trade.order.orderId} still open, status: {trade.orderStatus.status}")
                                logger.warning(f"[PANIC] Order {trade.order.orderId} still open")
                                still_open_count += 1

                        if still_open_count > 0:
                            print(f"[PANIC] ⚠ {still_open_count} orders still open after cancellation attempt")
                            logger.warning(f"[PANIC] {still_open_count} orders still open after cancellation attempt")
                        else:
                            print(f"[PANIC] ✓ All {cancelled_orders} orders successfully cancelled")
                            logger.info(f"[PANIC] All {cancelled_orders} orders successfully cancelled")
                    else:
                        print("[PANIC] No orders matched for cancellation")

                    # 2. Close all positions for this bot's accounts
                    logger.info(f"[PANIC] Checking for open positions...")
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
                        logger.info(f"[PANIC] Waiting for {closed_positions} position closures...")
                        ib.sleep(3)

                    return cancelled_orders, closed_positions

                except Exception as e:
                    logger.error(f"[PANIC] Error during panic operations: {e}", exc_info=True)
                    raise
                finally:
                    if ib.isConnected():
                        ib.disconnect()
                        logger.info("[PANIC] Disconnected from TWS")

                    # Clean up event loop
                    try:
                        loop.close()
                    except:
                        pass

            # Execute panic
            try:
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

            except Exception as e:
                logger.error(f"[PANIC] Failed to execute panic: {e}", exc_info=True)
                messages.error(request, f'PANIC failed: {str(e)}')

        except Bot.DoesNotExist:
            messages.error(request, 'Bot not found')

        return redirect('admin:trading_bot_changelist')


@admin.register(Cycle)
class CycleAdmin(admin.ModelAdmin):
    list_display = ['id', 'bot', 'cycle_number', 'symbol', 'status', 'net_pnl', 'created_at']
    list_filter = ['status', 'symbol']
    search_fields = ['symbol']
    readonly_fields = ['created_at', 'updated_at', 'completed_at', 'total_buys', 'total_sells', 'total_commission', 'net_pnl']
    raw_id_fields = ['bot']


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ['order_id', 'cycle', 'role', 'action', 'order_type', 'total_quantity', 'status', 'submitted_at']
    list_filter = ['status', 'role', 'action', 'order_type']
    search_fields = ['order_id', 'order_ref', 'account']
    readonly_fields = ['submitted_at', 'last_update_at', 'filled_at']
    raw_id_fields = ['cycle']


@admin.register(Execution)
class ExecutionAdmin(admin.ModelAdmin):
    list_display = ['exec_id', 'order', 'side', 'shares', 'price', 'commission', 'executed_at']
    list_filter = ['side']
    search_fields = ['exec_id', 'account']
    readonly_fields = ['created_at', 'executed_at']
    raw_id_fields = ['order', 'cycle']


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ['id', 'event_type', 'level', 'bot', 'cycle', 'created_at', 'message_preview']
    list_filter = ['event_type', 'level', 'created_at']
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
