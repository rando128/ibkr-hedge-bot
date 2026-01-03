from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from .models import Bot, Cycle, Order, Execution, Event
from .tasks import start_bot, stop_bot


@admin.register(Bot)
class BotAdmin(admin.ModelAdmin):
    list_display = ['id', 'name', 'symbol', 'qty', 'status_badge', 'cycles_count', 'last_pnl', 'created_at', 'action_buttons']
    list_filter = ['status', 'symbol', 'created_at']
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
            'fields': ('port', 'use_algo')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at', 'started_at', 'stopped_at'),
            'classes': ('collapse',)
        }),
    )

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
            return format_html(
                '<span style="color: {}; font-weight: bold;">${:.2f}</span>',
                color, last_cycle.net_pnl
            )
        return '-'
    last_pnl.short_description = 'Last P&L'

    def action_buttons(self, obj):
        """Display start/stop buttons"""
        if obj.status == 'RUNNING':
            return format_html(
                '<a class="button" href="{}">Stop</a>',
                reverse('admin:trading_bot_stop', args=[obj.pk])
            )
        elif obj.status in ('IDLE', 'STOPPED'):
            return format_html(
                '<a class="button" href="{}">Start</a>',
                reverse('admin:trading_bot_start', args=[obj.pk])
            )
        return '-'
    action_buttons.short_description = 'Actions'

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
        ]
        return custom_urls + urls

    def start_bot_view(self, request, bot_id):
        """View to start a bot"""
        from django.shortcuts import redirect
        from django.contrib import messages

        try:
            bot = Bot.objects.get(pk=bot_id)
            if bot.status in ('IDLE', 'STOPPED', 'ERROR'):
                start_bot.defer(bot_id=bot.id)
                messages.success(request, f'Bot "{bot.name or bot.symbol}" scheduled to start')
            else:
                messages.warning(request, f'Bot is already {bot.status}')
        except Bot.DoesNotExist:
            messages.error(request, 'Bot not found')

        return redirect('admin:trading_bot_changelist')

    def stop_bot_view(self, request, bot_id):
        """View to stop a bot"""
        from django.shortcuts import redirect
        from django.contrib import messages

        try:
            bot = Bot.objects.get(pk=bot_id)
            if bot.status == 'RUNNING':
                stop_bot.defer(bot_id=bot.id)
                messages.success(request, f'Bot "{bot.name or bot.symbol}" scheduled to stop')
            else:
                messages.warning(request, f'Bot is not running (status: {bot.status})')
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
