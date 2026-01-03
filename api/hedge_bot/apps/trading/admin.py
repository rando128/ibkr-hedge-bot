from django.contrib import admin
from .models import Bot, Cycle, Order, Execution, Event


@admin.register(Bot)
class BotAdmin(admin.ModelAdmin):
    list_display = ['id', 'name', 'symbol', 'qty', 'status', 'created_at']
    list_filter = ['status', 'symbol']
    search_fields = ['name', 'symbol', 'long_account', 'short_account']
    readonly_fields = ['created_at', 'updated_at', 'started_at', 'stopped_at']


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
