from __future__ import annotations

import logging

from django.contrib import admin, messages
from django.shortcuts import redirect
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html

from .models import Bot, Cycle, Event

logger = logging.getLogger(__name__)


@admin.register(Bot)
class BotAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "name",
        "symbol",
        "qty",
        "environment_badge",
        "status_badge",
        "last_cycle_state",
        "action_buttons",
        "log_button",
    ]
    list_filter = ["status", "environment", "symbol", "created_at"]
    search_fields = ["name", "symbol", "long_account", "short_account"]
    readonly_fields = ["created_at", "updated_at", "started_at", "stopped_at", "last_error"]
    actions = ["action_start_bots", "action_stop_bots", "action_panic_bots"]

    fieldsets = (
        ("Identification", {"fields": ("name", "status")}),
        (
            "Contract Configuration",
            {
                "fields": ("symbol", "primary_exchange", "exchange", "currency"),
            },
        ),
        (
            "Trading Parameters",
            {
                "fields": ("qty", "stop_pct", "trailing_pct"),
                "description": "Convention: 0.02 == 2%",
            },
        ),
        ("Accounts", {"fields": ("long_account", "short_account")}),
        ("Connection", {"fields": ("environment", "use_algo")}),
        ("Operations", {"fields": ("panic_requested", "last_error")}),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at", "started_at", "stopped_at"), "classes": ("collapse",)},
        ),
    )

    def environment_badge(self, obj: Bot):
        colors = {"PAPER": "#2196F3", "LIVE": "#FF5722"}
        labels = {"PAPER": "Paper", "LIVE": "Live"}
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px; font-weight: bold;">{}</span>',
            colors.get(obj.environment, "gray"),
            labels.get(obj.environment, obj.environment),
        )

    environment_badge.short_description = "Environment"

    def status_badge(self, obj: Bot):
        colors = {"RUNNING": "green", "STOPPING": "#f0ad4e", "STOPPED": "gray", "ERROR": "red"}
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px; font-weight: bold;">{}</span>',
            colors.get(obj.status, "gray"),
            obj.status,
        )

    status_badge.short_description = "Status"

    def last_cycle_state(self, obj: Bot):
        cycle = obj.cycles.order_by("-id").first()
        if not cycle:
            return "-"
        url = reverse("admin:trading_cycle_change", args=[cycle.id])
        return format_html('<a href="{}">{}</a>', url, cycle.state)

    last_cycle_state.short_description = "Last Cycle"

    def action_buttons(self, obj: Bot):
        buttons = []
        if obj.status in ("STOPPED", "ERROR"):
            buttons.append(format_html('<a class="button" href="{}">Start</a>', reverse("admin:trading_bot_start", args=[obj.pk])))
        elif obj.status in ("RUNNING", "STOPPING"):
            buttons.append(format_html('<a class="button" href="{}">Stop</a>', reverse("admin:trading_bot_stop", args=[obj.pk])))

        buttons.append(
            format_html(
                '<a class="button" href="{}" style="background-color: #dc3545; margin-left: 5px;" '
                'onclick="return confirm(\'PANIC: This will request a flatten/cancel for this bot. Continue?\')">🚨</a>',
                reverse("admin:trading_bot_panic", args=[obj.pk]),
            )
        )
        return format_html(" ".join(buttons)) if buttons else "-"

    action_buttons.short_description = "Actions"

    def log_button(self, obj: Bot):
        url = reverse("admin:trading_event_changelist") + f"?bot__id__exact={obj.id}"
        return format_html('<a class="button" href="{}">Logs</a>', url)

    log_button.short_description = "Logs"

    def action_start_bots(self, request, queryset):
        count = 0
        for bot in queryset:
            if bot.status in ("STOPPED", "ERROR"):
                bot.status = "RUNNING"
                bot.started_at = timezone.now()
                bot.stopped_at = None
                bot.panic_requested = False
                bot.last_error = ""
                bot.save(update_fields=["status", "started_at", "stopped_at", "panic_requested", "last_error"])
                Event.objects.create(bot=bot, level="INFO", event_type="BOT_START", message="Bot start requested (bulk)")
                count += 1
        self.message_user(request, f"{count} bot(s) set to RUNNING")

    action_start_bots.short_description = "Start selected bots"

    def action_stop_bots(self, request, queryset):
        count = 0
        for bot in queryset:
            if bot.status == "RUNNING":
                bot.status = "STOPPING"
                bot.stopped_at = timezone.now()
                bot.save(update_fields=["status", "stopped_at"])
                Event.objects.create(bot=bot, level="INFO", event_type="BOT_STOP", message="Bot stop requested (bulk)")
                count += 1
        self.message_user(request, f"{count} bot(s) set to STOPPING")

    action_stop_bots.short_description = "Stop selected bots (finish cycle)"

    def action_panic_bots(self, request, queryset):
        count = 0
        for bot in queryset:
            bot.panic_requested = True
            bot.save(update_fields=["panic_requested"])
            Event.objects.create(bot=bot, level="CRITICAL", event_type="PANIC_REQUESTED", message="PANIC requested (bulk)")
            count += 1
        self.message_user(request, f"{count} bot(s) panic_requested=True")

    action_panic_bots.short_description = "PANIC selected bots"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("<int:bot_id>/start/", self.admin_site.admin_view(self.start_bot_view), name="trading_bot_start"),
            path("<int:bot_id>/stop/", self.admin_site.admin_view(self.stop_bot_view), name="trading_bot_stop"),
            path("<int:bot_id>/panic/", self.admin_site.admin_view(self.panic_bot_view), name="trading_bot_panic"),
        ]
        return custom_urls + urls

    def start_bot_view(self, request, bot_id: int):
        try:
            bot = Bot.objects.get(pk=bot_id)
        except Bot.DoesNotExist:
            messages.error(request, "Bot not found")
            return redirect("admin:trading_bot_changelist")

        if bot.status not in ("STOPPED", "ERROR"):
            messages.warning(request, f'Bot is already {bot.status}')
            return redirect("admin:trading_bot_changelist")

        bot.status = "RUNNING"
        bot.started_at = timezone.now()
        bot.stopped_at = None
        bot.panic_requested = False
        bot.last_error = ""
        bot.save(update_fields=["status", "started_at", "stopped_at", "panic_requested", "last_error"])
        Event.objects.create(bot=bot, level="INFO", event_type="BOT_START", message="Bot start requested from admin")
        messages.success(request, f'Bot "{bot.name or bot.symbol}" set to RUNNING')
        return redirect("admin:trading_bot_changelist")

    def stop_bot_view(self, request, bot_id: int):
        try:
            bot = Bot.objects.get(pk=bot_id)
        except Bot.DoesNotExist:
            messages.error(request, "Bot not found")
            return redirect("admin:trading_bot_changelist")

        if bot.status != "RUNNING":
            messages.warning(request, f"Bot is not RUNNING (status={bot.status})")
            return redirect("admin:trading_bot_changelist")

        bot.status = "STOPPING"
        bot.stopped_at = timezone.now()
        bot.save(update_fields=["status", "stopped_at"])
        Event.objects.create(bot=bot, level="INFO", event_type="BOT_STOP", message="Bot stop requested from admin")
        messages.success(request, f'Bot "{bot.name or bot.symbol}" set to STOPPING')
        return redirect("admin:trading_bot_changelist")

    def panic_bot_view(self, request, bot_id: int):
        try:
            bot = Bot.objects.get(pk=bot_id)
        except Bot.DoesNotExist:
            messages.error(request, "Bot not found")
            return redirect("admin:trading_bot_changelist")

        bot.panic_requested = True
        bot.save(update_fields=["panic_requested"])
        Event.objects.create(bot=bot, level="CRITICAL", event_type="PANIC_REQUESTED", message="PANIC requested from admin")
        messages.warning(request, f'PANIC requested for "{bot.name or bot.symbol}"')
        return redirect("admin:trading_bot_changelist")


@admin.register(Cycle)
class CycleAdmin(admin.ModelAdmin):
    list_display = ["id", "bot", "cycle_number", "symbol", "state", "started_at", "last_activity_at", "net_pnl"]
    list_filter = ["state", "symbol", "started_at"]
    search_fields = ["symbol", "bot__symbol", "bot__name", "cycle_key"]
    readonly_fields = ["cycle_key", "started_at", "last_activity_at", "completed_at", "total_buys", "total_sells", "total_commission", "net_pnl"]


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ["created_at", "level", "event_type", "bot", "cycle", "from_state", "to_state", "message"]
    list_filter = ["level", "event_type", "created_at"]
    search_fields = ["message", "event_type", "bot__symbol", "bot__name", "cycle__symbol", "cycle__cycle_key"]
    readonly_fields = ["created_at"]

