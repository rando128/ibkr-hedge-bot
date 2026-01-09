import uuid

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("trading", "0008_order_total_commission_alter_event_event_type"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP TABLE IF EXISTS trading_executions CASCADE;
            DROP TABLE IF EXISTS trading_orders CASCADE;
            DROP TABLE IF EXISTS trading_events CASCADE;
            DROP TABLE IF EXISTS trading_cycles CASCADE;
            DROP TABLE IF EXISTS trading_bots CASCADE;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.CreateModel(
            name="Bot",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(blank=True, help_text="Optional bot name", max_length=100)),
                ("symbol", models.CharField(db_index=True, max_length=20)),
                ("qty", models.DecimalField(decimal_places=2, max_digits=12)),
                ("stop_pct", models.DecimalField(decimal_places=6, help_text="Stop loss ratio (e.g., 0.02 for 2%)", max_digits=8)),
                ("trailing_pct", models.DecimalField(decimal_places=6, help_text="Trailing stop ratio (e.g., 0.01 for 1%)", max_digits=8)),
                ("primary_exchange", models.CharField(blank=True, help_text="Primary exchange (e.g., 'NYSE', 'NASDAQ', 'SBF'). Leave blank if not needed.", max_length=20)),
                ("exchange", models.CharField(default="SMART", help_text="Routing exchange (usually 'SMART')", max_length=20)),
                ("currency", models.CharField(default="USD", help_text="Currency (e.g., 'USD', 'EUR')", max_length=3)),
                ("long_account", models.CharField(max_length=50)),
                ("short_account", models.CharField(max_length=50)),
                ("environment", models.CharField(choices=[("PAPER", "Paper Trading"), ("LIVE", "Live Trading")], default="PAPER", max_length=10)),
                ("use_algo", models.BooleanField(default=False, help_text="Use IBKR Adaptive algo (optional)")),
                ("status", models.CharField(choices=[("RUNNING", "Running"), ("STOPPING", "Stopping (finish current cycle)"), ("STOPPED", "Stopped"), ("ERROR", "Error")], db_index=True, default="STOPPED", max_length=20)),
                ("panic_requested", models.BooleanField(default=False, help_text="Operator emergency exit request")),
                ("last_error", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("stopped_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "trading_bots",
                "ordering": ["-created_at"],
                "indexes": [
                    models.Index(fields=["symbol", "status"], name="trading_bot_symbol_3c9cad_idx"),
                    models.Index(fields=["created_at"], name="trading_bot_created_1d86a5_idx"),
                ],
            },
        ),
        migrations.CreateModel(
            name="Cycle",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cycle_number", models.IntegerField(help_text="Sequential cycle number for this bot")),
                ("cycle_key", models.UUIDField(db_index=True, default=uuid.uuid4, unique=True)),
                ("symbol", models.CharField(max_length=20)),
                ("contract_id", models.IntegerField(blank=True, help_text="IBKR conId", null=True)),
                ("min_tick", models.DecimalField(decimal_places=6, default=0.01, max_digits=12)),
                ("state", models.CharField(choices=[("INITIALIZING", "Initializing"), ("ENTERING", "Entering (entries + SL protection)"), ("ACTIVE", "Active (both SL protections in place)"), ("TRANSITIONING", "Transitioning (flip SL to trailing)"), ("TRAILING", "Trailing active"), ("RECOVERING", "Recovering"), ("PANIC", "Panic (flatten/cancel)"), ("PNL_CALCULATION", "P&L calculation"), ("COMPLETED", "Completed"), ("ABORTED", "Aborted"), ("ERROR", "Error")], db_index=True, default="INITIALIZING", max_length=20)),
                ("started_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("last_activity_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                ("total_buys", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("total_sells", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("total_commission", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("net_pnl", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("bot", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="cycles", to="trading.bot")),
            ],
            options={
                "db_table": "trading_cycles",
                "ordering": ["-started_at"],
                "indexes": [
                    models.Index(fields=["bot", "cycle_number"], name="trading_cyc_bot_id_49c52a_idx"),
                    models.Index(fields=["bot", "state"], name="trading_cyc_bot_id_6d8264_idx"),
                    models.Index(fields=["cycle_key"], name="trading_cyc_cycle_k_af1dab_idx"),
                ],
            },
        ),
        migrations.CreateModel(
            name="Event",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("level", models.CharField(choices=[("DEBUG", "Debug"), ("INFO", "Info"), ("WARNING", "Warning"), ("ERROR", "Error"), ("CRITICAL", "Critical")], db_index=True, default="INFO", max_length=10)),
                ("event_type", models.CharField(db_index=True, max_length=50)),
                ("message", models.TextField()),
                ("data", models.JSONField(blank=True, null=True)),
                ("from_state", models.CharField(blank=True, default="", max_length=20)),
                ("to_state", models.CharField(blank=True, default="", max_length=20)),
                ("created_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("bot", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="events", to="trading.bot")),
                ("cycle", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="events", to="trading.cycle")),
            ],
            options={
                "db_table": "trading_events",
                "ordering": ["-created_at"],
                "indexes": [
                    models.Index(fields=["bot", "created_at"], name="trading_eve_bot_id_10b530_idx"),
                    models.Index(fields=["cycle", "created_at"], name="trading_eve_cycle_i_b00234_idx"),
                    models.Index(fields=["event_type", "created_at"], name="trading_eve_event_t_eca820_idx"),
                    models.Index(fields=["level", "created_at"], name="trading_eve_level_1c1c4d_idx"),
                ],
            },
        ),
    ]

