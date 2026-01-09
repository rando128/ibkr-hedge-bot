# Stub migration kept for compatibility with existing databases.
#
# The repo previously shipped migrations 0001..0008. We intentionally reset the
# trading schema in 0009 (data is disposable) and keep these stubs so Django's
# migration loader can match the migration history already present in DBs.

from django.db import migrations


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = []

