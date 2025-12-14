"""
Fixtures related to any global data needed when testing

For example, users, pages, models, etc.
"""

import pytest
from bdd.utils import data_utils
from pytest_django.fixtures import SettingsWrapper


@pytest.fixture(autouse=True)
def admin_user():
    """Fixture to make sure the admin user is set up."""
    return data_utils.get_or_create_admin_user()


