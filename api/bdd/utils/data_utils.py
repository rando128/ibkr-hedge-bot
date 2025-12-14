"""
Data creation utils to be used in both test fixtures or demo worlds.

For example, users, pages, models, etc.
"""

import logging

import httpx
from django.contrib.auth import get_user_model

logger = logging.getLogger(__name__)




def get_or_create_admin_user():
    """
    Create a superuser for ease of debugging.

    Useful to see django / wagtail admin when debugging
    - Will be available in all tests implicitly, so you can
      log in to the admin with the credentials defined here.
    """
    email = "good@user.com"
    password = "correct"  # noqa: S105

    try:
        user = get_user_model().objects.get(email=email)
    except get_user_model().DoesNotExist:
        user = get_user_model().objects.create_superuser(
            email=email,
            password=password,
        )

    return user




def create_world():
    """
    Create a demo world for ease of development.

    Not a made into a Pytest fixture as it's meant to be used
    more universally, eg. management command/other fixtures/steps.
    """
    get_or_create_admin_user()




def remove_world():
    """
    Remove all data created by the create_world function.

    We manually delete as opposed to wiping the DB, as there is
    data created in the migrations that we want to keep, and
    migrating to zero is throwing an error with Wagtail.site Not found.

    Not made into a Pytest fixture as it's meant to be used
    more universally, eg. management command/other fixtures/steps.
    """
    get_user_model().objects.all().delete()
