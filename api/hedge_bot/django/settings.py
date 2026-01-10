from importlib import metadata
import logging.handlers
from pathlib import Path

from model_w.env_manager import EnvManager
from model_w.preset.django import ModelWDjango

BASE_DIR = Path(__file__).resolve().parents[2]

# Variables set by the EnvManager but declared here so IDEs don't complain
DEBUG = False
MIDDLEWARE = []
REST_FRAMEWORK = {}


def get_package_version() -> str:
    """
    Trying to get the current package version using the metadata module. This
    assumes that the version is indeed set in pyproject.toml and that the
    package was cleanly installed.
    """
    try:
        return metadata.version("hedge_bot")
    except metadata.PackageNotFoundError:
        return "0.0.0"


with EnvManager(ModelWDjango()) as env:
    # ---
    # Apps
    # ---

    INSTALLED_APPS = [
        "drf_spectacular",
        "drf_spectacular_sidecar",
        "hedge_bot.apps.realtime",
        "procrastinate.contrib.django",
        "hedge_bot.apps.people",
        # "hedge_bot.apps.health",
        "hedge_bot.apps.trading",
    ]

    # ---
    # Plumbing
    # ---

    ROOT_URLCONF = "hedge_bot.django.urls"

    WSGI_APPLICATION = "hedge_bot.django.wsgi.application"
    ASGI_APPLICATION = "hedge_bot.django.asgi.application"

    # ---
    # Auth
    # ---

    AUTH_USER_MODEL = "people.User"

    # ---
    # i18n
    # ---

    LANGUAGES = [
        ("en", "English"),
    ]

    # ---
    # Logging
    # ---
    MIDDLEWARE.append(
        "hedge_bot.django.middleware.RequestLogMiddleware"
    )

    DATA_UPLOAD_MAX_NUMBER_FIELDS = 10000

    # ---
    # OpenAPI Schema
    # ---

    REST_FRAMEWORK["DEFAULT_SCHEMA_CLASS"] = "drf_spectacular.openapi.AutoSchema"

    SPECTACULAR_SETTINGS = {
        "TITLE": "Hedge Bot",
        "VERSION": get_package_version(),
        "SERVE_INCLUDE_SCHEMA": False,
        "SWAGGER_UI_DIST": "SIDECAR",  # shorthand to use the sidecar instead
        "SWAGGER_UI_FAVICON_HREF": "SIDECAR",
        "REDOC_DIST": "SIDECAR",
    }

    if DEBUG:
        # Django Debug Toolbar
        INSTALLED_APPS.append("debug_toolbar")
        MIDDLEWARE.insert(1, "debug_toolbar.middleware.DebugToolbarMiddleware")
        INTERNAL_IPS = [
            "127.0.0.1",
        ]
        DEBUG_TOOLBAR_CONFIG = {
            "SHOW_COLLAPSED": True,
        }

        # Django Extensions
        INSTALLED_APPS.append("django_extensions")

# ---
# Final Logging Override
# ---
# ---
# Final Logging Override with Rotation
# ---
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} [{name}:{funcName}:{lineno}] {message}',
            'style': '{',
        },
    },
    'handlers': {
        'agent_file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': str(BASE_DIR / 'agent.log'),
            'when': 'M',
            'interval': 5,
            'backupCount': 48,
            'formatter': 'verbose',
            'encoding': 'utf-8',
            'delay': True,
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'agent_file'],
            'level': 'DEBUG',
        },
        'hedge_bot': {
            'level': 'DEBUG',
            'handlers': ['console', 'agent_file'],
            'propagate': False,
        },
        'ib_insync': {
            'level': 'WARNING',
            'handlers': ['console', 'agent_file'],
            'propagate': False,
        },
    },
}

import logging.config
logging.config.dictConfig(LOGGING)
