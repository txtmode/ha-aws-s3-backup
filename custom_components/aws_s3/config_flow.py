"""Config flow for the AWS S3 integration."""

import re
from typing import Any, override
from urllib.parse import urlparse

from aiobotocore.session import AioSession
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionError,
    NoCredentialsError,
    NoRegionError,
    ParamValidationError,
)
import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_PREFIX
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    AWS_DOMAIN,
    CONF_ACCESS_KEY_ID,
    CONF_BUCKET,
    CONF_ENDPOINT_URL,
    CONF_SECRET_ACCESS_KEY,
    CONF_USE_IAM_ROLE,
    DEFAULT_ENDPOINT_URL,
    DESCRIPTION_AWS_S3_DOCS_URL,
    DESCRIPTION_BOTO3_DOCS_URL,
    DESCRIPTION_DOCS_URL,
    DOMAIN,
)

# PATCH(txtmode): region as spelled in a virtual-hosted or path-style S3 endpoint,
# e.g. https://s3.eu-south-2.amazonaws.com/ -> eu-south-2
_ENDPOINT_REGION_RE = re.compile(r"s3[.\-]([a-z0-9\-]+)\.amazonaws\.com")

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        # PATCH(txtmode): keys become optional — unused in IAM-role mode
        vol.Required(CONF_USE_IAM_ROLE, default=False): cv.boolean,
        vol.Optional(CONF_ACCESS_KEY_ID): cv.string,
        vol.Optional(CONF_SECRET_ACCESS_KEY): TextSelector(
            config=TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
        vol.Required(CONF_BUCKET): cv.string,
        vol.Required(CONF_ENDPOINT_URL, default=DEFAULT_ENDPOINT_URL): TextSelector(
            config=TextSelectorConfig(type=TextSelectorType.URL)
        ),
        vol.Optional(CONF_PREFIX, default=""): cv.string,
    }
)


class S3ConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow."""

    @override
    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle a flow initiated by the user."""
        errors: dict[str, str] = {}

        if user_input is not None:
            use_iam_role = user_input.get(CONF_USE_IAM_ROLE, False)
            normalized_prefix = user_input.get(CONF_PREFIX, "").strip("/")
            # Check for existing entries, treating missing prefix as empty
            for entry in self._async_current_entries(include_ignore=False):
                entry_prefix = (entry.data.get(CONF_PREFIX) or "").strip("/")
                if (
                    entry.data.get(CONF_BUCKET) == user_input[CONF_BUCKET]
                    and entry.data.get(CONF_ENDPOINT_URL)
                    == user_input[CONF_ENDPOINT_URL]
                    and entry_prefix == normalized_prefix
                ):
                    return self.async_abort(reason="already_configured")

            hostname = urlparse(user_input[CONF_ENDPOINT_URL]).hostname
            if not hostname or not hostname.endswith(AWS_DOMAIN):
                errors[CONF_ENDPOINT_URL] = "invalid_endpoint_url"
            elif not use_iam_role and not (
                user_input.get(CONF_ACCESS_KEY_ID)
                and user_input.get(CONF_SECRET_ACCESS_KEY)
            ):
                # PATCH(txtmode): the keys are only optional in IAM-role mode
                errors["base"] = "credentials_required"
            else:
                try:
                    session = AioSession()
                    # PATCH(txtmode): IAM-role mode omits the keys so botocore
                    # falls back to IMDS. The region is pinned from the endpoint
                    # so signing never depends on an IMDS region lookup.
                    client_kwargs: dict[str, Any] = {
                        "endpoint_url": user_input.get(CONF_ENDPOINT_URL)
                    }
                    if match := _ENDPOINT_REGION_RE.search(
                        user_input[CONF_ENDPOINT_URL]
                    ):
                        client_kwargs["region_name"] = match.group(1)
                    if not use_iam_role:
                        client_kwargs["aws_secret_access_key"] = user_input[
                            CONF_SECRET_ACCESS_KEY
                        ]
                        client_kwargs["aws_access_key_id"] = user_input[
                            CONF_ACCESS_KEY_ID
                        ]
                    async with session.create_client("s3", **client_kwargs) as client:
                        await client.head_bucket(Bucket=user_input[CONF_BUCKET])
                # PATCH(txtmode): an unusable instance profile raises
                # NoCredentialsError/NoRegionError — surface them as form errors
                # instead of a 500 out of the flow handler.
                except (ClientError, NoCredentialsError):
                    errors["base"] = "invalid_credentials"
                except ParamValidationError as err:
                    if "Invalid bucket name" in str(err):
                        errors[CONF_BUCKET] = "invalid_bucket_name"
                except ValueError:
                    errors[CONF_ENDPOINT_URL] = "invalid_endpoint_url"
                except (ConnectionError, NoRegionError, BotoCoreError):
                    errors[CONF_ENDPOINT_URL] = "cannot_connect"
                else:
                    data = dict(user_input)
                    if use_iam_role:
                        # PATCH(txtmode): never persist empty credential fields
                        data.pop(CONF_ACCESS_KEY_ID, None)
                        data.pop(CONF_SECRET_ACCESS_KEY, None)
                    if not normalized_prefix:
                        # Do not persist empty optional values
                        data.pop(CONF_PREFIX, None)
                    else:
                        data[CONF_PREFIX] = normalized_prefix

                    title = user_input[CONF_BUCKET]
                    if normalized_prefix:
                        title = f"{title} - {normalized_prefix}"
                    if use_iam_role:
                        # PATCH(txtmode)
                        title = f"{title} (IAM role)"

                    return self.async_create_entry(title=title, data=data)

        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(
                STEP_USER_DATA_SCHEMA, user_input
            ),
            errors=errors,
            description_placeholders={
                "docs_url": DESCRIPTION_DOCS_URL,  # PATCH(txtmode)
                "aws_s3_docs_url": DESCRIPTION_AWS_S3_DOCS_URL,
                "boto3_docs_url": DESCRIPTION_BOTO3_DOCS_URL,
            },
        )
