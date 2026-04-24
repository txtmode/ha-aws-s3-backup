"""Config flow for the AWS S3 integration."""

from __future__ import annotations

from typing import Any

from aiobotocore.session import AioSession
from botocore.exceptions import ClientError, ConnectionError, ParamValidationError
import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    CONF_ACCESS_KEY_ID,
    CONF_BUCKET,
    CONF_ENDPOINT_URL,
    CONF_PREFIX,
    CONF_SECRET_ACCESS_KEY,
    CONF_USE_IAM_ROLE,
    DEFAULT_ENDPOINT_URL,
    DESCRIPTION_AWS_S3_DOCS_URL,
    DESCRIPTION_BOTO3_DOCS_URL,
    DESCRIPTION_DOCS_URL,
    DOMAIN,
)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USE_IAM_ROLE, default=False): bool,
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

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle a flow initiated by the user."""
        errors: dict[str, str] = {}

        if user_input is not None:
            use_iam_role = user_input.get(CONF_USE_IAM_ROLE, False)
            normalized_prefix = user_input.get(CONF_PREFIX, "").strip("/")

            for entry in self._async_current_entries(include_ignore=False):
                entry_prefix = (entry.data.get(CONF_PREFIX) or "").strip("/")
                if (
                    entry.data.get(CONF_BUCKET) == user_input[CONF_BUCKET]
                    and entry.data.get(CONF_ENDPOINT_URL)
                    == user_input[CONF_ENDPOINT_URL]
                    and entry_prefix == normalized_prefix
                ):
                    return self.async_abort(reason="already_configured")

            if not use_iam_role and (
                not user_input.get(CONF_ACCESS_KEY_ID)
                or not user_input.get(CONF_SECRET_ACCESS_KEY)
            ):
                errors["base"] = "credentials_required"
            else:
                try:
                    session = AioSession()
                    client_kwargs: dict[str, Any] = {
                        "endpoint_url": user_input.get(CONF_ENDPOINT_URL) or None,
                    }
                    if not use_iam_role:
                        client_kwargs["aws_access_key_id"] = user_input[CONF_ACCESS_KEY_ID]
                        client_kwargs["aws_secret_access_key"] = user_input[CONF_SECRET_ACCESS_KEY]

                    async with session.create_client("s3", **client_kwargs) as client:
                        await client.head_bucket(Bucket=user_input[CONF_BUCKET])
                except ClientError:
                    errors["base"] = "invalid_credentials"
                except ParamValidationError as err:
                    if "Invalid bucket name" in str(err):
                        errors[CONF_BUCKET] = "invalid_bucket_name"
                except ValueError:
                    errors[CONF_ENDPOINT_URL] = "invalid_endpoint_url"
                except ConnectionError:
                    errors[CONF_ENDPOINT_URL] = "cannot_connect"
                else:
                    data: dict[str, Any] = {
                        CONF_USE_IAM_ROLE: use_iam_role,
                        CONF_BUCKET: user_input[CONF_BUCKET],
                        CONF_ENDPOINT_URL: user_input.get(CONF_ENDPOINT_URL) or None,
                    }
                    if not use_iam_role:
                        data[CONF_ACCESS_KEY_ID] = user_input[CONF_ACCESS_KEY_ID]
                        data[CONF_SECRET_ACCESS_KEY] = user_input[CONF_SECRET_ACCESS_KEY]
                    if normalized_prefix:
                        data[CONF_PREFIX] = normalized_prefix

                    title = user_input[CONF_BUCKET]
                    if normalized_prefix:
                        title = f"{title} - {normalized_prefix}"
                    if use_iam_role:
                        title = f"{title} (IAM role)"

                    return self.async_create_entry(title=title, data=data)

        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(
                STEP_USER_DATA_SCHEMA, user_input
            ),
            errors=errors,
            description_placeholders={
                "docs_url": DESCRIPTION_DOCS_URL,
                "aws_s3_docs_url": DESCRIPTION_AWS_S3_DOCS_URL,
                "boto3_docs_url": DESCRIPTION_BOTO3_DOCS_URL,
            },
        )
