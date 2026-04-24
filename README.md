# AWS S3 Backup for Home Assistant

A custom component that extends the built-in `aws_s3` integration with support for **EC2 IAM role credentials** (via IMDS), in addition to the standard static access key / secret key authentication.

## Features

- All features of the official [AWS S3 integration](https://www.home-assistant.io/integrations/aws_s3): native backup agent, upload/download/restore, multipart upload, backup size sensor
- **IAM role mode**: when running on an EC2 instance with an attached IAM role, no credentials need to be stored — boto3 fetches temporary credentials automatically from the instance metadata service (IMDS)
- Backwards compatible with static credentials (existing config entries continue to work)

## Installation

### HACS

1. Add this repository as a custom repository in HACS: `txtmode/ha-aws-s3-backup`
2. Install **AWS S3 Backup**
3. Restart Home Assistant

### Manual

Copy `custom_components/aws_s3/` into your Home Assistant config directory under `custom_components/`.

## Configuration

Go to **Settings → Devices & Services → Add Integration → AWS S3**.

| Field | Description |
|-------|-------------|
| Use IAM role | Enable to use EC2 instance profile (IMDS). No credentials needed. |
| Access key ID | Required if not using IAM role |
| Secret access key | Required if not using IAM role |
| Bucket name | Must already exist and be accessible |
| Endpoint URL | Region-specific S3 endpoint, e.g. `https://s3.eu-south-2.amazonaws.com/` |
| Prefix | Optional folder prefix, e.g. `backups` |

## IAM role requirements

The EC2 instance role needs the following S3 permissions on the bucket:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:ListBucket",
    "s3:DeleteObject"
  ],
  "Resource": [
    "arn:aws:s3:::your-bucket-name",
    "arn:aws:s3:::your-bucket-name/*"
  ]
}
```

## Notes

- This component overrides the built-in `aws_s3` integration. Remove it to revert to the built-in version.
- Requires `aiobotocore==2.21.1` (installed automatically via HACS or the HA requirements system).
- Tested with Home Assistant 2025.x.
