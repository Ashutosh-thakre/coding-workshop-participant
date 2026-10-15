# Data lake for the talent pipeline (data/talent-pipeline): the S3 bucket, read/write access for the
# EKS job pods, and the settings every data job receives (see helm_python_job in eks.tf).

locals {
  is_aws          = data.aws_caller_identity.this.id != "000000000000"
  datalake_bucket = format("%s-datalake-%s", var.aws_project, local.app_id)

  # Environment of the data jobs on EKS. The database settings reuse local.env_vars (as Lambda does).
  data_job_env = {
    IS_LOCAL      = local.env_vars.IS_LOCAL
    POSTGRES_HOST = local.env_vars.POSTGRES_HOST
    POSTGRES_PORT = tostring(local.env_vars.POSTGRES_PORT)
    POSTGRES_NAME = local.env_vars.POSTGRES_NAME
    POSTGRES_USER = local.env_vars.POSTGRES_USER
    POSTGRES_PASS = local.env_vars.POSTGRES_PASS
    LAKE_BUCKET   = local.datalake_bucket
    AWS_REGION    = data.aws_region.this.region
    SPARK_CORES   = "1" # matches the pod's CPU limit; keeps peak memory under its 2Gi limit
  }
}

resource "aws_s3_bucket" "datalake" {
  count         = local.is_aws ? 1 : 0
  bucket        = local.datalake_bucket
  force_destroy = true

  tags = local.app_tags
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  count                   = local.is_aws ? 1 : 0
  bucket                  = one(aws_s3_bucket.datalake.*.id)
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The EKS nodes (and so the job pods) run as the EKS role, which has no S3 access by default.
# Grant read/write on the data-lake bucket only.
resource "aws_iam_role_policy" "eks_datalake" {
  count = local.is_aws && var.aws_eks_enabled ? 1 : 0
  name  = format("%s-datalake-%s", var.aws_project, local.app_id)
  role  = format("%s-eks-%s-%s", var.aws_project, data.aws_region.this.region, local.app_id)

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = one(aws_s3_bucket.datalake.*.arn)
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${one(aws_s3_bucket.datalake.*.arn)}/*"
      },
    ]
  })
}
