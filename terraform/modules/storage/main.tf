resource "aws_s3_bucket" "bronze" {
  bucket = "${var.env}-ecommerce-bronze-layer"
  tags   = { Environment = var.env, Layer = "bronze" }
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.env}-ecommerce-silver-layer"
}

resource "aws_s3_bucket" "gold_staging" {
  bucket = "${var.env}-ecommerce-gold-staging"
}

resource "aws_glue_catalog_database" "ecommerce" {
  name = "${var.env}_ecommerce"
}

resource "aws_iam_role" "glue_role" {
  name = "${var.env}-glue-etl-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}