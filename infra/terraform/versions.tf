terraform {
  required_version = ">= 1.10.0, < 2.0.0"

  backend "s3" {
    bucket       = "nyc-taxi-data-lake-03b62c07"
    key          = "terraform-state/dev/terraform.tfstate"
    region       = "us-east-2"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.7, < 3.0"
    }

    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }

    random = {
      source  = "hashicorp/random"
      version = ">= 3.7, < 4.0"
    }
  }
}