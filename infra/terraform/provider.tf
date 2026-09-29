provider "aws" {
  region = "us-east-2"

  default_tags {
    tags = {
      Project     = "nyc-taxi-data-pipeline"
      Environment = "dev"
      ManagedBy   = "terraform"
      Repository  = "praneeth-sholapur/nyc-taxi-data-pipeline"
    }
  }
}