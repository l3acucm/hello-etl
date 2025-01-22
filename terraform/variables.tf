variable "aws_access_key_id" {
  description = "AWS access key id"
  type        = string
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
  sensitive   = true
}
variable "aws_region" {
  description = "AWS region"
  type        = string
}

# variable "postgres_username" {
#   description = "Postgres username"
#   type        = string
# }
#
# variable "postgres_password" {
#   description = "Postgres password"
#   type        = string
# }
#
# variable "postgres_database" {
#   description = "Postgres database"
#   type        = string
# }
#
# variable "postgres_host" {
#   description = "Postgres host"
#   type        = string
# }
#
# variable "postgres_port" {
#   description = "Postgres port"
#   type        = string
# }
