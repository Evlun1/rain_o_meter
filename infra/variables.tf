variable "aws_region" {
  type    = string
  default = "eu-west-3"
}

variable "mf_token" {
  description = "Météo France token to request their rain data"
  type        = string
  sensitive   = true
}

variable "project_name" {
  type    = string
  default = "rain_o_meter"
}
