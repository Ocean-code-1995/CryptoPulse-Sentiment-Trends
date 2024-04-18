
variable "credentials" {
  description = "Path to the keyfile containing GCP credentials."
  type        = string
  default     = "../secrets/cryptopulse-secret.json"
  validation {
    condition     = length(var.credentials) > 0 && can(regex("^.*\\.json$", var.credentials))
    error_message = "The credentials must be a non-empty string ending in '.jsterron'."
  }
}
variable "region" {
  type        = string
  description = "The default compute region"
  default     = "europe-west3"
}
variable "expiration_time" {
  type        = string
  description = "The expiration time for the JWT token"
  default     = "24h"
}
