# GOV.UK Data Scrubber

This tool is intended to run in GOV.UK production environments in AWS to create
copies of backend databases with sensitive data removed. Such copies can then be
imported into development environments.

## Databases in scope

* MySQL:
  * `whitehall`
* Postgres:
  * `email-alert-api`
  * `publishing_api`

## Inputs

* RDS database cluster/instance
* Source snapshot name (default: latest automatic)
* Database credentials (should come from /root/.my.cnf)
* AWS credentials (as per boto3)
* Sharing details (AWS account(s), ...)

