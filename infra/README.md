# Rain O Meter infra

This folder contains the infrastructure (as code) to deploy backend on AWS.

You need to use your own AWS credentials to deploy this application to your own account.

## Clean code practises

- one folder to deploy all backend
- sensitive data handled as environment TF_VAR

This is mostly meant to be working and easy to deploy for a personal project, not for a production grade service.

For such requirements, you could add :
- state and lock file encrypted and stored remotely on AWS S3 and DDB _(instead of locally)_
- build and deployment made by CI/CD on GitHub using
    - stages to build the app then deploy it on Terraform _(instead of locally)_
    - sensitive environment variable storage in CI/CD configuration, or TF Vault _(instead of .env file)_
    - authentication between Github and AWS handled through OIDC configuration _(instead of using personal user account)_
    - authorization through an IAM role with appropriate permissions to create this project _(instead of using personal user account)_

## Setup

### Requirements

Mandatory :
- a complete `backend` setup to build it before deployment
- an AWS role or user with all IAM permissions required to build and deploy this project (see [how to launch a basic terminal with aws credentials](https://wellarchitectedlabs.com/common/documentation/aws_credentials/) or [use aws-vault for a more advanced but convenient setup](https://github.com/99designs/aws-vault))
- [Terraform](https://developer.hashicorp.com/terraform/install) to deploy infrastructure locally
- [a portail-api MétéoFrance account](https://portail-api.meteofrance.fr/web/fr) with a subscription to "Données Climatologiques" API to query real data through token
