

# Manual setup (in the tenancy)

Create  `pathling-deploy` user with the following policies:

- AmazonElasticMapReduceFullAccess
- IAMReadOnlyAccess
- AmazonEC2FullAccess


# Managing the EMR cluster


## Prerequisites

- install terraform (v0.12.13 +)
- instal aws cli tool

Create a profile named `pathling-aehrc` in `~/.aws/credentials` with  keys for user `pathling-deploy`, eg:


	[pathling-aehrc]
	aws_access_key_id = ***REMOVED***
	aws_secret_access_key = *****************


Obtain the private key `pathling` and save it under: `~/.ssh/pathling.pem`. with `400` permissions.
This key can be used to ssh to EMR cluster master, e.g:

	ssh -i ~/.ssh/pathling.pem hadoop@<maste-public-dns>


Go  to `terraform` dir and initialize terraform with:

	terraform init


## Create/destroy the cluster


To create the cluster in `terrform` dir run:

	terraform apply

The outputs will include the URL to the fhir server, e.g:


	pathling_fhir_url = http://ec2-54-206-79-129.ap-southeast-2.compute.amazonaws.com:8888/fhir
	emr_cluster_id = j-2R197PG822ELH
	emr_cluster_master_dns = ec2-54-206-79-129.ap-southeast-2.compute.amazonaws.com
	emr_master_id = i-020c674c64cb603fd
	emr_master_private_ip = 172.31.12.105


To test the server you can use:

	./bin/check-metadata.sh


To import example dataset from (`s3://aehrc-pathling/share/data/test-set`):

	./bin/import.sh


To test a simple query:

	./bin/check-query.sh


To destory the cluster in `terrform` dir run:

	terrform destroy


# Tenancies:

	- aehrc: https://865780493209.signin.aws.amazon.com/console
	- csiro: https://csiro.awsapps.com/login/?client_id=0ea8cf0b2a7d3da3&redirect_uri=https://portal.sso.ap-southeast-2.amazonaws.com/auth/wd&organization=csiro


# Notes for the future


- Scala 2.12 on EMR: https://forums.aws.amazon.com/message.jspa?messageID=902431

# TODO:

- pickup up spark configuration from the context
- front load balancer/public IP configuration
- refactoring of the TERERFORM script
-- extract a module


