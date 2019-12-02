provider "aws" {
  region                  = "ap-southeast-2"
  profile                 = "clinsight-aehrc"
  shared_credentials_file = "$HOME/.aws/credentials"
}

variable "vpc_id" {
  default = null
  description = "The ID of VPC to create the cluster in, if empty default VPC is used"
}

variable "zone_id" {
  type        = string
  default     = "ap-southeast-2a"
  description = "Name of the availability zone to use"
}

variable "key_name" {
  type        = string
  default     = "clinsight"
  description = "The ssh key name to use for the EMR cluster"
}

variable "clinsight_bucket" {
  type        = string
  default     = "aehrc-clinsight"
  description = "The bucket to use for setup"
}

data "aws_vpc" "default" {
  default = var.vpc_id == null ? true : false
  id = var.vpc_id
}

data "aws_region" "current" {
}

data "aws_subnet" "subnet" {
  vpc_id               = data.aws_vpc.default.id
  availability_zone = var.zone_id
}

data "aws_iam_role" "EMR_DefaultRole" {
  name = "EMR_DefaultRole"
}

data "aws_iam_role" "EMR_EC2_DefaultRole" {
  name = "EMR_EC2_DefaultRole"
}

data "aws_security_group" "ElasticMapReduce_slave" {
  name = "ElasticMapReduce-slave"
}

data "aws_security_group" "ElasticMapReduce_master" {
  name = "ElasticMapReduce-master"
}

output "emr_cluster_id" {
  value = aws_emr_cluster.emr_clinsight.id
}

output "emr_cluster_master_dns" {
  value = aws_emr_cluster.emr_clinsight.master_public_dns
}

output "clinsight_fhir_url" {
  value = "http://${aws_emr_cluster.emr_clinsight.master_public_dns}:8888/fhir"
}

output "emr_master_private_ip" {
  value = data.aws_instance.master.private_ip
}

output "emr_master_id" {
  value = data.aws_instance.master.id
}

resource "aws_security_group" "allow_csiro_access" {
  name        = "allow_csiro_access"
  description = "Allow traffic for CSIRO intranet"

  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["140.253.176.0/24", "140.253.169.0/24"]
  }

  tags = {
    Name = "allow_csiro_access"
  }
}

resource "aws_security_group" "allow_http_access" {
  name        = "cl_allow_http_access"
  description = "ClinSight Allow HTTP traffic"

  ingress {
    from_port = 8888
    to_port   = 8888
    protocol  = "tcp"

    # this can be changed to public access
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ClinSight allow_http_access"
  }
}

resource "aws_s3_bucket_object" "clinsight_bootstrap" {
  bucket = var.clinsight_bucket
  key    = "/deploy/bootstrap/install-clinsight.sh"
  source = "../bootstrap/install-clinsight.sh"

  # The filemd5() function is available in Terraform 0.11.12 and later
  # For Terraform 0.11.11 and earlier, use the md5() function and the file() function:
  # etag = "${md5(file("path/to/file"))}"
  #etag = "${filemd5("../bootstrap/install-clinsight.sh")}"
  etag = filemd5("../bootstrap/install-clinsight.sh")
}

resource "aws_emr_cluster" "emr_clinsight" {
  name                              = "Clinsight Server"
  release_label                     = "emr-5.27.0"
  applications                      = ["Spark", "Ganglia"]
  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true
  visible_to_all_users              = true

  ec2_attributes {
    key_name                          = var.key_name
    subnet_id                         = data.aws_subnet.subnet.id
    emr_managed_master_security_group = data.aws_security_group.ElasticMapReduce_master.id
    emr_managed_slave_security_group  = data.aws_security_group.ElasticMapReduce_slave.id
    instance_profile = replace(
      data.aws_iam_role.EMR_EC2_DefaultRole.arn,
      "role",
      "instance-profile",
    )
    additional_master_security_groups = "${aws_security_group.allow_csiro_access.id},${aws_security_group.allow_http_access.id}"
  }

  master_instance_group {
    instance_type  = "m1.large"
    instance_count = "1"
  }

  core_instance_group {
    instance_type  = "m1.large"
    instance_count = "2"
  }

  ebs_root_volume_size = 100

  tags = {
    role = "rolename"
    env  = "env"
  }
  service_role = data.aws_iam_role.EMR_DefaultRole.arn

  log_uri = "s3://${var.clinsight_bucket}/logs/emr/"

  configurations_json = <<EOF
    [{
      "classification":"spark-defaults",
      "properties": {
        "spark.dynamicAllocation.enabled":"false"
      }
    },
    {
      "Classification": "spark",
      "Properties": {
        "maximizeResourceAllocation": "true"
      }
    }
  ]
EOF


  bootstrap_action {
    path = "s3://${aws_s3_bucket_object.clinsight_bootstrap.bucket}${aws_s3_bucket_object.clinsight_bootstrap.id}"
    name = "Install ClinSight"
    args = ["--release", "s3://${var.clinsight_bucket}/deploy/release/1.0.0-latest"]
  }

  # Enable debugging

  step {
    action_on_failure = "TERMINATE_CLUSTER"
    name              = "Setup Hadoop Debugging"

    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = ["state-pusher-script"]
    }
  }

  # Start Clinsight
  step {
    action_on_failure = "TERMINATE_CLUSTER"
    name              = "Start Clinsight"

    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = ["sudo", "start", "clinsight"]
    }
  }

  # Optional: ignore outside changes to running cluster steps
  lifecycle {
    ignore_changes = [step]
  }
}

data "aws_instance" "master" {
  filter {
    name   = "dns-name"
    values = [aws_emr_cluster.emr_clinsight.master_public_dns]
  }
}

