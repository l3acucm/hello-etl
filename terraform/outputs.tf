output "emr_master_public_dns" {
  description = "Public DNS of the EMR Master node"
  value       = aws_emr_cluster.cluster.master_public_dns
}

resource "local_file" "private_key" {
  filename = "${path.module}/emr-terraform-key.pem"
  content  = tls_private_key.ssh_key.private_key_pem
  file_permission = "0400" # Secure file permissions
}