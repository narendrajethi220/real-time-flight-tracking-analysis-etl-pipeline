# AWS Security Groups

This project uses separate EC2 instances for Kafka and Spark processing.

## Kafka Instance

The Kafka EC2 instance hosts the Kafka broker and accepts connections from producers and Spark consumers.

Required ports:

- Port 22 (SSH) - Remote administration
- Port 9092 (Kafka Broker) - Producer and Spark consumer communication
- Port 9093 (Kafka Controller) - Kafka KRaft controller communication

## Spark Instance

The Spark EC2 instance runs Spark Streaming jobs that consume data from Kafka.

Required ports:

Inbounds Rules: 
- Port 22 (SSH) - Remote administration
- Inbound access to Kafka on Port 9092
- Inbound access to RDS on Port 3306
- HTTPS Port 44 access to Amazon S3
- HTTP port 40 

Outbounds Rules:
- All (0.0.0.0/0)
- 3306 MySql 

## RDS Instance

The Amazon RDS database stores aggregated flight statistics.

Required ports:

- Port 3306 (MySQL)

Access is restricted to the Spark EC2 instance.

## S3 Access

Amazon S3 is used for storing reports, backups, and processed data.

Required ports:

- Port 443 (HTTPS)

## Security Considerations

- Kafka and Spark are deployed on separate EC2 instances.
- Kafka communication occurs through Port 9092.
- Database access is restricted to the Spark instance.
- RDS is not publicly accessible.
- S3 access uses HTTPS.
- SSH access is restricted to authorized administrators.
- Security groups follow the principle of least privilege.