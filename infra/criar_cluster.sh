eksctl create cluster \
   --version=1.21 \
   --name=datagio \
   --timeout=60m0s \
   --managed \
   --instance-types=m5.xlarge \
   --alb-ingress-access --node-private-networking \
   --region=us-east-1 \
   --nodes-min=2 --nodes-max=3 \
   --full-ecr-access \
   --asg-access \
   --nodegroup-name=ng-datagio