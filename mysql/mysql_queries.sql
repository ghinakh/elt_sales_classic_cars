docker ps -a

docker start -ai mysql-db

docker cp "D:\Dokumen Ghina\archives\Portofolio\sales_classic_cars\mysql\mysqlsampledatabase.sql" mysql-db:/tmp/mysqlsampledatabase.sql

docker exec -it mysql-db bash

mysql --version

mysql -h db-sales-classic-cars.cluster-cm9ou64wcqti.us-east-1.rds.amazonaws.com -u admin -p < /tmp/mysqlsampledatabase.sql
