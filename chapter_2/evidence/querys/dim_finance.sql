CREATE TABLE
	silver_layer.dim_finance AS
SELECT
	unique_id,
	clabe
FROM
	bronze_layer.batch_first_load
	