-- Table: golden_layer.payment_data

-- DROP TABLE IF EXISTS golden_layer.payment_data;

CREATE TABLE IF NOT EXISTS golden_layer.payment_data
(
    unique_id character varying(100) COLLATE pg_catalog."default",
    clabe character varying(100) COLLATE pg_catalog."default",
    download_speed integer,
    upload_speed integer,
    session_duration integer,
    consumed_traffic integer,
    payment_amount integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS golden_layer.payment_data
    OWNER to postgres;