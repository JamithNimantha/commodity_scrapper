CREATE TABLE IF NOT EXISTS public.commodities_impact
(
    commodity_name character varying(30) COLLATE pg_catalog."default" NOT NULL,
    indicator character varying(7) COLLATE pg_catalog."default",
    corr_1 character varying(4) COLLATE pg_catalog."default",
    corr_2 character varying(4) COLLATE pg_catalog."default",
    corr_3 character varying(4) COLLATE pg_catalog."default",
    corr_4 character varying(4) COLLATE pg_catalog."default",
    corr_5 character varying(4) COLLATE pg_catalog."default",
    neg_1 character varying(4) COLLATE pg_catalog."default",
    neg_2 character varying(4) COLLATE pg_catalog."default",
    neg_3 character varying(4) COLLATE pg_catalog."default",
    neg_4 character varying(4) COLLATE pg_catalog."default",
    neg_5 character varying(4) COLLATE pg_catalog."default",
    record_price_change boolean,
    CONSTRAINT commodities_impact_pkey PRIMARY KEY (commodity_name)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.commodities_impact
    OWNER to postgres;