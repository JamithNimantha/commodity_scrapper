CREATE TABLE IF NOT EXISTS public.commodities
(
    commodity_name character varying(30) COLLATE pg_catalog."default" NOT NULL,
    update_date date NOT NULL,
    update_time time without time zone NOT NULL,
    price numeric(15,2),
    change numeric(10,2),
    day_percent numeric(8,4),
    week_percent numeric(8,4),
    month_percent numeric(8,4),
    yoy_percent numeric(8,4),
    currency character varying(10) COLLATE pg_catalog."default",
    quantity character varying(20) COLLATE pg_catalog."default",
    data_date date,
    last_price numeric(15,2),
    CONSTRAINT commodities_pkey PRIMARY KEY (commodity_name, update_date, update_time)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.commodities
    OWNER to postgres;

COMMENT ON TABLE public.commodities
    IS 'https://tradingeconomics.com/commodities Daily
percent figures are mutiplied by 100. Which means all that has been done is knock of % sign from the figures on web';