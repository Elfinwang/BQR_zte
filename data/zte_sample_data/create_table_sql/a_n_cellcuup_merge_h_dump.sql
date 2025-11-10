--
-- Greenplum Database database dump
--

-- Dumped from database version 12
-- Dumped by pg_dump version 12

SET gp_default_storage_options = '';
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

--
-- Name: a_n_cellcuup_merge_h; Type: TABLE; Schema: public; Owner: pm
--

CREATE TABLE public.a_n_cellcuup_merge_h (
    collecttime timestamp without time zone,
    collecttimegmt timestamp without time zone,
    timezoneoffset numeric(4,0),
    dstsaving numeric(2,0),
    loadtime timestamp without time zone DEFAULT timezone('utc'::text, now()),
    granularity numeric(10,0),
    status numeric(10,0),
    meid numeric(10,0),
    me character varying(255),
    gnbcucpfunction character varying(255),
    nrcellcu character varying(255),
    cellid numeric(5,0),
    dumemoid character varying(49),
    gnbid numeric(10,0),
    gnbidlength numeric(5,0),
    gnbplmn character varying(10),
    masteroperatorid character varying(30),
    nrcarriergroupid numeric(5,0),
    nrphysicalcellduid numeric(5,0),
    c605210006 numeric(26,4),
    c605210007 numeric(26,4),
    c605210010 numeric(26,4),
    c605210011 numeric(26,4),
    c605210022 numeric(26,4),
    c605210023 numeric(26,4),
    c605210034 numeric(26,4),
    c605210035 numeric(26,4),
    c605220000 numeric(18,0),
    c605220001 numeric(18,0),
    c605220002 numeric(18,0),
    c605220003 numeric(12,0),
    c605220004 numeric(12,0),
    c605220005 numeric(18,0),
    c605220006 numeric(18,0),
    c605220020 numeric(18,0),
    c605220021 numeric(18,0),
    c605220022 numeric(18,0),
    c605220023 numeric(18,0),
    c605220024 numeric(18,0),
    c605220025 numeric(18,0),
    c605220026 numeric(18,0),
    c605220027 numeric(18,0),
    c605220038 numeric(18,4),
    c605220039 numeric(18,0),
    c605230000 numeric(26,4),
    c605230003 numeric(26,4),
    c605280000 numeric(18,0),
    c605280001 numeric(18,0),
    c605280002 numeric(18,0),
    c605280003 numeric(18,0),
    c605280004 numeric(18,0),
    c605280005 numeric(18,0),
    c605280006 numeric(18,0),
    c605280007 numeric(18,0),
    c605280008 numeric(18,0),
    c605280009 numeric(18,0),
    c605280010 numeric(26,4),
    c605280012 numeric(26,4),
    c605280013 numeric(26,4),
    c605280015 numeric(26,4),
    c605280016 numeric(26,4),
    c605280018 numeric(26,4),
    c605280019 numeric(26,4),
    c605280020 numeric(18,0),
    c605280021 numeric(18,0),
    c605280022 numeric(18,0),
    c605280023 numeric(18,0),
    c605280024 numeric(18,0),
    c605280025 numeric(18,0),
    c605280026 numeric(26,4),
    c605330006 numeric(26,4),
    c605330007 numeric(26,4),
    c605330008 numeric(26,4),
    c605330009 numeric(26,4),
    c605340000 numeric(18,0),
    c605340001 numeric(18,0),
    c605340002 numeric(18,0),
    c605340003 numeric(18,0),
    c605340004 numeric(18,0),
    c605340005 numeric(18,0),
    c605340006 numeric(18,0),
    c605340007 numeric(18,0),
    c605340008 numeric(18,0),
    c605340009 numeric(18,0),
    c605340010 numeric(18,0),
    c605340011 numeric(18,0),
    c605340012 numeric(18,0),
    c605340013 numeric(18,0),
    c605340014 numeric(18,0),
    c605390000 numeric(18,0),
    c605390001 numeric(18,0),
    c605390002 numeric(20,0),
    c605390003 numeric(20,0),
    c605390004 numeric(20,0),
    c605210026 numeric(26,4),
    c605210027 numeric(26,4),
    c605210028 numeric(26,4),
    c605210029 numeric(26,4),
    c605210030 numeric(18,0),
    c605210031 numeric(18,0),
    c605210032 numeric(18,0),
    c605210033 numeric(18,0),
    c605220032 numeric(18,0),
    c605220033 numeric(18,0),
    c605220034 numeric(20,0),
    c605220035 numeric(20,0),
    c605220036 numeric(20,0),
    c605219992 numeric(26,4),
    c605219993 numeric(26,4),
    c605219994 numeric(26,4),
    c605219995 numeric(26,4),
    c605219996 numeric(26,4),
    c605219997 numeric(26,4),
    c605219998 numeric(26,4),
    c605219999 numeric(26,4),
    c605229982 numeric(18,0),
    c605229983 numeric(18,0),
    c605229984 numeric(18,0),
    c605229985 numeric(18,0),
    c605229986 numeric(18,0),
    c605229987 numeric(18,0),
    c605229988 numeric(18,0),
    c605229989 numeric(18,0),
    c605229990 numeric(18,0),
    c605229991 numeric(18,0),
    c605229992 numeric(18,0),
    c605229993 numeric(18,0),
    c605229994 numeric(18,0),
    c605229995 numeric(18,0),
    c605229996 numeric(18,0),
    c605229997 numeric(18,0),
    c605229998 numeric(18,0),
    c605229999 numeric(18,0),
    c605239998 numeric(26,4),
    c605239999 numeric(26,4),
    c605280011 numeric(26,4),
    c605280014 numeric(26,4),
    c605280017 numeric(26,4),
    c605210039 numeric(22,4),
    c605210040 numeric(22,4),
    c605310006 numeric(18,0),
    c605310007 numeric(22,4),
    c605310008 numeric(18,0),
    c605310009 numeric(18,0),
    c605390005 numeric(22,4),
    c605390006 numeric(18,0),
    c605390007 numeric(18,0),
    c605390008 numeric(18,0),
    c605390009 numeric(18,0),
    c605390010 numeric(18,0),
    c605390011 numeric(18,0),
    plmninfocu character varying(255) DEFAULT 1,
    servingplmn character varying(10)
)
PARTITION BY RANGE (collecttime) DISTRIBUTED BY (me);


ALTER TABLE public.a_n_cellcuup_merge_h OWNER TO pm;

--
-- Name: index_a_n_cellcuup_merge_h_collecttime_me_gnbcucpfunction; Type: INDEX; Schema: public; Owner: pm
--

CREATE INDEX index_a_n_cellcuup_merge_h_collecttime_me_gnbcucpfunction ON ONLY public.a_n_cellcuup_merge_h USING btree (collecttime, me, gnbcucpfunction);


--
-- Greenplum Database database dump complete
--

