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

SET default_table_access_method = heap;

--
-- Name: pm_mo_group; Type: TABLE; Schema: public; Owner: pm
--

CREATE TABLE public.pm_mo_group (
    groupid character varying(60) NOT NULL,
    model character varying(60),
    layer0 character varying(60) NOT NULL,
    layer1 character varying(60),
    layer2 character varying(60),
    layer3 character varying(60),
    layer4 character varying(60),
    layer5 character varying(60),
    layer6 character varying(60),
    layer7 character varying(60),
    layer8 character varying(60),
    layer9 character varying(60),
    layer10 character varying(60),
    grouptype character varying(60),
    meplmnlist character varying(1000),
    nr boolean,
    nbiot boolean,
    lte boolean,
    ltefdd boolean,
    ltetdd boolean,
    umts boolean,
    gsm boolean
) DISTRIBUTED BY (layer0);


ALTER TABLE public.pm_mo_group OWNER TO pm;

--
-- Greenplum Database database dump complete
--

