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
-- Name: a_n_nrcell_mo_dimension_h; Type: TABLE; Schema: public; Owner: pm
--

CREATE TABLE public.a_n_nrcell_mo_dimension_h (
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
    gnbplmn character varying(257),
    gnbidlength character varying(257),
    gnbid character varying(257),
    cellid numeric(7,0),
    dume character varying(257),
    gnbdufunction character varying(257),
    nrcelldu character varying(257),
    dumemoid character varying(51),
    masteroperatorid character varying(32),
    nrradioinfrastructure character varying(257),
    nrphysicalcellduid numeric(12,0),
    nrphysicalcelldu character varying(257),
    nrcarriergroupid numeric(7,0),
    nrcarriergroup numeric(7,0),
    cellatt character varying(52),
    duplexmode character varying(52),
    ssbfrequency numeric(12,2),
    frequencybandlistdl character varying(257)
)
PARTITION BY RANGE (collecttime) DISTRIBUTED BY (me);


ALTER TABLE public.a_n_nrcell_mo_dimension_h OWNER TO pm;

--
-- Name: index_a_n_nrcell_mo_dimension_h_collecttime_me_gnbcucpfunction; Type: INDEX; Schema: public; Owner: pm
--

CREATE INDEX index_a_n_nrcell_mo_dimension_h_collecttime_me_gnbcucpfunction ON ONLY public.a_n_nrcell_mo_dimension_h USING btree (collecttime, me, gnbcucpfunction);


--
-- Greenplum Database database dump complete
--

