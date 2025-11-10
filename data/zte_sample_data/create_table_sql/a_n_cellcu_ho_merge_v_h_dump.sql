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
-- Name: a_n_cellcu_ho_merge_v_h; Type: TABLE; Schema: public; Owner: pm
--

CREATE TABLE public.a_n_cellcu_ho_merge_v_h (
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
    pdf_index character varying(255),
    c600000000 numeric(12,0),
    c600000001 numeric(12,0),
    c600000002 numeric(12,0),
    c600000003 numeric(12,0),
    c600000004 numeric(12,0),
    c600000018 numeric(12,0),
    c600000019 numeric(12,0),
    c600000023 numeric(12,0),
    c600000025 numeric(12,0),
    c600000026 numeric(12,0),
    c600000090 numeric(12,0),
    c600080000 numeric(18,0),
    c600080001 numeric(18,0),
    c600080002 numeric(18,0),
    c600080003 numeric(18,0),
    c600080004 numeric(18,0),
    c600080005 numeric(18,0),
    c600080006 numeric(18,0),
    c600080007 numeric(18,0),
    c600080008 numeric(18,0),
    c600080009 numeric(18,0),
    c600080010 numeric(18,0),
    c600080011 numeric(18,0),
    c600080012 numeric(18,0),
    c600080013 numeric(18,0),
    c600080014 numeric(18,0),
    c600080015 numeric(18,0),
    c600080017 numeric(18,0),
    c600080019 numeric(18,0),
    c600080020 numeric(18,0),
    c600080021 numeric(24,6),
    c600080022 numeric(18,0),
    c600080023 numeric(18,0),
    c600080024 numeric(18,0),
    c600080025 numeric(18,0),
    c600080026 numeric(18,0),
    c600080027 numeric(18,0),
    c600080040 numeric(18,0),
    c600080041 numeric(18,0),
    c600080050 numeric(18,0),
    c600080051 numeric(18,0),
    c600080053 numeric(18,0),
    c600080054 numeric(18,0),
    c600080055 numeric(18,0),
    c600080056 numeric(18,0),
    c600080057 numeric(18,0),
    c600080058 numeric(18,0),
    c600080059 numeric(12,0),
    c600080060 numeric(12,0),
    c600080061 numeric(12,0),
    c600080062 numeric(18,0),
    c600080063 numeric(12,0),
    c600080064 numeric(12,0),
    c600080065 numeric(12,0),
    c600080066 numeric(12,0),
    c600080067 numeric(12,0),
    c600080068 numeric(18,0),
    c600080070 numeric(12,0),
    c600080073 numeric(18,0),
    c600080105 numeric(18,0),
    c600080106 numeric(18,0),
    c600080107 numeric(18,0),
    c600080108 numeric(18,0),
    c600080109 numeric(18,0),
    c600080110 numeric(12,0),
    c600080111 numeric(18,0),
    c600080112 numeric(18,0),
    c600080113 numeric(12,0),
    c600080114 numeric(12,0),
    c600080115 numeric(12,0),
    c600080116 numeric(12,0),
    c600080117 numeric(12,0),
    c600080118 numeric(12,0),
    c600080119 numeric(12,0),
    c600080120 numeric(12,0),
    c600080121 numeric(12,0),
    c600080122 numeric(12,0),
    c600080123 numeric(12,0),
    c600080126 numeric(12,0),
    c600190000 numeric(12,0),
    c600190001 numeric(12,0),
    c600800025 numeric(12,0),
    c600800026 numeric(12,0),
    c600800027 numeric(12,0),
    c600800028 numeric(12,0),
    c600800029 numeric(12,0),
    c600800031 numeric(12,0),
    c600800032 numeric(12,0),
    c600800033 numeric(12,0),
    c600840000 numeric(12,0),
    c600840001 numeric(12,0),
    c600840002 numeric(12,0),
    c600840003 numeric(12,0),
    c600840004 numeric(12,0),
    c600840005 numeric(12,0),
    c600840007 numeric(12,0),
    c600850033 numeric(18,0),
    c600080074 numeric(18,0),
    c600080075 numeric(18,0),
    c600000022 numeric(12,0),
    c600000024 numeric(12,0),
    c600000045 numeric(18,0),
    c600000046 numeric(18,4),
    c600080016 numeric(18,0),
    c600080018 numeric(18,0),
    c600080028 numeric(18,0),
    c600080029 numeric(18,0),
    c600080030 numeric(18,0),
    c600080031 numeric(18,0),
    c600080032 numeric(20,6),
    c600080033 numeric(20,6),
    c600080034 numeric(20,6),
    c600080035 numeric(20,6),
    c600850003 numeric(18,0),
    c600850004 numeric(18,0),
    c600850010 numeric(18,0),
    c600850011 numeric(18,0),
    c600850012 numeric(18,0),
    c600850029 numeric(18,0),
    c600850030 numeric(18,0),
    c600850031 numeric(18,0),
    c600850032 numeric(18,0),
    c600850034 numeric(18,0),
    c600850035 numeric(18,0),
    c600850036 numeric(18,0),
    c600850038 numeric(12,0),
    c600080124 numeric(12,0),
    plmninfocu character varying(255) DEFAULT 1,
    servingplmn character varying(10)
)
PARTITION BY RANGE (collecttime) DISTRIBUTED BY (me);


ALTER TABLE public.a_n_cellcu_ho_merge_v_h OWNER TO pm;

--
-- Name: index_a_n_cellcu_ho_merge_v_h_collecttime; Type: INDEX; Schema: public; Owner: pm
--

CREATE INDEX index_a_n_cellcu_ho_merge_v_h_collecttime ON ONLY public.a_n_cellcu_ho_merge_v_h USING bitmap (collecttime);


--
-- Name: index_a_n_cellcu_ho_merge_v_h_me; Type: INDEX; Schema: public; Owner: pm
--

CREATE INDEX index_a_n_cellcu_ho_merge_v_h_me ON ONLY public.a_n_cellcu_ho_merge_v_h USING bitmap (me);


--
-- Greenplum Database database dump complete
--

