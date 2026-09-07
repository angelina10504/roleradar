-- STALE. Do not read this as the live schema.
--
-- This dump predates the primary-key fix. It shows jobs_extracted with
-- PRIMARY KEY (source_job_id) and no id column, which is the upsert-shaped bug
-- decision (a) exists to prevent. The live table has been checked and has
-- PRIMARY KEY (id) plus the (source_job_id, ingested_at DESC) index, matching
-- 01_jobs_extracted.sql.
--
-- Kept only as a record of what the schema used to be. Re-dump before trusting
-- it again:
--   docker exec roleradar-pg pg_dump -U roleradar -d roleradar --schema-only

--
-- PostgreSQL database dump
--

\restrict BV8fC4xZXFrrcgHZKDVL3XisnGAxhVCe5syopO30eseSOrOIvoMeBCbcMrwUViB

-- Dumped from database version 16.14 (Debian 16.14-1.pgdg13+1)
-- Dumped by pg_dump version 16.14 (Debian 16.14-1.pgdg13+1)

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
-- Name: dead_letter_log; Type: TABLE; Schema: public; Owner: roleradar
--

CREATE TABLE public.dead_letter_log (
    id bigint NOT NULL,
    source_job_id text,
    source_url text,
    raw_title text,
    raw_company text,
    description text,
    ingested_at timestamp with time zone NOT NULL,
    model text,
    extraction_version text,
    error_codes jsonb NOT NULL,
    errors_json jsonb NOT NULL,
    raw_output jsonb
);


ALTER TABLE public.dead_letter_log OWNER TO roleradar;

--
-- Name: dead_letter_log_id_seq; Type: SEQUENCE; Schema: public; Owner: roleradar
--

CREATE SEQUENCE public.dead_letter_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dead_letter_log_id_seq OWNER TO roleradar;

--
-- Name: dead_letter_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: roleradar
--

ALTER SEQUENCE public.dead_letter_log_id_seq OWNED BY public.dead_letter_log.id;


--
-- Name: jobs_extracted; Type: TABLE; Schema: public; Owner: roleradar
--

CREATE TABLE public.jobs_extracted (
    source_job_id text NOT NULL,
    source_url text NOT NULL,
    raw_title text,
    raw_company text,
    ingested_at timestamp without time zone,
    model text,
    extraction_version text,
    normalized_title text,
    company text,
    seniority text,
    tech_stack jsonb,
    remote_type text,
    salary_min numeric,
    salary_max numeric,
    salary_period text,
    mentions_n8n_model boolean,
    mentions_n8n_actual boolean,
    ai_ml_role boolean
);


ALTER TABLE public.jobs_extracted OWNER TO roleradar;

--
-- Name: dead_letter_log id; Type: DEFAULT; Schema: public; Owner: roleradar
--

ALTER TABLE ONLY public.dead_letter_log ALTER COLUMN id SET DEFAULT nextval('public.dead_letter_log_id_seq'::regclass);


--
-- Name: dead_letter_log dead_letter_log_pkey; Type: CONSTRAINT; Schema: public; Owner: roleradar
--

ALTER TABLE ONLY public.dead_letter_log
    ADD CONSTRAINT dead_letter_log_pkey PRIMARY KEY (id);


--
-- Name: jobs_extracted jobs_extracted_pkey; Type: CONSTRAINT; Schema: public; Owner: roleradar
--

ALTER TABLE ONLY public.jobs_extracted
    ADD CONSTRAINT jobs_extracted_pkey PRIMARY KEY (source_job_id);


--
-- PostgreSQL database dump complete
--

\unrestrict BV8fC4xZXFrrcgHZKDVL3XisnGAxhVCe5syopO30eseSOrOIvoMeBCbcMrwUViB

