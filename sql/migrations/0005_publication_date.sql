-- 0005 publication_date
--
-- When the posting went up, as opposed to ingested_at, which is when this
-- pipeline saw it.
--
-- timestamp without time zone, on purpose. Remotive sends
-- "2026-08-21T05:54:39" with no offset and does not document the zone. Storing
-- it as timestamptz would mean choosing one, and the column would then claim
-- more than the source said.

ALTER TABLE public.jobs_extracted
    ADD COLUMN publication_date timestamp without time zone;

ALTER TABLE public.dead_letter_log
    ADD COLUMN publication_date timestamp without time zone;
