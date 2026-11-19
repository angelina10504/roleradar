-- Reverts 0005. Drops publication_date and the values in it.
ALTER TABLE public.dead_letter_log DROP COLUMN publication_date;
ALTER TABLE public.jobs_extracted  DROP COLUMN publication_date;
