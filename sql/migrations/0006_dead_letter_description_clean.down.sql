-- Reverts 0006. Drops description_clean and the descriptions in it.
COMMENT ON COLUMN public.dead_letter_log.description IS NULL;
ALTER TABLE public.dead_letter_log DROP COLUMN description_clean;
