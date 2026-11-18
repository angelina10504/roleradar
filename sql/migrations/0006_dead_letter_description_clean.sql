-- 0006 dead_letter_log.description_clean
--
-- The validator emits description_clean. The column was named description, so
-- the value never landed: description is NULL in every row.
--
-- This adds a column with the name the validator uses, so no mapping node is
-- needed. The old column is left in place. Renaming it would be tidier, but a
-- rename and the canvas change cannot happen at the same instant, and whichever
-- went first would break inserts until the other caught up. Drop the old
-- column in a later migration, once new rows show description_clean filled.

ALTER TABLE public.dead_letter_log
    ADD COLUMN description_clean text;

COMMENT ON COLUMN public.dead_letter_log.description IS
    'Deprecated. Never populated. Use description_clean.';
