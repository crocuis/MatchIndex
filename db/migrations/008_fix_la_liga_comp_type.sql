UPDATE competitions
SET
    comp_type = 'league',
    is_international = FALSE,
    updated_at = NOW()
WHERE slug = 'la-liga'
  AND (comp_type <> 'league' OR is_international <> FALSE);
