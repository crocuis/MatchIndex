UPDATE competitions
SET
    comp_type = 'league',
    updated_at = NOW()
WHERE slug IN (
    '1-bundesliga',
    'ligue-1',
    'premier-league',
    'serie-a'
)
  AND comp_type <> 'league';
