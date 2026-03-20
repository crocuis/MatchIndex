UPDATE team_translations AS ko
SET
    name = '노팅엄 포레스트',
    short_name = CASE
        WHEN ko.short_name = '노팅엄 포리스트' THEN '노팅엄 포레스트'
        ELSE ko.short_name
    END
FROM team_translations AS en
WHERE ko.team_id = en.team_id
  AND ko.locale = 'ko'
  AND en.locale = 'en'
  AND en.name = 'Nottingham Forest'
  AND ko.name = '노팅엄 포리스트';
