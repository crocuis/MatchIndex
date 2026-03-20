import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL!);

async function analyzeSerieA() {
  try {
    // Get Serie A competition ID
    const competitions = await sql`
      SELECT id, slug, code FROM competitions 
      WHERE code = 'SA' OR slug LIKE '%serie%'
      LIMIT 5
    `;
    console.log('=== Serie A Competitions ===');
    console.log(JSON.stringify(competitions, null, 2));
    
    if (competitions.length === 0) {
      console.log('No Serie A competition found');
      await sql.end();
      return;
    }
    
    const compId = competitions[0].id;
    
    // Get all seasons for Serie A
    const seasons = await sql`
      SELECT DISTINCT 
        cs.id,
        s.slug as season_slug,
        s.start_date,
        s.end_date,
        COUNT(DISTINCT m.id) as match_count
      FROM competition_seasons cs
      JOIN seasons s ON s.id = cs.season_id
      LEFT JOIN matches m ON m.competition_season_id = cs.id
      WHERE cs.competition_id = ${compId}
      GROUP BY cs.id, s.slug, s.start_date, s.end_date
      ORDER BY s.start_date DESC
    `;
    
    console.log('\n=== Serie A Seasons ===');
    console.log(JSON.stringify(seasons, null, 2));
    
    // For each season, analyze source distribution and duplicates
    for (const season of seasons) {
      console.log(`\n=== Season ${season.season_slug} (${season.match_count} matches) ===`);
      
      // Source distribution
      const sources = await sql`
        SELECT 
          COALESCE(sm.source, 'unknown') as source,
          COUNT(DISTINCT m.id) as match_count
        FROM matches m
        LEFT JOIN source_sync_manifests sm ON sm.id = (
          SELECT id FROM source_sync_manifests 
          WHERE match_id = m.id 
          ORDER BY created_at DESC 
          LIMIT 1
        )
        WHERE m.competition_season_id = ${season.id}
        GROUP BY sm.source
        ORDER BY match_count DESC
      `;
      
      console.log('Source Distribution:');
      console.log(JSON.stringify(sources, null, 2));
      
      // Find duplicate fixtures (same date, home, away)
      const duplicates = await sql`
        SELECT 
          m.match_date,
          ht.slug as home_team,
          at.slug as away_team,
          COUNT(DISTINCT m.id) as duplicate_count,
          ARRAY_AGG(DISTINCT m.id ORDER BY m.id) as match_ids,
          ARRAY_AGG(DISTINCT COALESCE(sm.source, 'unknown') ORDER BY COALESCE(sm.source, 'unknown')) as sources
        FROM matches m
        JOIN teams ht ON ht.id = m.home_team_id
        JOIN teams at ON at.id = m.away_team_id
        LEFT JOIN source_sync_manifests sm ON sm.id = (
          SELECT id FROM source_sync_manifests 
          WHERE match_id = m.id 
          ORDER BY created_at DESC 
          LIMIT 1
        )
        WHERE m.competition_season_id = ${season.id}
        GROUP BY m.match_date, ht.slug, at.slug
        HAVING COUNT(DISTINCT m.id) > 1
        ORDER BY m.match_date DESC
      `;
      
      if (duplicates.length > 0) {
        console.log(`\nDuplicate Fixtures (${duplicates.length} groups):`);
        console.log(JSON.stringify(duplicates, null, 2));
      } else {
        console.log('\nNo duplicate fixtures found');
      }
      
      // Check for duplicate team/player patterns
      const teamDuplicates = await sql`
        SELECT 
          t.slug,
          COUNT(DISTINCT ts.id) as team_season_count,
          COUNT(DISTINCT pc.id) as player_contract_count
        FROM teams t
        LEFT JOIN team_seasons ts ON ts.team_id = t.id AND ts.competition_season_id = ${season.id}
        LEFT JOIN player_contracts pc ON pc.team_id = t.id AND pc.competition_season_id = ${season.id}
        WHERE t.id IN (
          SELECT DISTINCT home_team_id FROM matches WHERE competition_season_id = ${season.id}
          UNION
          SELECT DISTINCT away_team_id FROM matches WHERE competition_season_id = ${season.id}
        )
        GROUP BY t.slug
        HAVING COUNT(DISTINCT ts.id) > 1 OR COUNT(DISTINCT pc.id) > 1
        ORDER BY team_season_count DESC, player_contract_count DESC
      `;
      
      if (teamDuplicates.length > 0) {
        console.log(`\nTeam/Player Duplication Patterns:`);
        console.log(JSON.stringify(teamDuplicates, null, 2));
      }
    }
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await sql.end();
  }
}

analyzeSerieA();
