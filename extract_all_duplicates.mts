import { readFile } from 'node:fs/promises';
import path from 'node:path';

interface DuplicateMatchGroup {
  competition_slug: string;
  season_slug: string;
  match_count: number;
}

async function extractDuplicates() {
  try {
    const cleanupPlanPath = path.join(process.cwd(), 'data/sofascore-duplicate-cleanup-plan.json');
    const cleanupPlanContent = await readFile(cleanupPlanPath, 'utf-8');
    const cleanupPlan = JSON.parse(cleanupPlanContent);
    
    const allDuplicates = cleanupPlan.duplicateMatchGroups as DuplicateMatchGroup[];
    
    // Group by competition
    const byCompetition: Record<string, DuplicateMatchGroup[]> = {};
    for (const group of allDuplicates) {
      if (!byCompetition[group.competition_slug]) {
        byCompetition[group.competition_slug] = [];
      }
      byCompetition[group.competition_slug].push(group);
    }
    
    console.log('=== All Competitions with Duplicates ===\n');
    for (const [comp, groups] of Object.entries(byCompetition)) {
      const totalDups = groups.reduce((sum, g) => sum + g.match_count, 0);
      console.log(`${comp}: ${totalDups} total duplicate groups across ${groups.length} season(s)`);
      for (const g of groups) {
        console.log(`  - ${g.season_slug}: ${g.match_count} groups`);
      }
    }
    
  } catch (error) {
    console.error('Error:', error);
  }
}

extractDuplicates();
