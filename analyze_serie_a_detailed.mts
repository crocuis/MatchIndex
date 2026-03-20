import { readFile } from 'node:fs/promises';
import path from 'node:path';

interface DuplicateMatchGroup {
  competition_slug: string;
  season_slug: string;
  match_count: number;
}

async function analyzeSerieA() {
  try {
    // Read the cleanup plan
    const cleanupPlanPath = path.join(process.cwd(), 'data/sofascore-duplicate-cleanup-plan.json');
    const cleanupPlanContent = await readFile(cleanupPlanPath, 'utf-8');
    const cleanupPlan = JSON.parse(cleanupPlanContent);
    
    console.log('=== Serie A Data Quality Analysis ===\n');
    
    // Filter for Serie A
    const serieADuplicates = cleanupPlan.duplicateMatchGroups.filter(
      (group: DuplicateMatchGroup) => group.competition_slug === 'serie-a'
    );
    
    console.log('Duplicate Match Groups by Season:');
    console.log(JSON.stringify(serieADuplicates, null, 2));
    
    // Summary statistics
    const totalDuplicateGroups = serieADuplicates.length;
    const totalDuplicateMatches = serieADuplicates.reduce((sum: number, g: DuplicateMatchGroup) => sum + g.match_count, 0);
    
    console.log(`\n=== Summary ===`);
    console.log(`Total seasons with duplicates: ${totalDuplicateGroups}`);
    console.log(`Total duplicate match groups: ${totalDuplicateMatches}`);
    
    // Analyze by season
    console.log(`\n=== Per-Season Breakdown ===`);
    for (const group of serieADuplicates) {
      console.log(`Season ${group.season_slug}: ${group.match_count} duplicate fixture groups`);
    }
    
  } catch (error) {
    console.error('Error:', error);
  }
}

analyzeSerieA();
