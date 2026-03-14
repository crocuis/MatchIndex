const LEADING_FC_PATTERN = /^FC\s+/i;
const TRAILING_CLUB_SUFFIX_PATTERN = /\s+(FC|WFC)$/i;

export function deriveReviewedTeamShortName(name: string) {
  const normalized = name.trim().replace(/\s+/g, ' ');
  if (!normalized) {
    return '';
  }

  const withoutLeadingFc = normalized.replace(LEADING_FC_PATTERN, '').trim();
  const withoutTrailingSuffix = withoutLeadingFc.replace(TRAILING_CLUB_SUFFIX_PATTERN, '').trim();

  return withoutTrailingSuffix || normalized;
}
