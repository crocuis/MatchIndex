import { getRequestConfig } from 'next-intl/server';
import { cookies } from 'next/headers';

const LOCALE_COOKIE = 'MATCHINDEX_LOCALE';
const VALID_LOCALES = ['en', 'ko'] as const;
type Locale = (typeof VALID_LOCALES)[number];

export default getRequestConfig(async () => {
  const cookieStore = await cookies();
  const raw = cookieStore.get(LOCALE_COOKIE)?.value ?? 'en';
  const locale: Locale = VALID_LOCALES.includes(raw as Locale) ? (raw as Locale) : 'en';

  return {
    locale,
    messages: (await import(`../../messages/${locale}.json`)).default,
  };
});
