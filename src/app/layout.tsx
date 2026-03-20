import type { Metadata } from 'next';
import { cookies } from 'next/headers';
import { NextIntlClientProvider } from 'next-intl';
import { getLocale, getMessages } from 'next-intl/server';
import { BrowserTimeZoneProvider } from '@/components/providers/BrowserTimeZoneProvider';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopBar } from '@/components/layout/TopBar';
import { APP_VERSION } from '@/config/app';
import { normalizeTimeZone, TIME_ZONE_COOKIE } from '@/lib/timeZone';
import './globals.css';

export const metadata: Metadata = {
  title: { template: '%s | MatchIndex', default: 'MatchIndex' },
  description: 'Football data exploration platform',
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const locale = await getLocale();
  const messages = await getMessages();
  const cookieStore = await cookies();
  const initialTimeZone = normalizeTimeZone(cookieStore.get(TIME_ZONE_COOKIE)?.value);
  const hasDatabase = Boolean(process.env.DATABASE_URL);

  return (
    <html lang={locale} className="dark">
      <body className="antialiased">
        <NextIntlClientProvider messages={messages}>
          <BrowserTimeZoneProvider initialTimeZone={initialTimeZone}>
            <div className="flex h-screen flex-col overflow-hidden bg-surface-0 font-sans text-[13px]">
              <TopBar />
              <div className="flex flex-1 overflow-hidden">
                <Sidebar appVersion={APP_VERSION} hasDatabase={hasDatabase} />
                <main className="flex-1 overflow-y-auto p-4 md:p-6 bg-surface-0">
                  <div className="mx-auto max-w-[1400px]">
                    {children}
                  </div>
                </main>
              </div>
            </div>
          </BrowserTimeZoneProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
